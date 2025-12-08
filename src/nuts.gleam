import gleam/bit_array
import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/erlang/reference.{type Reference}
import gleam/function
import gleam/int
import gleam/io
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import mug
import nuts/connect_options
import nuts/internal/command
import nuts/internal/protocol
import nuts/nkey

pub opaque type Config {
  Config(
    host: String,
    port: Int,
    retry_interval: Int,
    ping_interval: Int,
    ping_timeout: Int,
    nkey: Option(String),
  )
}

type Subscription {
  Subscription(topic: String, callback: Callback)
}

type Callback =
  fn(Event) -> Result(Nil, Nil)

type Subscribers =
  dict.Dict(String, Subscription)

pub type Event {
  Message(topic: String, headers: List(#(String, String)), payload: BitArray)
}

type PingState {
  PingSent(Reference)
  PingIdle
}

type ConnectionState {
  Disconnected
  Connected(socket: mug.Socket, buffer: BitArray, ping: PingState)
}

type State {
  State(
    config: Config,
    subscribers: Subscribers,
    self: Subject(Message),
    next_sid: Int,
    conn: ConnectionState,
  )
}

pub opaque type Message {
  Connect
  Data(mug.TcpMessage)
  Publish(
    topic: String,
    payload: BitArray,
    headers: List(#(String, String)),
    reply: Subject(Result(Nil, Nil)),
  )
  Subscribe(topic: String, callback: Callback)
  IsConnected(Subject(Bool))
  Shutdown
  SendPing
  GotPong(Reference)
  PingTimeout(Reference)
}

pub fn new(host: String, port: Int) {
  Config(
    host:,
    port:,
    retry_interval: 1000,
    ping_interval: 15_000,
    ping_timeout: 5000,
    nkey: None,
  )
}

pub fn nkey(config: Config, nkey: String) {
  Config(..config, nkey: Some(nkey))
}

pub fn start(config: Config) {
  actor.new_with_initialiser(1000, fn(self) {
    actor.send(self, Connect)
    actor.initialised(State(
      config,
      dict.new(),
      self: self,
      next_sid: 1,
      conn: Disconnected,
    ))
    |> actor.selecting(
      process.new_selector()
      |> mug.select_tcp_messages(Data)
      |> process.select(self),
    )
    |> actor.returning(self)
    |> Ok
  })
  |> actor.on_message(handle_message)
  |> actor.start()
  |> result.map(fn(started) { started.data })
}

fn handle_message(state: State, msg: Message) {
  case state {
    State(conn: Connected(socket:, ..) as conn, ..) as state -> {
      case msg {
        Data(tcp) -> {
          mug.receive_next_packet_as_message(socket)
          case tcp {
            mug.Packet(_socket, data) -> {
              let buffer = bit_array.append(conn.buffer, data)
              case read_protocol_messages(state, buffer) {
                Ok(state) -> state
                Error(_) -> disconnect(state)
              }
              |> actor.continue
            }
            mug.SocketClosed(_) -> {
              process.send_after(
                state.self,
                state.config.retry_interval,
                Connect,
              )
              actor.continue(disconnect(state))
            }
            mug.TcpError(_, _) -> {
              process.send_after(
                state.self,
                state.config.retry_interval,
                Connect,
              )
              actor.continue(disconnect(state))
            }
          }
        }
        Connect -> actor.continue(state)
        Publish(topic:, payload:, reply:, headers:) -> {
          let updated_state = case headers {
            [] ->
              tcp_send_bits(
                state,
                command.pub_(subject: topic, reply_to: None, payload:),
                function.identity,
              )
            headers ->
              tcp_send_bits(
                state,
                command.hpub(subject: topic, reply_to: None, headers:, payload:),
                function.identity,
              )
          }

          actor.send(reply, Ok(Nil))
          actor.continue(updated_state)
        }
        Subscribe(topic, callback) -> {
          let #(sid, updated_state) =
            register_subscriber(state, topic, callback)

          case mug.send(socket, command.sub(topic, sid, None)) {
            Ok(Nil) -> actor.continue(updated_state)
            Error(_) -> actor.continue(disconnect(state))
          }
        }
        Shutdown -> actor.stop()
        IsConnected(reply) -> {
          process.send(reply, True)
          actor.continue(state)
        }
        SendPing -> {
          case mug.send(socket, command.ping()) {
            Ok(Nil) -> {
              let ping_ref = reference.new()
              process.send_after(state.self, 2000, PingTimeout(ping_ref))
              actor.continue(
                State(
                  ..state,
                  conn: Connected(..conn, ping: PingSent(ping_ref)),
                ),
              )
            }
            Error(_) -> actor.continue(disconnect(state))
          }
        }
        GotPong(ref) -> {
          case conn.ping {
            PingSent(ping_ref) if ping_ref == ref -> {
              process.send_after(
                state.self,
                state.config.ping_interval,
                SendPing,
              )
              actor.continue(
                State(..state, conn: Connected(..conn, ping: PingIdle)),
              )
            }
            PingIdle | PingSent(_) -> actor.continue(state)
          }
        }
        PingTimeout(ref) -> {
          case conn.ping {
            PingSent(ping_ref) if ping_ref == ref -> {
              io.println_error("didnt get Pong on time, disconnecting")
              disconnect(state) |> actor.continue
            }
            PingIdle | PingSent(_) -> actor.continue(state)
          }
        }
      }
    }
    State(..) -> {
      case msg {
        Connect -> {
          case setup_connection(state.config) {
            Ok(socket) -> {
              mug.receive_next_packet_as_message(socket)
              case resubscribe(socket, state.subscribers) {
                Ok(socket) -> {
                  State(
                    config: state.config,
                    next_sid: state.next_sid,
                    subscribers: state.subscribers,
                    conn: Connected(
                      socket: socket,
                      buffer: <<>>,
                      ping: PingIdle,
                    ),
                    self: state.self,
                  )
                  |> actor.continue
                }
                Error(_err) -> actor.continue(state)
              }
            }
            Error(_) -> {
              process.send_after(
                state.self,
                state.config.retry_interval,
                Connect,
              )
              actor.continue(state)
            }
          }
        }
        Subscribe(topic:, callback:) -> {
          let #(_sid, state) = register_subscriber(state, topic, callback)
          actor.continue(state)
        }
        Shutdown -> actor.stop()
        IsConnected(reply) -> {
          process.send(reply, False)
          actor.continue(state)
        }
        Data(_) -> actor.continue(state)
        Publish(reply:, ..) -> {
          process.send(reply, Error(Nil))
          actor.continue(state)
        }
        SendPing | GotPong(_) | PingTimeout(_) -> actor.continue(state)
      }
    }
  }
}

//fn send_ping(state: ConnectionState) -> ConnectionState {
//  case state {
//    Disconnected -> state
//    Connected(socket:, ..) ->
//      case mug.send(socket, command.ping()) {
//        Ok(Nil) -> Connected(..state, ping: PingSent(reference.new()))
//        Error(_) -> Disconnected
//      }
//  }
//}

fn update_buffer(conn: ConnectionState, buffer: BitArray) {
  case conn {
    Connected(..) -> Connected(..conn, buffer:)
    Disconnected -> conn
  }
}

fn read_protocol_messages(state: State, buffer: BitArray) -> Result(State, Nil) {
  case protocol.parse(buffer) {
    protocol.Continue(msg, buffer) ->
      state
      |> handle_server_message(msg)
      |> read_protocol_messages(buffer)
    protocol.NeedsMoreData ->
      Ok(State(..state, conn: update_buffer(state.conn, buffer)))
    protocol.ProtocolError(..) -> Error(Nil)
  }
}

fn handle_server_message(state: State, msg) {
  case msg {
    protocol.Info(serverinfo) -> {
      let signature = case serverinfo.nonce, state.config.nkey {
        Some(nonce), Some(nkey) -> {
          case nkey.from_seed(nkey) {
            Ok(keypair) -> {
              [
                connect_options.Signature(bit_array.base64_url_encode(
                  nkey.sign(keypair, bit_array.from_string(nonce)),
                  False,
                )),
                connect_options.NKey(nkey.public(keypair)),
              ]
              |> Ok
            }
            Error(_) -> Error(Nil)
          }
        }
        _, _ -> Error(Nil)
      }

      use _ <- tcp_send_bits(
        state,
        command.connect([
          connect_options.Verbose(False),
          connect_options.Pedantic(False),
          connect_options.TlsRequired(False),
          connect_options.Name("Nuts Gleam NATS Client"),
          connect_options.Lang("gleam"),
          connect_options.Version("0.0.0"),
          connect_options.Protocol(0),
          connect_options.Headers(True),
          connect_options.NoResponders(True),
          ..result.unwrap(signature, [])
        ]),
      )
      state
    }
    protocol.Ping -> {
      use _ <- tcp_send_bits(state, command.pong())
      state
    }
    protocol.Pong -> {
      case state.conn {
        Disconnected -> Nil
        Connected(ping: PingSent(ping_ref), ..) -> {
          process.send(state.self, GotPong(ping_ref))
          Nil
        }
        Connected(ping: PingIdle, ..) ->
          panic as "got unexpected PONG from server"
      }

      state
    }
    protocol.Msg(topic:, payload:, sid:, ..) -> {
      state |> dispatch_message(sid, topic, [], payload)
    }
    protocol.Hmsg(topic:, payload:, sid:, headers:, ..) -> {
      state |> dispatch_message(sid, topic, headers, payload)
    }
  }
}

fn dispatch_message(state: State, sid, topic, headers, payload) {
  case dict.get(state.subscribers, sid) {
    Ok(subscription) -> {
      case subscription.callback(Message(topic, headers, payload)) {
        Ok(_) -> state
        Error(_) -> update_subscribers(state, dict.delete(_, sid))
      }
    }
    Error(_) -> state
  }
}

fn disconnect(state: State) {
  case state.conn {
    Disconnected -> state
    Connected(..) -> {
      process.send_after(state.self, 5000, Connect)
      State(..state, conn: Disconnected)
    }
  }
}

fn update_subscribers(state: State, update: fn(Subscribers) -> Subscribers) {
  State(..state, subscribers: update(state.subscribers))
}

fn resubscribe(socket: mug.Socket, subscribers: Dict(String, Subscription)) {
  use acc, sid, susbcription <- dict.fold(subscribers, Ok(socket))
  case acc {
    Ok(socket) -> {
      mug.send(socket, <<
        "SUB ",
        susbcription.topic:utf8,
        " ",
        sid:utf8,
        "\r\n",
      >>)
      |> result.replace(socket)
    }
    Error(err) -> Error(err)
  }
}

fn register_subscriber(state: State, topic, callback) {
  let sid = state.next_sid |> int.to_string
  let state =
    update_subscribers(state, dict.insert(_, sid, Subscription(topic, callback)))

  #(sid, State(..state, next_sid: state.next_sid + 1))
}

fn tcp_send_bits(state: State, data: BitArray, update_state: fn(State) -> State) {
  case state.conn {
    Connected(socket:, ..) -> {
      case mug.send(socket, data) {
        Ok(Nil) -> update_state(state)
        Error(_) -> disconnect(state)
      }
    }
    Disconnected -> state
  }
}

fn setup_connection(config: Config) {
  mug.new(config.host, config.port)
  |> mug.connect()
}

// -------------------------------- [ PUBLIC API ] --------------------------------------
pub fn publish_bits(subject: Subject(Message), topic, payload) {
  actor.call(subject, 5000, Publish(topic:, payload:, reply: _, headers: []))
}

pub fn subscribe(subject: Subject(Message), topic: String, callback: Callback) {
  actor.send(subject, Subscribe(topic, callback))
}

pub fn shutdown(subject: Subject(Message)) {
  actor.send(subject, Shutdown)
}

pub fn is_connected(subject: Subject(Message)) {
  actor.call(subject, 5000, IsConnected)
}
