import gleam/bit_array
import gleam/dict
import gleam/erlang/process
import gleam/function
import gleam/int
import gleam/io
import gleam/option
import gleam/otp/actor
import gleam/result
import mug
import nuts/connect_options
import nuts/internal/protocol

pub type Config {
  Config(host: String, port: Int)
}

type Subscription {
  Subscription(topic: String, callback: Callback)
}

type Callback =
  fn(String, List(#(String, String)), BitArray) -> Result(Nil, Nil)

type State {
  Connected(
    config: Config,
    subscribers: dict.Dict(String, Subscription),
    socket: mug.Socket,
    next_sid: Int,
    buffer: BitArray,
    self: process.Subject(Message),
  )
  Disconnected(
    config: Config,
    subscribers: dict.Dict(String, Subscription),
    self: process.Subject(Message),
    next_sid: Int,
  )
}

pub opaque type Message {
  Reconnect
  Data(mug.TcpMessage)
  Publish(topic: String, payload: BitArray, reply: process.Subject(Nil))
  Subscribe(topic: String, callback: Callback)
  Shutdown
}

pub fn start(config: Config) {
  actor.start_spec(actor.Spec(
    init: fn() {
      let subject = process.new_subject()
      let selector =
        process.new_selector()
        |> process.selecting(subject, function.identity)
        |> mug.selecting_tcp_messages(Data)

      let state = Disconnected(config, dict.new(), self: subject, next_sid: 1)

      process.send(subject, Reconnect)
      actor.Ready(selector:, state:)
    },
    init_timeout: 1000,
    loop: handle_message,
  ))
}

fn handle_message(msg, state: State) {
  case state {
    Connected(..) as state -> {
      case msg {
        Data(tcp) -> {
          mug.receive_next_packet_as_message(state.socket)
          case tcp {
            mug.Packet(_socket, data) -> {
              let buffer = bit_array.append(state.buffer, data)
              case read_protocol_messages(state, buffer) {
                Ok(state) -> state
                Error(_) -> disconnect(state)
              }
              |> actor.continue
            }
            mug.SocketClosed(_) -> {
              io.println("SOCKET CLOSED")
              process.send_after(state.self, 3000, Reconnect)
              actor.continue(disconnect(state))
            }
            mug.TcpError(_, _) -> {
              io.println("SOCKET ERROR")
              process.send_after(state.self, 3000, Reconnect)
              actor.continue(disconnect(state))
            }
          }
        }
        Reconnect -> panic
        Publish(topic:, payload:, reply:) -> {
          process.send(reply, Nil)
          actor.continue(tcp_send(
            state,
            protocol.Pub(topic:, payload:),
            function.identity,
          ))
        }
        Subscribe(topic, callback) -> {
          let #(sid, updated_state) =
            register_subscriber(state, topic, callback)

          case
            mug.send(
              state.socket,
              protocol.Sub(topic:, sid:, queue_group: option.None)
                |> protocol.cmd_to_bits,
            )
          {
            Ok(Nil) -> actor.continue(updated_state)
            Error(_) -> actor.continue(disconnect(state))
          }
        }
        Shutdown -> actor.Stop(process.Normal)
      }
    }
    Disconnected(..) -> {
      case msg {
        Reconnect -> {
          io.println("setup connection")
          case setup_connection(state.config) {
            Ok(socket) -> {
              mug.receive_next_packet_as_message(socket)
              case resubscribe(socket, state.subscribers) {
                Ok(socket) -> {
                  io.println("connection success")
                  Connected(
                    config: state.config,
                    socket:,
                    buffer: <<>>,
                    next_sid: 1,
                    subscribers: state.subscribers,
                    self: state.self,
                  )
                  |> actor.continue
                }
                Error(_err) -> {
                  actor.continue(state)
                }
              }
            }
            Error(_) -> {
              process.send_after(state.self, 3000, Reconnect)
              actor.continue(state)
            }
          }
        }
        Subscribe(topic:, callback:) -> {
          let #(_sid, state) = register_subscriber(state, topic, callback)
          actor.continue(state)
        }
        Shutdown -> actor.Stop(process.Normal)
        _ -> actor.continue(state)
      }
    }
  }
}

fn register_subscriber(state: State, topic, callback) {
  let sid = state.next_sid |> int.to_string
  let subscribers =
    dict.insert(state.subscribers, sid, Subscription(topic, callback))

  case state {
    Connected(..) -> #(
      sid,
      Connected(..state, next_sid: state.next_sid + 1, subscribers:),
    )
    Disconnected(..) -> #(
      sid,
      Disconnected(..state, next_sid: state.next_sid + 1, subscribers:),
    )
  }
}

fn read_protocol_messages(state: State, buffer: BitArray) -> Result(State, Nil) {
  case protocol.parse(buffer) {
    protocol.Continue(msg, buffer) ->
      read_protocol_messages(handler_server_message(state, msg), buffer)
    protocol.NeedsMoreData -> {
      case state {
        Connected(..) -> Ok(Connected(..state, buffer: buffer))
        _ -> Ok(state)
      }
    }
    protocol.ProtocolError(..) -> Error(Nil)
  }
}

fn handler_server_message(state, msg) {
  case msg {
    protocol.Info(..) -> {
      use _ <- tcp_send(
        state,
        protocol.Connect([
          connect_options.Verbose(False),
          connect_options.Version("0.0.0"),
          connect_options.Lang("gleam"),
          connect_options.Protocol(0),
          connect_options.TlsRequired(False),
        ]),
      )
      state
    }
    protocol.Ping -> {
      use _ <- tcp_send(state, protocol.ClientPong)
      state
    }
    protocol.Pong -> state
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
      let assert Ok(Nil) = subscription.callback(topic, headers, payload)
      state
    }
    Error(_) -> {
      io.println_error("GOT MSG FOR CLIENT WITHOUT CALLBACK")
      state
    }
  }
}

fn disconnect(state: State) {
  case state {
    Connected(subscribers:, config:, self:, next_sid:, ..) ->
      Disconnected(subscribers:, config:, self:, next_sid:)
    Disconnected(..) -> state
  }
}

fn resubscribe(socket: mug.Socket, subscribers: dict.Dict(String, Subscription)) {
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

fn tcp_send(
  state: State,
  data: protocol.Command,
  update_state: fn(State) -> State,
) {
  case state {
    Connected(socket:, ..) -> {
      case mug.send(socket, protocol.cmd_to_bits(data)) {
        Ok(Nil) -> update_state(state)
        Error(_) -> disconnect(state)
      }
    }
    Disconnected(..) -> state
  }
}

fn setup_connection(config: Config) {
  mug.new(config.host, config.port)
  |> mug.connect()
}

// -------------------------------- [ PUBLIC API ] --------------------------------------
pub fn publish_bits(subject: process.Subject(Message), topic, payload) {
  actor.call(subject, Publish(topic:, payload:, reply: _), 5000)
}

pub fn subscribe(
  subject: process.Subject(Message),
  topic: String,
  callback: Callback,
) {
  actor.send(subject, Subscribe(topic, callback))
}

pub fn shutdown(subject: process.Subject(Message)) {
  actor.send(subject, Shutdown)
}
