import gleam/dict
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/io
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import gleam/string
import mug
import nuts/connect_options
import nuts/internal/command
import nuts/internal/protocol.{type ServerInfo}

pub opaque type Config {
  Config(
    host: String,
    port: Int,
    client_name: Option(String),
    nkey_seed: Option(String),
    logger: fn(String) -> Nil,
  )
}

fn default_logger(value: String) {
  io.println("log:" <> value)
  Nil
}

pub fn new(host: String, port: Int) -> Config {
  Config(host, port, client_name: None, nkey_seed: None, logger: default_logger)
}

pub fn client_name(config, client_name: String) {
  Config(..config, client_name: Some(client_name))
}

pub fn nkey_seed(config, nkey: String) {
  Config(..config, nkey_seed: Some(nkey))
}

pub fn logger(config: Config, logger: fn(String) -> Nil) -> Config {
  Config(..config, logger: logger)
}

pub type NatsError {
  ConnectionError(mug.Error)
  NatsError(String)
  NotConnected
}

pub opaque type Message {
  SetupConnection
  PacketData(mug.TcpMessage)
  Publish(NatsMessage, reply: Subject(Result(Nil, NatsError)))
  Subscribe(
    subject: String,
    subscriber_subject: Subject(ReceivedMessage),
    reply: Subject(Result(Subject(ReceivedMessage), Nil)),
  )
  SubscriberDown(process.Down)
  Shutdown(reply: Subject(Result(Nil, NatsError)))
  IsConnected(reply: Subject(Bool))
}

type State {
  State(
    self: Subject(Message),
    config: Config,
    log: fn(String) -> Nil,
    socket: Option(mug.Socket),
    buffer: BitArray,
    next_sid: Int,
    subscribers: dict.Dict(String, Subscription),
    authenticated: Bool,
    server_info: Option(ServerInfo),
  )
}

type Subscription {
  Subscription(
    sid: String,
    subject: Subject(ReceivedMessage),
    monitor: process.Monitor,
  )
}

pub fn start(config: Config) {
  actor.new_with_initialiser(1000, fn(self) {
    process.send(self, SetupConnection)

    actor.initialised(State(
      self,
      config:,
      log: config.logger,
      socket: None,
      buffer: <<>>,
      next_sid: 1,
      subscribers: dict.new(),
      authenticated: False,
      server_info: None,
    ))
    |> actor.returning(self)
    |> actor.selecting(
      process.new_selector()
      |> process.select(self)
      |> mug.select_tcp_messages(PacketData)
      |> process.select_monitors(SubscriberDown),
    )
    |> Ok
  })
  |> actor.on_message(handle_message)
  |> actor.start()
}

fn handle_message(state: State, msg: Message) {
  //echo #(msg, state) as "handle_message"
  case msg {
    SetupConnection -> {
      state.log("setting up connection")
      let socket_res =
        mug.new(state.config.host, state.config.port)
        |> mug.connect()

      case socket_res {
        Error(error) -> {
          state.log("failed to make connection\n" <> string.inspect(error))
          state |> actor.continue()
        }
        Ok(socket) -> {
          mug.receive_next_packet_as_message(socket)
          State(..state, socket: Some(socket), authenticated: False)
          |> actor.continue()
        }
      }
    }

    PacketData(mug.Packet(socket, bit_array)) -> {
      let buffer = <<state.buffer:bits, bit_array:bits>>
      mug.receive_next_packet_as_message(socket)

      let state = handle_server_messages(state, buffer)

      actor.continue(state)
    }
    PacketData(mug.SocketClosed(_socket)) -> {
      actor.continue(State(..state, socket: None))
    }
    PacketData(mug.TcpError(_socket, error)) -> {
      state.log("tcp error: " <> string.inspect(error))
      actor.continue(State(..state, socket: None))
    }
    Publish(message, reply_to) -> {
      case state.socket, state.authenticated {
        None, _ -> {
          state.log("socket not connected")
          actor.send(reply_to, Error(NotConnected))
          actor.continue(state)
        }
        Some(_), False -> {
          state.log("socket not authenticated yet")
          actor.send(reply_to, Error(NatsError("socket not authenticated")))
          actor.continue(state)
        }
        Some(_), _ -> {
          let bits = case message.headers {
            [] ->
              command.pub_(
                subject: message.subject,
                reply_to: message.reply_to,
                payload: message.payload,
              )
            headers ->
              command.hpub(
                subject: message.subject,
                reply_to: message.reply_to,
                headers: headers,
                payload: message.payload,
              )
          }

          case send_or_disconnect(state, bits) {
            Error(#(error, state)) -> {
              state.log("mug error: " <> string.inspect(error))
              actor.send(reply_to, Error(error))
              actor.continue(state)
            }
            Ok(state) -> {
              actor.send(reply_to, Ok(Nil))
              actor.continue(state)
            }
          }
        }
      }
    }

    Subscribe(subject:, subscriber_subject:, reply:) -> {
      let sid_str = int.to_string(state.next_sid)
      let state = case
        send_or_disconnect(
          state,
          command.sub(subject: subject, sid: sid_str, queue_group: None),
        )
      {
        Error(#(err, state)) -> {
          state.log("error while subscribing: " <> string.inspect(err))
          state
        }
        Ok(state) -> state
      }

      let subscribers = case process.subject_owner(subscriber_subject) {
        Error(_) -> state.subscribers
        Ok(pid) -> {
          let monitor = process.monitor(pid)
          dict.insert(
            state.subscribers,
            sid_str,
            Subscription(sid: sid_str, subject: subscriber_subject, monitor:),
          )
        }
      }

      actor.send(reply, Ok(subscriber_subject))
      actor.continue(State(..state, next_sid: state.next_sid + 1, subscribers:))
    }
    SubscriberDown(down) -> {
      let subscribers =
        dict.filter(state.subscribers, fn(sid, subscription) {
          case subscription.monitor == down.monitor {
            False -> True
            True -> {
              let _ = case state.socket {
                None -> Ok(Nil)
                Some(socket) -> {
                  mug.send(socket, command.unsub(sid, None))
                }
              }
              False
            }
          }
        })

      actor.continue(State(..state, subscribers:))
    }
    Shutdown(reply) -> {
      state.log("connection shutdown")

      case state.socket {
        None -> Ok(Nil)
        Some(socket) -> {
          socket
          |> mug.shutdown()
          |> result.map_error(ConnectionError)
        }
      }
      |> actor.send(reply, _)
      actor.stop()
    }
    IsConnected(reply:) -> {
      actor.send(reply, state.authenticated)
      actor.continue(state)
    }
  }
}

fn handle_server_messages(state: State, buffer) -> State {
  case protocol.parse(buffer) {
    protocol.Continue(cmd, buffer) -> {
      state.log("server msg: " <> string.inspect(cmd))
      let updated_state = handle_server_message(state, cmd)
      handle_server_messages(updated_state, buffer)
    }
    protocol.NeedsMoreData -> State(..state, buffer:)
    protocol.ProtocolError(err) -> {
      state.log("protocol error: " <> err)
      state
    }
  }
}

fn handle_server_message(state: State, msg: protocol.ServerMessage) -> State {
  case msg {
    protocol.OK -> {
      case state {
        State(authenticated: False, socket: Some(..), ..) -> {
          State(..state, authenticated: True)
        }
        _ -> state
      }
    }
    protocol.Info(server_info) -> {
      let connect_opts =
        connect_options.ConnectOptions(
          verbose: False,
          pedantic: False,
          tls_required: False,
          lang: "gleam",
          version: "0.0.1",
          headers: True,
          echo_: False,
          protocol: 0,
          auth: case state.config.nkey_seed, server_info.nonce {
            None, _ -> connect_options.NoAuth
            Some(nkey_seed), Some(nonce) -> {
              case connect_options.nkey_auth(nkey_seed, nonce) {
                Error(err) -> {
                  state.log(
                    "failed to authenticate with nkey " <> string.inspect(err),
                  )
                  connect_options.NoAuth
                }
                Ok(auth) -> auth
              }
            }
            _, _ -> connect_options.NoAuth
          },
          name: state.config.client_name
            |> option.unwrap("Gleam NUTS2"),
          no_responders: True,
        )

      case
        send_or_disconnect(
          State(..state, authenticated: True, server_info: Some(server_info)),
          command.connect(connect_opts),
        )
      {
        Error(#(error, state)) -> {
          state.log(string.inspect(error))
          state
        }
        Ok(state) -> state
      }
    }
    protocol.Msg(..) as m -> {
      dispatch_message(
        state,
        ReceivedMessage(
          sid: m.sid,
          conn: state.self,
          message: NatsMessage(
            subject: m.topic,
            headers: [],
            payload: m.payload,
            reply_to: m.reply_to,
          ),
        ),
      )
    }
    protocol.Hmsg(..) as m -> {
      dispatch_message(
        state,
        ReceivedMessage(
          sid: m.sid,
          conn: state.self,
          message: NatsMessage(
            subject: m.topic,
            headers: m.headers,
            payload: m.payload,
            reply_to: m.reply_to,
          ),
        ),
      )
    }

    srv_msg -> {
      state.log("unhandled server message:" <> string.inspect(srv_msg))
      state
    }
  }
}

fn dispatch_message(state: State, msg: ReceivedMessage) {
  case dict.get(state.subscribers, msg.sid) {
    Error(_) -> state
    Ok(subscription) -> {
      actor.send(subscription.subject, msg)
      state
    }
  }
}

fn send_or_disconnect(
  state: State,
  bits: BitArray,
) -> Result(State, #(NatsError, State)) {
  state.log("sending " <> string.inspect(bits))
  case state.socket {
    None -> Error(#(NotConnected, state))
    Some(socket) -> {
      case mug.send(socket, bits) {
        Error(err) -> {
          process.send_after(state.self, 1000, SetupConnection)
          Error(#(
            ConnectionError(err),
            State(
              ..state,
              authenticated: False,
              socket: None,
              server_info: None,
            ),
          ))
        }
        Ok(_) -> Ok(state)
      }
    }
  }
}

pub type NatsMessage {
  NatsMessage(
    subject: String,
    headers: List(#(String, String)),
    payload: BitArray,
    reply_to: Option(String),
  )
}

pub type ReceivedMessage {
  ReceivedMessage(sid: String, conn: Subject(Message), message: NatsMessage)
}

pub fn publish(subject: Subject(Message), message: NatsMessage) {
  actor.call(subject, 1000, Publish(message, _))
}

pub fn subscribe(
  conn: Subject(Message),
  nats_subbject: String,
) -> Result(Subject(ReceivedMessage), Nil) {
  let message_subject = process.new_subject()
  actor.call(conn, 5000, Subscribe(nats_subbject, message_subject, _))
}

pub fn new_message(subject: String, payload: BitArray) {
  NatsMessage(subject:, headers: [], payload:, reply_to: None)
}

pub fn reply_to(message: NatsMessage, reply_to: Option(String)) {
  NatsMessage(..message, reply_to:)
}

pub fn set_header(message: NatsMessage, key: String, value: String) {
  NatsMessage(..message, headers: [#(key, value), ..message.headers])
}

pub fn shutdown(conn: Subject(Message)) {
  actor.call(conn, 5000, Shutdown)
}

pub fn is_connected(conn: Subject(Message)) -> Bool {
  actor.call(conn, 5000, IsConnected)
}
