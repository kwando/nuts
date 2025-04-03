import gleam/bit_array
import gleam/dict
import gleam/erlang/process
import gleam/function
import gleam/int
import gleam/option
import gleam/otp/actor
import gleam/result
import mug
import nuts/connect_options
import nuts/internal/protocol

pub opaque type Config {
  Config(host: String, port: Int, retry_interval: Int)
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

type State {
  Connected(
    config: Config,
    subscribers: Subscribers,
    socket: mug.Socket,
    next_sid: Int,
    buffer: BitArray,
    self: process.Subject(Msg),
  )
  Disconnected(
    config: Config,
    subscribers: Subscribers,
    self: process.Subject(Msg),
    next_sid: Int,
  )
}

pub opaque type Msg {
  Connect
  Data(mug.TcpMessage)
  Publish(
    topic: String,
    payload: BitArray,
    reply: process.Subject(Result(Nil, Nil)),
  )
  Subscribe(topic: String, callback: Callback)
  IsConnected(process.Subject(Bool))
  Shutdown
}

pub fn new(host: String, port: Int) {
  Config(host:, port:, retry_interval: 1000)
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

      process.send(subject, Connect)
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
        Connect -> panic
        Publish(topic:, payload:, reply:) -> {
          let updated_state =
            tcp_send(state, protocol.Pub(topic:, payload:), function.identity)
          process.send(reply, Ok(Nil))
          actor.continue(updated_state)
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
        IsConnected(reply) -> {
          process.send(reply, True)
          actor.continue(state)
        }
      }
    }
    Disconnected(..) -> {
      case msg {
        Connect -> {
          case setup_connection(state.config) {
            Ok(socket) -> {
              mug.receive_next_packet_as_message(socket)
              case resubscribe(socket, state.subscribers) {
                Ok(socket) -> {
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
        Shutdown -> actor.Stop(process.Normal)
        IsConnected(reply) -> {
          process.send(reply, False)
          actor.continue(state)
        }
        Data(_) -> actor.continue(state)
        Publish(reply:, ..) -> {
          process.send(reply, Error(Nil))
          actor.continue(state)
        }
      }
    }
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
      case subscription.callback(Message(topic, headers, payload)) {
        Ok(_) -> state
        Error(_) -> update_subscribers(state, dict.delete(_, sid))
      }
    }
    Error(_) -> state
  }
}

fn disconnect(state: State) {
  case state {
    Connected(subscribers:, config:, self:, next_sid:, ..) ->
      Disconnected(subscribers:, config:, self:, next_sid:)
    Disconnected(..) -> state
  }
}

fn update_subscribers(state: State, update: fn(Subscribers) -> Subscribers) {
  case state {
    Connected(subscribers:, ..) ->
      Connected(..state, subscribers: update(subscribers))
    Disconnected(subscribers:, ..) ->
      Disconnected(..state, subscribers: update(subscribers))
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

fn register_subscriber(state: State, topic, callback) {
  let sid = state.next_sid |> int.to_string
  let state =
    update_subscribers(state, dict.insert(_, sid, Subscription(topic, callback)))

  case state {
    Connected(..) -> #(sid, Connected(..state, next_sid: state.next_sid + 1))
    Disconnected(..) -> #(
      sid,
      Disconnected(..state, next_sid: state.next_sid + 1),
    )
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
pub fn publish_bits(subject: process.Subject(Msg), topic, payload) {
  actor.call(subject, Publish(topic:, payload:, reply: _), 5000)
}

pub fn subscribe(
  subject: process.Subject(Msg),
  topic: String,
  callback: Callback,
) {
  actor.send(subject, Subscribe(topic, callback))
}

pub fn shutdown(subject: process.Subject(Msg)) {
  actor.send(subject, Shutdown)
}

pub fn is_connected(subject: process.Subject(Msg)) {
  actor.call(subject, IsConnected, 5000)
}
