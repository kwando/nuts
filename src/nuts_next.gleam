import gleam/bit_array
import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/otp/supervision
import gleam/result
import gleam/string
import mug
import nuts/connect_options.{ConnectOptions}
import nuts/internal/command
import nuts/internal/protocol.{type ServerInfo}

pub opaque type Options {
  Options(
    host: String,
    port: Int,
    nkey_seed: Option(String),
    reconnect_interval: Int,
  )
}

pub fn new(host, port) {
  Options(host:, port:, nkey_seed: None, reconnect_interval: 1000)
}

pub fn nkey_seed(options: Options, string: String) -> Options {
  Options(..options, nkey_seed: Some(string))
}

pub type NatsMessage {
  NatsMessage(
    subject: String,
    reply_to: Option(String),
    headers: List(#(String, String)),
    payload: BitArray,
  )
}

pub type NatsError {
  NotConnected
  NetworkError(mug.Error)
  ProtocolError(String)
  AuthenticationFailed
  GenericError(String)
}

pub opaque type Message {
  Connect
  IsConnected(Subject(Bool))
  Publish(NatsMessage, Subject(Result(Nil, NatsError)))
  Subscribe(
    subject: String,
    subscriber: Subject(NatsMessage),
    reply: Subject(Result(Subscription, NatsError)),
  )
  SocketData(mug.TcpMessage)
  SubscriberDown(process.Down)
  Unsubscribe(Subscription, Subject(Result(Nil, NatsError)))
  Shutdown(Subject(Nil))
}

type Subscriber {
  Subscriber(
    sid: String,
    monitor: process.Monitor,
    subject: Subject(NatsMessage),
    pattern: String,
  )
}

pub opaque type Subscription {
  Subscription(subscriber: Subscriber)
}

pub fn get_subject(subscription: Subscription) {
  subscription.subscriber.subject
}

// ------------------------------------------- SubscriberMap -------------------------------------------
type SubscriberMap {
  SubscriberMap(
    sids: Dict(String, Subscriber),
    monitors: Dict(process.Monitor, Subscriber),
  )
}

fn new_subscriber_map() {
  SubscriberMap(sids: dict.new(), monitors: dict.new())
}

fn add_subscriber(map: SubscriberMap, subscriber: Subscriber) {
  assert !dict.has_key(map.sids, subscriber.sid)
    as { "sid " <> subscriber.sid <> " already registered" }
  assert !dict.has_key(map.monitors, subscriber.monitor)
    as "monitor already registered"
  SubscriberMap(
    sids: dict.insert(map.sids, subscriber.sid, subscriber),
    monitors: dict.insert(map.monitors, subscriber.monitor, subscriber),
  )
}

fn remove_subscriber(map: SubscriberMap, subscriber: Subscriber) {
  SubscriberMap(
    sids: dict.delete(map.sids, subscriber.sid),
    monitors: dict.delete(map.monitors, subscriber.monitor),
  )
}

fn get_subscriber_by_sid(
  map: SubscriberMap,
  sid: String,
) -> Result(Subscriber, Nil) {
  dict.get(map.sids, sid)
}

fn get_subscriber_by_monitor(
  map: SubscriberMap,
  monitor: process.Monitor,
) -> Result(Subscriber, Nil) {
  dict.get(map.monitors, monitor)
}

fn update_subscribers(
  state: ClientState,
  callback: fn(SubscriberMap) -> SubscriberMap,
) {
  ClientState(..state, subscribers: callback(state.subscribers))
}

//------------------------------------------- NatsConnection -------------------------------------------

type ClientState {
  ClientState(
    options: Options,
    socket: Option(mug.Socket),
    buffer: BitArray,
    server_info: Option(ServerInfo),
    subscribers: SubscriberMap,
    next_sid: Int,
    self: Subject(Message),
  )
}

pub fn start(process_name: process.Name(Message), options: Options) {
  actor.new_with_initialiser(1000, fn(self) {
    actor.send(self, Connect)

    actor.initialised(ClientState(
      options,
      socket: None,
      buffer: <<>>,
      server_info: None,
      subscribers: new_subscriber_map(),
      next_sid: 1,
      self:,
    ))
    |> actor.selecting(
      process.new_selector()
      |> process.select(self)
      |> process.select_monitors(SubscriberDown)
      |> mug.select_tcp_messages(SocketData),
    )
    |> Ok
  })
  |> actor.named(process_name)
  |> actor.on_message(handle_message)
  |> actor.start
}

pub fn supervised(
  name: process.Name(Message),
  options: Options,
) -> supervision.ChildSpecification(Nil) {
  supervision.worker(fn() { start(name, options) })
}

fn handle_message(
  state: ClientState,
  msg: Message,
) -> actor.Next(ClientState, a) {
  case msg {
    Connect -> {
      state
      |> setup_connection
      |> actor.continue
    }
    IsConnected(reply) -> {
      actor.send(reply, state.socket != None && state.server_info != None)
      actor.continue(state)
    }
    Publish(msg, reply) -> {
      case state.socket {
        Some(_) -> {
          let bytes = case msg.headers {
            [] -> command.pub_(msg.subject, msg.reply_to, msg.payload)

            headers ->
              command.hpub(msg.subject, msg.reply_to, headers, msg.payload)
          }

          case send_bits(state, bytes) {
            Ok(_) -> {
              actor.send(reply, Ok(Nil))
              actor.continue(state)
            }
            Error(error) -> {
              actor.send(reply, Error(error))
              state
              |> close_connection()
              |> actor.continue()
            }
          }
        }
        None -> {
          actor.send(reply, Error(NotConnected))
          actor.continue(state)
        }
      }
    }
    SocketData(mug_message) ->
      case mug_message {
        mug.Packet(socket, data) -> {
          mug.receive_next_packet_as_message(socket)
          let buffer = bit_array.append(state.buffer, data)
          case parse_server_messages(state, buffer) {
            Ok(state) -> actor.continue(state)
            Error(err) -> {
              io.println_error("error: " <> string.inspect(err))

              state
              |> close_connection
              |> actor.continue
            }
          }
        }
        mug.SocketClosed(_) | mug.TcpError(_, _) ->
          state
          |> close_connection
          |> actor.continue
      }
    Subscribe(subject:, subscriber:, reply:) -> {
      let sid = state.next_sid |> int.to_base36
      case process.subject_owner(subscriber) {
        Ok(pid) -> {
          let monitor = process.monitor(pid)

          case
            send_bits(state, command.sub(subject:, sid:, queue_group: None))
          {
            Ok(Nil) | Error(NotConnected) -> {
              let subscriber =
                Subscriber(
                  sid:,
                  monitor:,
                  subject: subscriber,
                  pattern: subject,
                )
              actor.send(reply, Ok(Subscription(subscriber:)))
              let state =
                state
                |> update_subscribers(add_subscriber(_, subscriber))
              let state = ClientState(..state, next_sid: state.next_sid + 1)
              actor.continue(state)
            }
            Error(err) -> {
              actor.send(reply, Error(err))
              actor.continue(state)
            }
          }
        }
        // the process is is already dead, so no idea to setup a subscription
        Error(Nil) -> {
          actor.send(reply, Error(GenericError("subscriber is down")))
          actor.continue(state)
        }
      }
    }
    SubscriberDown(down) -> {
      case get_subscriber_by_monitor(state.subscribers, down.monitor) {
        Ok(subscriber) -> {
          let _ = send_bits(state, command.unsub(subscriber.sid, None))

          state
          |> update_subscribers(remove_subscriber(_, subscriber))
          |> actor.continue()
        }
        Error(_) -> actor.continue(state)
      }
    }
    Unsubscribe(subscription, reply) -> {
      case
        get_subscriber_by_sid(state.subscribers, subscription.subscriber.sid)
      {
        Ok(subscriber) -> {
          case send_bits(state, command.unsub(subscriber.sid, None)) {
            Ok(_) | Error(NotConnected) -> {
              actor.send(reply, Ok(Nil))
              actor.continue(
                state
                |> update_subscribers(remove_subscriber(_, subscriber)),
              )
            }
            Error(_) -> {
              actor.send(reply, Ok(Nil))
              actor.continue(
                state
                |> update_subscribers(remove_subscriber(_, subscriber)),
              )
            }
          }
        }
        Error(_) -> {
          actor.send(reply, Ok(Nil))
          actor.continue(state)
        }
      }
    }
    Shutdown(reply) -> {
      actor.send(reply, Nil)
      actor.stop()
    }
  }
}

fn close_connection(state: ClientState) {
  case state.socket {
    Some(socket) -> {
      let _ = mug.shutdown(socket)

      ClientState(..state, socket: None, server_info: None)
      |> schedule_reconnect()
    }
    None -> state
  }
}

fn schedule_reconnect(state: ClientState) {
  process.send_after(state.self, state.options.reconnect_interval, Connect)
  state
}

fn parse_server_messages(state, buffer) -> Result(ClientState, NatsError) {
  case protocol.parse(buffer) {
    protocol.Continue(message, remaining_bytes) -> {
      let state = ClientState(..state, buffer: remaining_bytes)
      case handle_server_message(state, message) {
        Ok(state) -> parse_server_messages(state, remaining_bytes)
        Error(err) -> Error(err)
      }
    }
    protocol.ProtocolError(error) -> Error(ProtocolError(error))
    protocol.NeedsMoreData -> ClientState(..state, buffer:) |> Ok
  }
}

fn send_bits(state: ClientState, bits: BitArray) {
  case state.socket {
    Some(socket) -> {
      mug.send(socket, bits)
      |> result.map_error(NetworkError)
    }
    None -> Error(NotConnected)
  }
}

fn handle_server_message(
  state: ClientState,
  message: protocol.ServerMessage,
) -> Result(ClientState, NatsError) {
  case message {
    protocol.Info(server_info) -> {
      let auth = case state.options.nkey_seed, server_info.nonce {
        Some(nkey), Some(nonce) -> connect_options.nkey_auth(nkey, nonce)
        _, _ -> Ok(connect_options.NoAuth)
      }

      case auth {
        Ok(auth) -> {
          case
            send_bits(
              state,
              command.connect(ConnectOptions(
                verbose: False,
                pedantic: True,
                tls_required: False,
                lang: "gleam",
                version: "0.0.1",
                headers: True,
                echo_: True,
                protocol: 0,
                auth:,
                name: "nuts",
                no_responders: True,
              )),
            )
          {
            Ok(_) -> {
              ClientState(..state, server_info: Some(server_info))
              |> resubscribe
            }
            Error(err) -> Error(err)
          }
        }
        Error(err) -> {
          io.println_error(string.inspect(err))
          Error(AuthenticationFailed)
        }
      }
    }
    protocol.Ping -> {
      case send_bits(state, command.pong()) {
        Ok(_) -> Ok(state)
        Error(err) -> Error(err)
      }
    }
    protocol.Pong -> Ok(state)
    protocol.Msg(topic:, sid:, reply_to:, payload:) -> {
      broadcast_message(
        state,
        sid,
        NatsMessage(subject: topic, reply_to:, headers: [], payload:),
      )
    }
    protocol.Hmsg(topic:, headers:, sid:, reply_to:, payload:) -> {
      broadcast_message(
        state,
        sid,
        NatsMessage(subject: topic, reply_to:, headers:, payload:),
      )
    }
    protocol.OK -> Ok(state)
    protocol.ERR(error) -> Error(ProtocolError(error))
  }
}

fn resubscribe(state: ClientState) -> Result(ClientState, NatsError) {
  let subscribers = state.subscribers.sids |> dict.values

  let command =
    list.fold(subscribers, <<>>, fn(acc, subscriber) {
      bit_array.append(
        acc,
        command.sub(
          subject: subscriber.pattern,
          sid: subscriber.sid,
          queue_group: None,
        ),
      )
    })

  case command {
    <<>> -> Ok(state)
    command ->
      send_bits(state, command)
      |> result.replace(state)
  }
}

fn broadcast_message(state: ClientState, sid: String, nats_message: NatsMessage) {
  case get_subscriber_by_sid(state.subscribers, sid) {
    Ok(subscriber) -> {
      actor.send(subscriber.subject, nats_message)
      Ok(state)
    }

    Error(Nil) -> {
      io.println_error(
        "warning: message received but but no subscription found",
      )
      Ok(state)
    }
  }
}

fn setup_connection(state: ClientState) {
  let socket =
    mug.new(state.options.host, state.options.port)
    |> mug.connect

  case socket {
    Ok(socket) -> {
      mug.receive_next_packet_as_message(socket)
      ClientState(..state, socket: Some(socket))
    }
    Error(_) -> {
      ClientState(..state, socket: None)
      |> schedule_reconnect
    }
  }
}

// message API
pub fn new_message(subject: String, payload: BitArray) -> NatsMessage {
  NatsMessage(subject:, payload:, headers: [], reply_to: None)
}

pub fn reply_to(message: NatsMessage, reply_to: Option(String)) {
  NatsMessage(..message, reply_to:)
}

pub fn add_header(message: NatsMessage, header: String, value: String) {
  NatsMessage(..message, headers: [#(header, value), ..message.headers])
}

// --- client API
pub fn is_connected(subject: Subject(Message)) -> Bool {
  actor.call(subject, 5000, IsConnected)
}

pub fn publish(
  subject: Subject(Message),
  message: NatsMessage,
) -> Result(Nil, NatsError) {
  actor.call(subject, 5000, Publish(message, _))
}

pub fn subscribe(
  conn: Subject(Message),
  nats_subject: String,
) -> Result(Subscription, NatsError) {
  let subscriber = process.new_subject()
  actor.call(conn, 5000, Subscribe(nats_subject, subscriber:, reply: _))
}

pub fn unsubscribe(conn: Subject(Message), subscription: Subscription) {
  actor.call(conn, 5000, Unsubscribe(subscription, _))
}

pub fn shutdown(conn: Subject(Message)) -> Nil {
  actor.call(conn, 5000, Shutdown)
}
