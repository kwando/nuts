import gleam/bit_array
import gleam/bool
import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/erlang/reference.{type Reference}
import gleam/int
import gleam/io
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/otp/supervision
import gleam/result
import gleam/string
import gleam/uri
import mug
import nuts/connect_options.{ConnectOptions}
import nuts/inbox
import nuts/internal/command
import nuts/internal/protocol.{type ParserConfig, type ServerInfo, ParserConfig}

const actor_timeout = 5000

pub opaque type Options {
  Options(
    host: String,
    port: Int,
    nkey_seed: Option(String),
    reconnect_interval: Int,
    reconnect_max: Int,
    logger: Logger,
    ping_interval: Int,
    ping_timeout: Int,
  )
}

pub fn new(host: String, port: Int) -> Options {
  Options(
    host:,
    port:,
    nkey_seed: None,
    reconnect_interval: 1000,
    reconnect_max: 30_000,
    logger: noop_logger(),
    ping_interval: 300_000,
    ping_timeout: 30_000,
  )
}

pub fn nkey_seed(options: Options, string: String) -> Options {
  Options(..options, nkey_seed: Some(string))
}

pub fn with_logger(options: Options, logger: Logger) {
  Options(..options, logger:)
}

pub fn with_ping_interval(options: Options, ms: Int) -> Options {
  Options(..options, ping_interval: ms)
}

pub fn with_ping_timeout(options: Options, ms: Int) -> Options {
  Options(..options, ping_timeout: ms)
}

pub fn with_reconnect(options: Options, initial: Int, max: Int) -> Options {
  Options(..options, reconnect_interval: initial, reconnect_max: max)
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
  BadURL
  JsonDecodeError(json.DecodeError, BitArray)
}

pub opaque type Message {
  Connect
  IsConnected(Subject(Bool))
  Publish(NatsMessage, Subject(Result(Nil, NatsError)))
  Subscribe(
    subject: String,
    subscriber: Subject(NatsMessage),
    reply: Subject(Result(Subscription, NatsError)),
    queue_group: Option(String),
  )
  SocketData(mug.TcpMessage)
  SubscriberDown(process.Down)
  Unsubscribe(Subscription, Subject(Result(Nil, NatsError)))
  Shutdown(Subject(Nil))
  Flush(Subject(Result(Nil, NatsError)))
  Request(
    subject: String,
    headers: List(#(String, String)),
    payload: BitArray,
    timeout: Int,
    reply: Subject(Result(NatsMessage, NatsError)),
  )
  RequestTimeout(inbox: String)
  PingInterval(ping_ref: Reference)
  PingTimeout(ping_ref: Reference)
}

type Subscriber {
  Subscriber(
    sid: String,
    monitor: process.Monitor,
    subject: Subject(NatsMessage),
    pattern: String,
    queue_group: Option(String),
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
    parser_config: ParserConfig,
    subscribers: SubscriberMap,
    next_sid: Int,
    self: Subject(Message),
    logger: Logger,
    connection_attempt: Int,
    request_map: Dict(String, Subject(Result(NatsMessage, NatsError))),
    inbox_prefix: String,
    ping_ref: Option(Reference),
    flush_reply: Option(Subject(Result(Nil, NatsError))),
  )
}

pub fn start(process_name: process.Name(Message), options: Options) {
  actor.new_with_initialiser(actor_timeout, fn(self) {
    actor.send(self, Connect)

    actor.initialised(ClientState(
      options,
      socket: None,
      buffer: <<>>,
      server_info: None,
      parser_config: protocol.default_parser_config(),
      subscribers: new_subscriber_map(),
      next_sid: 1,
      self:,
      logger: options.logger,
      connection_attempt: 1,
      request_map: dict.new(),
      inbox_prefix: inbox.generate_prefix(),
      ping_ref: None,
      flush_reply: None,
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

pub fn from_url(url: String) {
  case uri.parse(url) {
    Ok(uri) ->
      new(
        uri.host |> option.unwrap("127.0.0.1"),
        uri.port |> option.unwrap(4222),
      )
      |> Ok
    Error(_) -> Error(BadURL)
  }
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
              state.logger.debug(
                "parse_server_messages: " <> string.inspect(err),
              )

              state
              |> close_connection
              |> actor.continue
            }
          }
        }
        mug.SocketClosed(_) | mug.TcpError(_, _) -> {
          state.logger.debug("socket closed")
          state
          |> close_connection
          |> actor.continue
        }
      }
    Subscribe(subject:, subscriber:, reply:, queue_group:) -> {
      let sid = state.next_sid |> int.to_base36
      case process.subject_owner(subscriber) {
        Ok(pid) -> {
          let monitor = process.monitor(pid)

          let state = ClientState(..state, next_sid: state.next_sid + 1)
          let subscriber =
            Subscriber(
              sid:,
              monitor:,
              subject: subscriber,
              pattern: subject,
              queue_group:,
            )
          actor.send(reply, Ok(Subscription(subscriber:)))
          let state =
            state
            |> update_subscribers(add_subscriber(_, subscriber))

          case send_bits(state, command.sub(subject:, sid:, queue_group:)) {
            Ok(Nil) -> {
              actor.continue(state)
            }
            Error(err) -> {
              state.logger.warning(
                "failed setup subscription on server: " <> string.inspect(err),
              )
              state
              |> close_connection()
              |> actor.continue
            }
          }
        }
        // the process is already dead, so no idea to setup a subscription
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
              actor.send(reply, Error(GenericError("subscriber not found")))
              actor.continue(
                close_connection(state)
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
    Flush(reply) -> {
      case state.socket {
        Some(_) -> {
          case send_bits(state, command.ping()) {
            Ok(_) ->
              actor.continue(ClientState(..state, flush_reply: Some(reply)))
            Error(_) -> {
              actor.send(reply, Error(NotConnected))
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
    Request(subject:, headers:, payload:, reply:, timeout:) -> {
      let inbox =
        inbox.new_inbox(state.inbox_prefix, state.next_sid |> int.to_base36)

      case state.socket {
        Some(_) -> {
          let bytes = case headers {
            [] -> command.pub_(subject, Some(inbox), payload)
            headers -> command.hpub(subject, Some(inbox), headers, payload)
          }

          case send_bits(state, bytes) {
            Ok(_) -> {
              process.send_after(state.self, timeout, RequestTimeout(inbox))

              state
              |> register_request(inbox, reply)
              |> increment_sid
              |> actor.continue
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
    RequestTimeout(inbox:) -> {
      case dict.get(state.request_map, inbox) {
        Ok(subject) -> {
          process.send(subject, Error(GenericError("request timeout")))
          ClientState(
            ..state,
            request_map: dict.delete(state.request_map, inbox),
          )
        }
        Error(_) -> state
      }
      |> actor.continue()
    }
    PingInterval(ping_ref) -> {
      use <- bool.guard(
        when: state.ping_ref != Some(ping_ref),
        return: actor.continue(state),
      )
      case state.socket {
        Some(_) -> {
          case send_bits(state, command.ping()) {
            Ok(_) -> {
              process.send_after(
                state.self,
                state.options.ping_timeout,
                PingTimeout(ping_ref),
              )
              actor.continue(state)
            }
            Error(_) ->
              state
              |> close_connection()
              |> actor.continue()
          }
        }
        None -> actor.continue(state)
      }
    }
    PingTimeout(ping_ref) -> {
      use <- bool.guard(
        when: state.ping_ref != Some(ping_ref),
        return: actor.continue(state),
      )
      state.logger.warning("ping timeout: server did not respond")
      state
      |> close_connection()
      |> actor.continue()
    }
  }
}

fn increment_sid(client_state: ClientState) -> ClientState {
  ClientState(..client_state, next_sid: client_state.next_sid + 1)
}

fn register_request(
  client_state: ClientState,
  token: String,
  reply: Subject(Result(NatsMessage, NatsError)),
) -> ClientState {
  ClientState(
    ..client_state,
    request_map: client_state.request_map
      |> dict.insert(token, reply),
  )
}

fn close_connection(state: ClientState) {
  state.logger.debug("close connection")
  case state.socket {
    Some(socket) -> {
      let _ = mug.shutdown(socket)
      case state.flush_reply {
        Some(reply) -> actor.send(reply, Error(NotConnected))
        None -> Nil
      }

      ClientState(
        ..state,
        socket: None,
        server_info: None,
        parser_config: protocol.default_parser_config(),
        connection_attempt: 1,
        ping_ref: None,
        flush_reply: None,
      )
      |> setup_connection
    }
    None -> state
  }
}

fn schedule_reconnect(state: ClientState) {
  let delay =
    exponential_delay(
      state.options.reconnect_interval,
      state.options.reconnect_max,
      state.connection_attempt,
    )
  state.logger.debug("reconnect in " <> int.to_string(delay) <> "ms")
  process.send_after(state.self, delay, Connect)
  state
}

fn exponential_delay(initial: Int, max: Int, attempt: Int) -> Int {
  let base = case attempt {
    a if a <= 2 -> initial
    _ -> double_repeatedly(initial, attempt - 2, max)
  }
  let capped = int.min(base, max)
  let jitter = int.random(capped / 10 + 1)
  capped + jitter
}

fn double_repeatedly(value: Int, times: Int, cap: Int) -> Int {
  case times, value >= cap {
    0, _ -> value
    _, True -> cap
    _, _ -> double_repeatedly(value * 2, times - 1, cap)
  }
}

fn parse_server_messages(
  state: ClientState,
  buffer: BitArray,
) -> Result(ClientState, NatsError) {
  case protocol.parse(buffer, config: state.parser_config) {
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
  state.logger.debug(
    ">>"
    <> bit_array.to_string(bits) |> result.unwrap("<<binary is not a string>>"),
  )
  case state.socket {
    Some(socket) -> {
      socket
      |> mug.send(bits)
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
                protocol: 0,
                auth:,
                name: "nuts",
                no_responders: True,
              )),
            )
          {
            Ok(_) -> {
              let ping_ref = reference.new()
              process.send_after(
                state.self,
                state.options.ping_interval,
                PingInterval(ping_ref),
              )
              ClientState(
                ..state,
                server_info: Some(server_info),
                parser_config: ParserConfig(
                  max_payload: server_info.max_payload,
                ),
                ping_ref: Some(ping_ref),
              )
              |> resubscribe
            }
            Error(err) -> Error(err)
          }
        }
        Error(err) -> {
          state.logger.debug(string.inspect(err))
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
    protocol.Pong -> {
      let ping_ref = reference.new()
      process.send_after(
        state.self,
        state.options.ping_interval,
        PingInterval(ping_ref),
      )
      case state.flush_reply {
        Some(reply) -> {
          actor.send(reply, Ok(Nil))
          Ok(ClientState(..state, ping_ref: Some(ping_ref), flush_reply: None))
        }
        None -> Ok(ClientState(..state, ping_ref: Some(ping_ref)))
      }
    }
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
    list.fold(
      subscribers,
      command.sub(
        state.inbox_prefix <> "*",
        state.inbox_prefix,
        queue_group: None,
      ),
      fn(acc, subscriber) {
        bit_array.append(
          acc,
          command.sub(
            subject: subscriber.pattern,
            sid: subscriber.sid,
            queue_group: subscriber.queue_group,
          ),
        )
      },
    )

  send_bits(state, command)
  |> result.replace(state)
}

fn broadcast_message(
  state: ClientState,
  sid: String,
  nats_message: NatsMessage,
) {
  case string.starts_with(nats_message.subject, state.inbox_prefix) {
    True -> {
      case state.request_map |> dict.get(nats_message.subject) {
        Ok(response_subject) -> {
          process.send(response_subject, Ok(nats_message))

          ClientState(
            ..state,
            request_map: dict.delete(state.request_map, nats_message.subject),
          )
          |> Ok
        }
        Error(_) -> {
          state.logger.warning(
            "got message for inbox but no responder available",
          )
          Ok(state)
        }
      }
    }
    False ->
      case get_subscriber_by_sid(state.subscribers, sid) {
        Ok(subscriber) -> {
          actor.send(subscriber.subject, nats_message)
          Ok(state)
        }

        Error(Nil) -> {
          state.logger.warning("message received but no subscription found")
          Ok(state)
        }
      }
  }
}

fn setup_connection(state: ClientState) {
  state.logger.debug(
    "setup connection " <> int.to_string(state.connection_attempt),
  )
  assert state.socket == None as "cant reconnect when there is an open socket"

  let socket =
    mug.new(state.options.host, state.options.port)
    |> mug.connect

  case socket {
    Ok(socket) -> {
      mug.receive_next_packet_as_message(socket)
      state.logger.debug(
        "socket opened to "
        <> state.options.host
        <> ":"
        <> int.to_string(state.options.port),
      )
      ClientState(..state, socket: Some(socket))
    }
    Error(err) -> {
      state.logger.debug("failed to connect: " <> string.inspect(err))
      ClientState(
        ..state,
        socket: None,
        connection_attempt: state.connection_attempt + 1,
      )
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
  actor.call(subject, actor_timeout, IsConnected)
}

pub fn publish(
  subject: Subject(Message),
  message: NatsMessage,
) -> Result(Nil, NatsError) {
  actor.call(subject, actor_timeout, Publish(message, _))
}

pub fn subscribe(
  conn: Subject(Message),
  nats_subject: String,
) -> Result(Subscription, NatsError) {
  let subscriber = process.new_subject()
  actor.call(conn, actor_timeout, Subscribe(
    nats_subject,
    subscriber:,
    reply: _,
    queue_group: None,
  ))
}

pub fn subscribe_with_queue(
  conn: Subject(Message),
  nats_subject: String,
  queue_group: String,
) -> Result(Subscription, NatsError) {
  let subscriber = process.new_subject()
  actor.call(conn, actor_timeout, Subscribe(
    nats_subject,
    subscriber:,
    reply: _,
    queue_group: Some(queue_group),
  ))
}

pub fn unsubscribe(conn: Subject(Message), subscription: Subscription) {
  actor.call(conn, actor_timeout, Unsubscribe(subscription, _))
}

pub fn shutdown(conn: Subject(Message)) -> Nil {
  actor.call(conn, actor_timeout, Shutdown)
}

pub fn flush(conn: Subject(Message), timeout: Int) -> Result(Nil, NatsError) {
  actor.call(conn, timeout + 5000, Flush)
}

// Logger
pub type Logger {
  Logger(
    info: fn(String) -> Nil,
    debug: fn(String) -> Nil,
    warning: fn(String) -> Nil,
  )
}

fn noop(_) {
  Nil
}

fn stderr_log(msg) {
  io.println_error(msg)
}

fn add_context(log: fn(String) -> Nil, context) {
  fn(line) { log(context <> line) }
}

pub fn noop_logger() {
  Logger(info: noop, debug: noop, warning: noop)
}

pub fn default_logger(context: String) {
  let output = add_context(stderr_log, context)
  Logger(info: output, debug: output, warning: output)
}

pub fn request(
  conn: Subject(Message),
  subject subject: String,
  headers headers: List(#(String, String)),
  payload payload: BitArray,
  timeout timeout: Int,
) -> Result(NatsMessage, NatsError) {
  actor.call(conn, timeout + 5000, Request(
    subject,
    headers,
    payload,
    timeout,
    _,
  ))
}
