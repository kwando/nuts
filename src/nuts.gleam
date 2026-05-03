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
import gleam/uri
import mug
import nuts/connect_options.{ConnectOptions}
import nuts/internal/command
import nuts/internal/protocol.{type ServerInfo}

@external(erlang, "nuts_ffi", "random_string")
fn random_string(length: Int) -> String

const actor_timeout = 5000

pub opaque type Options {
  Options(
    host: String,
    port: Int,
    nkey_seed: Option(String),
    reconnect_interval: Int,
    logger: Logger,
    name: Option(process.Name(Message)),
  )
}

pub fn new(host: String, port: Int) -> Options {
  Options(
    host:,
    port:,
    nkey_seed: None,
    reconnect_interval: 1000,
    logger: noop_logger(),
    name: None,
  )
}

pub fn nkey_seed(options: Options, string: String) -> Options {
  Options(..options, nkey_seed: Some(string))
}

pub fn with_logger(options: Options, logger: Logger) {
  Options(..options, logger:)
}

pub fn with_name(options: Options, name: process.Name(Message)) -> Options {
  Options(..options, name: Some(name))
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
  RequestTimedOut
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
  Request(
    message: NatsMessage,
    reply_subject: Subject(Result(NatsMessage, NatsError)),
    timeout: Int,
    reply: Subject(Result(Nil, NatsError)),
  )
  RequestTimeout(inbox: String)
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
type Responder {
  Responder(
    subject: Subject(Result(NatsMessage, NatsError)),
    timeout_timer: process.Timer,
  )
}

type ClientState {
  ClientState(
    options: Options,
    socket: Option(mug.Socket),
    buffer: BitArray,
    server_info: Option(ServerInfo),
    subscribers: SubscriberMap,
    next_sid: Int,
    self: Subject(Message),
    logger: Logger,
    connection_attempt: Int,
    inbox_prefix: String,
    response_map: Dict(String, Responder),
  )
}

pub fn start(options: Options) {
  actor.new_with_initialiser(actor_timeout, fn(self) {
    actor.send(self, Connect)

    actor.initialised(ClientState(
      options,
      socket: None,
      buffer: <<>>,
      server_info: None,
      subscribers: new_subscriber_map(),
      next_sid: 1,
      self:,
      logger: options.logger,
      connection_attempt: 1,
      response_map: dict.new(),
      inbox_prefix: new_inbox_prefix(),
    ))
    |> actor.selecting(
      process.new_selector()
      |> process.select(self)
      |> process.select_monitors(SubscriberDown)
      |> mug.select_tcp_messages(SocketData),
    )
    |> actor.returning(self)
    |> Ok
  })
  |> apply_option(options.name, actor.named)
  |> actor.on_message(handle_message)
  |> actor.start
}

fn apply_option(input: b, option: Option(a), callback: fn(b, a) -> b) -> b {
  case option {
    Some(value) -> callback(input, value)
    None -> input
  }
}

pub fn supervised(
  name: process.Name(Message),
  options: Options,
) -> supervision.ChildSpecification(Subject(Message)) {
  supervision.worker(fn() { start(options |> with_name(name)) })
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
    Publish(msg, reply) -> handle_publish(state, msg, reply)
    SocketData(mug_message) -> handle_socket_data(mug_message, state)
    Subscribe(subject:, subscriber:, reply:) ->
      handle_subscribe(state, subscriber, subject, reply)
    SubscriberDown(down) -> handle_subscriber_down(state, down)
    Unsubscribe(subscription, reply) ->
      handle_unsubscribe(state, subscription, reply)
    Request(message:, reply_subject:, reply:, timeout:) ->
      handle_request(state, message, reply, reply_subject, timeout)

    RequestTimeout(inbox) -> handle_request_timeout(state, inbox)
    Shutdown(reply) -> {
      actor.send(reply, Nil)
      actor.stop()
    }
  }
}

// ---------------- Handler functions ------------------
fn handle_socket_data(
  mug_message: mug.TcpMessage,
  state: ClientState,
) -> actor.Next(ClientState, a) {
  case mug_message {
    mug.Packet(socket, data) -> {
      mug.receive_next_packet_as_message(socket)
      let buffer = bit_array.append(state.buffer, data)
      case parse_server_messages(state, buffer) {
        Ok(state) -> actor.continue(state)
        Error(err) -> {
          state.logger.debug("parse_server_messages: " <> string.inspect(err))

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
}

fn handle_unsubscribe(
  state: ClientState,
  subscription: Subscription,
  reply: Subject(Result(Nil, NatsError)),
) -> actor.Next(ClientState, a) {
  case get_subscriber_by_sid(state.subscribers, subscription.subscriber.sid) {
    Ok(subscriber) -> {
      case send_bits(state, command.encode_unsub(subscriber.sid, None)) {
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

fn handle_subscriber_down(
  state: ClientState,
  down: process.Down,
) -> actor.Next(ClientState, a) {
  case get_subscriber_by_monitor(state.subscribers, down.monitor) {
    Ok(subscriber) -> {
      let _ = send_bits(state, command.encode_unsub(subscriber.sid, None))

      state
      |> update_subscribers(remove_subscriber(_, subscriber))
      |> actor.continue()
    }
    Error(_) -> actor.continue(state)
  }
}

fn handle_subscribe(
  state: ClientState,
  subscriber: Subject(NatsMessage),
  subject: String,
  reply: Subject(Result(Subscription, NatsError)),
) -> actor.Next(ClientState, a) {
  let sid = state.next_sid |> int.to_base36
  case process.subject_owner(subscriber) {
    Ok(pid) -> {
      let monitor = process.monitor(pid)

      let state = ClientState(..state, next_sid: state.next_sid + 1)
      let subscriber =
        Subscriber(sid:, monitor:, subject: subscriber, pattern: subject)
      actor.send(reply, Ok(Subscription(subscriber:)))
      let state =
        state
        |> update_subscribers(add_subscriber(_, subscriber))

      case
        send_bits(state, command.encode_sub(subject:, sid:, queue_group: None))
      {
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

fn handle_publish(
  state: ClientState,
  msg: NatsMessage,
  reply: Subject(Result(Nil, NatsError)),
) -> actor.Next(ClientState, a) {
  case state.socket {
    Some(_) -> {
      let bytes = encode_message(msg)
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

fn handle_request(
  state: ClientState,
  message: NatsMessage,
  reply: Subject(Result(Nil, NatsError)),
  reply_subject: Subject(Result(NatsMessage, NatsError)),
  timeout: Int,
) -> actor.Next(ClientState, a) {
  let inbox = new_inbox_from_prefix(state.inbox_prefix)
  let bytes = encode_message(NatsMessage(..message, reply_to: Some(inbox)))

  case send_bits(state, bytes) {
    Ok(_) -> {
      actor.send(reply, Ok(Nil))
      let timeout_timer =
        process.send_after(state.self, timeout, RequestTimeout(inbox))

      update_response_map(state, dict.insert(
        _,
        inbox,
        Responder(subject: reply_subject, timeout_timer:),
      ))
      |> actor.continue
    }
    Error(err) -> {
      actor.send(reply, Error(err))
      actor.continue(state)
    }
  }
}

fn handle_request_timeout(
  state: ClientState,
  inbox: String,
) -> actor.Next(ClientState, a) {
  case dict.get(state.response_map, inbox) {
    Ok(responder) -> {
      assert process.cancel_timer(responder.timeout_timer)
        == process.TimerNotFound
        as "this message should have come from the responder"
      process.send(responder.subject, Error(RequestTimedOut))
      update_response_map(state, dict.delete(_, inbox))
    }
    // We got a timeout for a response which we have already responded to. This can happen if the
    // response is received after the timeout has already expired.
    Error(_) -> state
  }
  |> actor.continue
}

/// Convert a NatsMessage to a BitArray for sending over the socket.∏
fn encode_message(msg: NatsMessage) -> BitArray {
  case msg.headers {
    [] -> command.encode_pub(msg.subject, msg.reply_to, msg.payload)

    headers ->
      command.encode_hpub(msg.subject, msg.reply_to, headers, msg.payload)
  }
}

fn close_connection(state: ClientState) {
  state.logger.debug("close connection")
  case state.socket {
    Some(socket) -> {
      let _ = mug.shutdown(socket)

      ClientState(
        ..state,
        socket: None,
        server_info: None,
        connection_attempt: 1,
      )
      |> setup_connection
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
              command.encode_connect(ConnectOptions(
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
              ClientState(..state, server_info: Some(server_info))
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
      case send_bits(state, command.encode_pong()) {
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
  let request_responder =
    command.encode_sub(
      subject: state.inbox_prefix <> "*",
      sid: "0",
      queue_group: None,
    )

  let command =
    list.fold(subscribers, request_responder, fn(acc, subscriber) {
      bit_array.append(
        acc,
        command.encode_sub(
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

fn broadcast_message(
  state: ClientState,
  sid: String,
  nats_message: NatsMessage,
) -> Result(ClientState, a) {
  case state.response_map |> dict.get(nats_message.subject) {
    Ok(responder) -> {
      case process.cancel_timer(responder.timeout_timer) {
        process.TimerNotFound -> Nil
        process.Cancelled(..) ->
          process.send(responder.subject, Ok(nats_message))
      }

      ClientState(
        ..state,
        response_map: dict.delete(state.response_map, nats_message.subject),
      )
      |> Ok
    }
    Error(_) -> {
      case get_subscriber_by_sid(state.subscribers, sid) {
        Ok(subscriber) -> {
          actor.send(subscriber.subject, nats_message)
          Ok(state)
        }

        Error(Nil) -> {
          state.logger.warning("message received but but no subscription found")
          Ok(state)
        }
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
  actor.call(conn, actor_timeout, Subscribe(nats_subject, subscriber:, reply: _))
}

pub fn unsubscribe(conn: Subject(Message), subscription: Subscription) {
  actor.call(conn, actor_timeout, Unsubscribe(subscription, _))
}

pub fn shutdown(conn: Subject(Message)) -> Nil {
  actor.call(conn, actor_timeout, Shutdown)
}

pub fn request(
  conn: Subject(Message),
  message: NatsMessage,
  timeout: Int,
) -> Result(NatsMessage, NatsError) {
  let reply_subject = process.new_subject()
  case
    actor.call(conn, actor_timeout, Request(
      message:,
      timeout:,
      reply_subject:,
      reply: _,
    ))
  {
    Ok(_) ->
      case process.receive(reply_subject, timeout + actor_timeout) {
        Ok(result) -> result
        Error(_) -> Error(RequestTimedOut)
      }
    Error(err) -> Error(err)
  }
}

fn update_response_map(state: ClientState, callback) {
  ClientState(..state, response_map: callback(state.response_map))
}

fn new_inbox_prefix() -> String {
  "_INBOX." <> random_string(10) <> "."
}

fn new_inbox_from_prefix(prefix: String) -> String {
  prefix <> random_string(5)
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
