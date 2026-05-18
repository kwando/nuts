//// NATS JetStream client for the Guppy library.
////
//// This module provides a high-level API for interacting with NATS JetStream,
//// including stream management, consumer management, and persistent message
//// publishing.
////
//// ## Quick start
////
//// ```gleam
//// import guppy
//// import guppy/jetstream
//// import gleam/option.{None}
////
//// pub fn main() {
////   let assert Ok(conn) = guppy.start(guppy.new("127.0.0.1", 4222))
////   let js = jetstream.new_context(conn)
////
////   // Create a stream
////   let _ =
////     jetstream.create_stream(
////       js,
////       jetstream.StreamOptions(
////         stream_name: "orders",
////         description: None,
////         subjects: ["orders.>"],
////         retention: jetstream.Limits,
////         discard_policy: jetstream.DiscardOld,
////         max_consumers: -1,
////         max_msgs: -1,
////         max_bytes: -1,
////         max_age: 0,
////         storage: jetstream.Memory,
////         num_replicas: 1,
////       ),
////     )
////
////   // Publish a message
////   let _ =
////     jetstream.publish(
////       js,
////       "orders.created",
////       <<"{\"id\": 1}">>,
////       jetstream.default_publish_options(),
////     )
//// }
//// ```

import gleam/bit_array
import gleam/dynamic/decode.{type Decoder}
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import gleam/time/calendar
import gleam/time/duration.{type Duration}
import gleam/time/timestamp.{type Timestamp}
import guppy.{type NatsMessage, NatsMessage}

// ── Context ─────────────────────────────────────────────────────────────────

pub opaque type JetstreamContext {
  JetstreamContext(
    conn: Subject(guppy.Message),
    log: fn(String) -> Nil,
    request_timeout: Int,
  )
}

pub fn new_context(conn: Subject(guppy.Message)) -> JetstreamContext {
  JetstreamContext(conn, fn(_) -> Nil { Nil }, request_timeout: 5000)
}

pub fn with_logger(
  js: JetstreamContext,
  log: fn(String) -> Nil,
) -> JetstreamContext {
  JetstreamContext(..js, log:)
}

// ── Errors ──────────────────────────────────────────────────────────────────

pub type JetstreamError {
  ConnectionError(guppy.NatsError)
  ResponseDecodeError(json.DecodeError, input: BitArray)
  ApiError(code: Int, description: String, err_code: Int)
  StreamNotFound
  StreamAlreadyExistsWithDifferentConfig
  ConsumerNotFound
  WrongLastSequence
  WrongLastSubjectSequence
  MessageTooLarge
}

pub type StreamApiError {
  StreamApiError(code: Int, description: String, err_code: Int)
}

// ── Stream Types ────────────────────────────────────────────────────────────

/// How messages are retained in the stream.
pub type Retention {
  /// Messages are retained until storage limits are exceeded.
  Limits
  /// Messages are retained until there are no consumers interested in them.
  Interest
  /// Messages are retained until they are acknowledged by all consumers.
  Workqueue
}

/// Where stream data is persisted.
pub type Storage {
  /// Messages are stored on disk.
  File
  /// Messages are stored in memory only.
  Memory
}

/// Behaviour when a stream limit is reached.
pub type DiscardPolicy {
  /// Reject new messages once limits are hit.
  DiscardNew
  /// Remove oldest messages to make room for new ones.
  DiscardOld
}

/// Configuration for creating or updating a JetStream stream.
///
/// Used with `create_stream` and `update_stream` to define the properties
/// of a NATS JetStream stream.
///
/// ## Example
///
/// ```gleam
/// jetstream.StreamOptions(
///   stream_name: "orders",
///   description: option.Some("Order events"),
///   subjects: ["orders.>"],
///   retention: jetstream.Limits,
///   discard_policy: jetstream.DiscardOld,
///   max_consumers: -1,
///   max_msgs: -1,
///   max_bytes: -1,
///   max_age: 0,
///   storage: jetstream.Memory,
///   num_replicas: 1,
/// )
/// ```
pub type StreamOptions {
  StreamOptions(
    /// The name of the stream.
    stream_name: String,
    /// Optional human-readable description of the stream.
    description: Option(String),
    /// List of subjects this stream will capture messages for.
    subjects: List(String),
    /// Message retention policy: how long messages are kept in the stream.
    retention: Retention,
    /// Discard policy applied when limits are reached.
    discard_policy: DiscardPolicy,
    /// Maximum number of consumers allowed on this stream (-1 for unlimited).
    max_consumers: Int,
    /// Maximum number of messages the stream can hold (-1 for unlimited).
    max_msgs: Int,
    /// Maximum total size in bytes the stream can consume (-1 for unlimited).
    max_bytes: Int,
    /// Maximum age of messages in nanoseconds before they are removed (0 for unlimited).
    max_age: Int,
    /// Storage backend used by the stream.
    storage: Storage,
    /// Number of replicas for this stream (0 for default).
    num_replicas: Int,
  )
}

pub type StreamConfig {
  StreamConfig(
    name: String,
    description: Option(String),
    subjects: List(String),
  )
}

pub type StreamState {
  StreamState(
    messages: Int,
    bytes: Int,
    first_seq: Int,
    first_ts: Timestamp,
    last_seq: Int,
    last_ts: Timestamp,
    consumer_count: Int,
  )
}

pub type StreamNamesResponse {
  StreamNamesResponse(
    total: Int,
    offset: Int,
    limit: Int,
    streams: List(String),
  )
}

@internal
pub type StreamGetInfoResponse {
  StreamGetInfoResponse(config: StreamConfig, state: StreamState)
}

pub type StreamPurgeResponse {
  StreamPurgeResponse(success: Bool, purged: Bool)
}

pub type DeleteMessageResponse {
  DeleteMessageResponse(success: Bool)
}

pub type StreamCreateResponse {
  StreamCreated(name: String)
}

pub type StreamDeleteResponse {
  StreamDeleteResponse(success: Bool)
}

// ── Stream API ───────────────────────────────────────────────────────────────

pub fn list_stream_names(
  js: JetstreamContext,
) -> Result(StreamNamesResponse, JetstreamError) {
  let assert Ok(msg) =
    guppy.request(
      js.conn,
      list_stream_names_request(None, None),
      js.request_timeout,
    )

  decode_response(js, msg, stream_names_response_decoder())
}

pub fn create_stream(
  js: JetstreamContext,
  options: StreamOptions,
) -> Result(StreamCreateResponse, JetstreamError) {
  use msg <- make_request(js, stream_create_request(options))

  use api_response <- result.try(decode_response(
    js,
    msg,
    stream_create_response_decoder(),
  ))
  api_response
  |> result.map_error(map_api_error)
}

pub fn get_info(
  js: JetstreamContext,
  stream: String,
) -> Result(StreamGetInfoResponse, JetstreamError) {
  use resp <- make_request(
    js,
    stream_get_info_request(
      stream:,
      deleted_details: False,
      subjects_filter: None,
      offset: 0,
    ),
  )
  js.log(resp.payload |> bit_array.to_string |> result.unwrap("<<binary>>"))

  use decoded_result <- result.try(decode_response(
    js,
    resp,
    stream_get_info_response_decoder(),
  ))
  decoded_result
  |> result.map_error(map_api_error)
}

pub fn update_stream(
  js: JetstreamContext,
  options: StreamOptions,
) -> Result(StreamGetInfoResponse, JetstreamError) {
  use msg <- make_request(js, stream_update_request(options))
  use decoded_result <- result.try(decode_response(
    js,
    msg,
    stream_update_response_decoder(),
  ))
  decoded_result
  |> result.map_error(map_api_error)
}

pub fn delete_stream(
  js: JetstreamContext,
  stream: String,
) -> Result(StreamDeleteResponse, JetstreamError) {
  use msg <- make_request(js, stream_delete_request(stream))
  use decoded_result <- result.try(decode_response(
    js,
    msg,
    stream_delete_response_decoder(),
  ))
  decoded_result
  |> result.map_error(map_api_error)
}

pub fn purge_stream(
  js: JetstreamContext,
  stream: String,
  subject_filter: Option(String),
) -> Result(StreamPurgeResponse, JetstreamError) {
  use msg <- make_request(js, stream_purge_request(stream, subject_filter))
  use decoded_result <- result.try(decode_response(
    js,
    msg,
    stream_purge_response_decoder(),
  ))
  decoded_result
  |> result.map_error(map_api_error)
}

pub fn delete_message(
  js: JetstreamContext,
  stream: String,
  sequence: Int,
) -> Result(DeleteMessageResponse, JetstreamError) {
  use msg <- make_request(js, delete_message_request(stream, sequence))
  use decoded_result <- result.try(decode_response(
    js,
    msg,
    delete_message_response_decoder(),
  ))
  decoded_result
  |> result.map_error(map_api_error)
}

// ── Stream Internal ─────────────────────────────────────────────────────────

@internal
pub fn list_stream_names_request(
  subject_filter: Option(String),
  offset: Option(Int),
) -> NatsMessage {
  NatsMessage(
    subject: "$JS.API.STREAM.NAMES",
    reply_to: None,
    headers: [],
    payload: json_payload(json.object(
      []
      |> optional_field("offset", offset, json.int)
      |> optional_field("subject", subject_filter, json.string),
    )),
  )
}

@internal
pub fn stream_names_response_decoder() -> Decoder(StreamNamesResponse) {
  use total <- decode.field("total", decode.int)
  use offset <- decode.field("offset", decode.int)
  use limit <- decode.field("limit", decode.int)
  use streams <- decode.field(
    "streams",
    decode.optional(decode.list(decode.string)),
  )
  decode.success(StreamNamesResponse(
    total:,
    offset:,
    limit:,
    streams: option.unwrap(streams, []),
  ))
}

@internal
pub fn stream_get_info_request(
  stream stream: String,
  deleted_details deleted_details: Bool,
  subjects_filter subjects_filter: Option(String),
  offset offset: Int,
) -> NatsMessage {
  NatsMessage(
    subject: "$JS.API.STREAM.INFO." <> stream,
    reply_to: None,
    headers: [],
    payload: json_payload(json.object(
      [
        #("deleted_details", json.bool(deleted_details)),
        #("offset", json.int(offset)),
      ]
      |> optional_field("subject", subjects_filter, json.string),
    )),
  )
}

@internal
pub fn stream_get_info_response_decoder() -> Decoder(
  Result(StreamGetInfoResponse, StreamApiError),
) {
  use <- decode_stream_api_error()
  use config <- decode.field("config", stream_config_decoder())
  use state <- decode.field("state", stream_state_decoder())
  decode.success(Ok(StreamGetInfoResponse(config:, state:)))
}

@internal
pub fn stream_create_request(request: StreamOptions) -> NatsMessage {
  NatsMessage(
    subject: "$JS.API.STREAM.CREATE." <> request.stream_name,
    reply_to: None,
    headers: [],
    payload: json_payload(json.object(
      [
        #("name", json.string(request.stream_name)),
        #("subjects", json.array(request.subjects, json.string)),
        #("retention", json.string(retention_to_string(request.retention))),
        #("max_consumers", json.int(request.max_consumers)),
        #("max_msgs", json.int(request.max_msgs)),
        #("max_bytes", json.int(request.max_bytes)),
        #("max_age", json.int(request.max_age)),
        #("storage", json.string(storage_to_string(request.storage))),
        #("num_replicas", json.int(request.num_replicas)),
        #(
          "discard",
          json.string(discard_policy_to_string(request.discard_policy)),
        ),
      ]
      |> optional_field("description", request.description, json.string),
    )),
  )
}

@internal
pub fn stream_create_response_decoder() -> Decoder(
  Result(StreamCreateResponse, StreamApiError),
) {
  use <- decode_stream_api_error()
  use name <- decode.subfield(["config", "name"], decode.string)
  decode.success(Ok(StreamCreated(name)))
}

@internal
pub fn stream_delete_request(stream_name: String) -> NatsMessage {
  NatsMessage(
    subject: "$JS.API.STREAM.DELETE." <> stream_name,
    reply_to: None,
    headers: [],
    payload: bit_array.from_string(""),
  )
}

@internal
pub fn stream_delete_response_decoder() {
  use <- decode_stream_api_error()
  use success <- decode.field("success", decode.bool)
  decode.success(Ok(StreamDeleteResponse(success)))
}

@internal
pub fn stream_purge_request(
  stream_name: String,
  subject_filter: Option(String),
) -> NatsMessage {
  NatsMessage(
    subject: "$JS.API.STREAM.PURGE." <> stream_name,
    reply_to: None,
    headers: [],
    payload: case subject_filter {
      Some(filter) ->
        json_payload(
          json.object([
            #("filter", json.string(filter)),
          ]),
        )
      None -> bit_array.from_string("")
    },
  )
}

fn stream_purge_response_decoder() -> Decoder(
  Result(StreamPurgeResponse, StreamApiError),
) {
  use <- decode_stream_api_error()
  use success <- decode.optional_field("success", True, decode.bool)
  use purged_count <- decode.optional_field("purged", 0, decode.int)
  decode.success(Ok(StreamPurgeResponse(success:, purged: purged_count > 0)))
}

@internal
pub fn delete_message_request(
  stream_name: String,
  sequence: Int,
) -> NatsMessage {
  NatsMessage(
    subject: "$JS.API.STREAM.MSG.DELETE." <> stream_name,
    reply_to: None,
    headers: [],
    payload: json_payload(json.object([#("seq", json.int(sequence))])),
  )
}

fn delete_message_response_decoder() -> Decoder(
  Result(DeleteMessageResponse, StreamApiError),
) {
  use <- decode_stream_api_error()
  use success <- decode.field("success", decode.bool)
  decode.success(Ok(DeleteMessageResponse(success:)))
}

@internal
pub fn stream_update_request(request: StreamOptions) -> NatsMessage {
  NatsMessage(
    subject: "$JS.API.STREAM.UPDATE." <> request.stream_name,
    reply_to: None,
    headers: [],
    payload: json_payload(json.object(
      [
        #("name", json.string(request.stream_name)),
        #("subjects", json.array(request.subjects, json.string)),
        #("retention", json.string(retention_to_string(request.retention))),
        #("max_consumers", json.int(request.max_consumers)),
        #("max_msgs", json.int(request.max_msgs)),
        #("max_bytes", json.int(request.max_bytes)),
        #("max_age", json.int(request.max_age)),
        #("storage", json.string(storage_to_string(request.storage))),
        #("num_replicas", json.int(request.num_replicas)),
        #(
          "discard",
          json.string(discard_policy_to_string(request.discard_policy)),
        ),
      ]
      |> optional_field("description", request.description, json.string),
    )),
  )
}

@internal
pub fn stream_update_response_decoder() -> Decoder(
  Result(StreamGetInfoResponse, StreamApiError),
) {
  use <- decode_stream_api_error()
  use config <- decode.field("config", stream_config_decoder())
  use state <- decode.field("state", stream_state_decoder())
  decode.success(Ok(StreamGetInfoResponse(config:, state:)))
}

fn stream_config_decoder() -> Decoder(StreamConfig) {
  use name <- decode.field("name", decode.string)
  use description <- decode.optional_field(
    "description",
    None,
    decode.optional(decode.string),
  )
  use subjects <- decode.field("subjects", decode.list(decode.string))
  decode.success(StreamConfig(name:, description:, subjects:))
}

fn stream_state_decoder() -> Decoder(StreamState) {
  let ts_decoder = decode_timestamp()
  use messages <- decode.field("messages", decode.int)
  use bytes <- decode.field("bytes", decode.int)
  use first_seq <- decode.field("first_seq", decode.int)
  use first_ts <- decode.field("first_ts", ts_decoder)
  use last_seq <- decode.field("last_seq", decode.int)
  use last_ts <- decode.field("last_ts", ts_decoder)
  use consumer_count <- decode.field("consumer_count", decode.int)
  decode.success(StreamState(
    messages:,
    bytes:,
    first_seq:,
    first_ts:,
    last_seq:,
    last_ts:,
    consumer_count:,
  ))
}

fn retention_to_string(retention: Retention) -> String {
  case retention {
    Limits -> "limits"
    Interest -> "interest"
    Workqueue -> "workqueue"
  }
}

fn storage_to_string(storage: Storage) -> String {
  case storage {
    File -> "file"
    Memory -> "memory"
  }
}

fn discard_policy_to_string(discard_policy: DiscardPolicy) -> String {
  case discard_policy {
    DiscardNew -> "new"
    DiscardOld -> "old"
  }
}

// ── Consumer Types ───────────────────────────────────────────────────────────

/// Where the consumer should start delivering messages from.
pub type DeliverPolicy {
  /// Deliver all available messages.
  DeliverAll
  /// Deliver only new messages that arrive after the consumer is created.
  DeliverNew
  /// Deliver only the last message for each subject.
  DeliverLast
  /// Deliver starting from the given sequence number.
  DeliverByStartSequence(Int)
  /// Deliver starting from the given timestamp.
  DeliverByStartTime(Timestamp)
}

/// How messages must be acknowledged by the consumer.
pub type AckPolicy {
  /// Messages do not need to be acknowledged.
  NoAck
  /// Acknowledging a message also acknowledges all earlier messages.
  AckAll
  /// Each message must be individually acknowledged.
  AckExplicit
}

/// The rate at which messages are delivered to the consumer.
pub type ReplayPolicy {
  /// Messages are delivered as fast as the consumer can process them.
  Instant
  /// Messages are delivered at the same rate they were originally received.
  Original
}

pub type ConsumerConfig {
  ConsumerConfig(
    description: Option(String),
    durable: Bool,
    deliver_policy: DeliverPolicy,
    ack_policy: AckPolicy,
    ack_wait: Option(Duration),
    max_deliver: Int,
    max_ack_pending: Option(Int),
    max_waiting: Option(Int),
    backoff: Option(List(Duration)),
    inactive_threshold: Option(Duration),
    replay_policy: ReplayPolicy,
    deliver_subject: Option(String),
    deliver_group: Option(String),
    flow_control: Bool,
    idle_heartbeat: Option(Duration),
    ratelimit: Option(Int),
    headers_only: Bool,
  )
}

pub fn default_consumer_config() -> ConsumerConfig {
  ConsumerConfig(
    description: None,
    durable: False,
    deliver_policy: DeliverAll,
    ack_policy: AckExplicit,
    ack_wait: None,
    max_deliver: -1,
    max_ack_pending: None,
    max_waiting: None,
    backoff: None,
    inactive_threshold: None,
    replay_policy: Instant,
    deliver_subject: None,
    deliver_group: None,
    flow_control: False,
    idle_heartbeat: None,
    ratelimit: None,
    headers_only: False,
  )
}

pub type ConsumerCreateResponse {
  ConsumerCreateResponse(
    stream_name: String,
    name: String,
    created: Timestamp,
    timestamp: Timestamp,
  )
}

pub type ConsumerDeleteResponse {
  ConsumerDeleteResponse(success: Bool)
}

pub type ConsumerGetInfoResponse {
  ConsumerGetInfoResponse(
    stream_name: String,
    name: String,
    config: ConsumerConfig,
    created: Timestamp,
    timestamp: Timestamp,
    num_pending: Int,
    num_ack_pending: Int,
    num_redelivered: Int,
    num_waiting: Int,
  )
}

// ── Consumer API ─────────────────────────────────────────────────────────────

pub fn create_consumer(
  js: JetstreamContext,
  stream stream: String,
  consumer_name consumer_name: String,
  config config: ConsumerConfig,
) -> Result(ConsumerCreateResponse, JetstreamError) {
  use msg <- make_request(
    js,
    consumer_create_request(stream:, consumer_name:, config:),
  )
  use decoded_result <- result.try(decode_response(
    js,
    msg,
    consumer_create_response_decoder(),
  ))
  decoded_result
  |> result.map_error(map_api_error)
}

pub fn get_consumer_info(
  js: JetstreamContext,
  stream stream: String,
  consumer_name consumer_name: String,
) -> Result(ConsumerGetInfoResponse, JetstreamError) {
  use msg <- make_request(
    js,
    consumer_get_info_request(stream:, consumer_name:),
  )
  use decoded_result <- result.try(decode_response(
    js,
    msg,
    consumer_get_info_response_decoder(),
  ))
  decoded_result
  |> result.map_error(map_api_error)
}

pub fn delete_consumer(
  js: JetstreamContext,
  stream stream: String,
  consumer_name consumer_name: String,
) -> Result(ConsumerDeleteResponse, JetstreamError) {
  use msg <- make_request(js, consumer_delete_request(stream:, consumer_name:))
  use decoded_result <- result.try(decode_response(
    js,
    msg,
    consumer_delete_response_decoder(),
  ))
  decoded_result
  |> result.map_error(map_api_error)
}

// ── Consumer Internal ────────────────────────────────────────────────────────

@internal
pub fn consumer_create_request(
  stream stream: String,
  consumer_name consumer_name: String,
  config config: ConsumerConfig,
) -> guppy.NatsMessage {
  let subject = "$JS.API.CONSUMER.CREATE." <> stream <> "." <> consumer_name
  let durable_field = case config.durable {
    True -> [#("durable_name", json.string(consumer_name))]
    False -> []
  }
  guppy.NatsMessage(
    subject:,
    reply_to: None,
    headers: [],
    payload: json_payload(
      json.object([
        #("stream_name", json.string(stream)),
        #(
          "config",
          json.object(
            list.append(
              durable_field,
              deliver_policy_to_json(config.deliver_policy),
            )
            |> list.append([
              #("ack_policy", ack_policy_to_json(config.ack_policy)),
              #("replay_policy", replay_policy_to_json(config.replay_policy)),
              #("max_deliver", json.int(config.max_deliver)),
            ])
            |> optional_field("description", config.description, json.string)
            |> optional_field("ack_wait", config.ack_wait, duration_to_json)
            |> optional_field(
              "max_ack_pending",
              config.max_ack_pending,
              json.int,
            )
            |> optional_field("max_waiting", config.max_waiting, json.int)
            |> optional_field("backoff", config.backoff, fn(delays) {
              json.array(delays, duration_to_json)
            })
            |> optional_field(
              "inactive_threshold",
              config.inactive_threshold,
              duration_to_json,
            )
            |> optional_field(
              "deliver_subject",
              config.deliver_subject,
              json.string,
            )
            |> optional_field(
              "deliver_group",
              config.deliver_group,
              json.string,
            )
            |> optional_field(
              "flow_control",
              Some(config.flow_control),
              json.bool,
            )
            |> optional_field(
              "idle_heartbeat",
              config.idle_heartbeat,
              duration_to_json,
            )
            |> optional_field("ratelimit", config.ratelimit, json.int)
            |> optional_field(
              "headers_only",
              Some(config.headers_only),
              json.bool,
            ),
          ),
        ),
      ]),
    ),
  )
}

@internal
pub fn consumer_create_response_decoder() -> Decoder(
  Result(ConsumerCreateResponse, StreamApiError),
) {
  use <- decode_stream_api_error()

  use stream_name <- decode.field("stream_name", decode.string)
  use name <- decode.field("name", decode.string)
  use created <- decode.field("created", decode_timestamp())
  use timestamp <- decode.field("ts", decode_timestamp())
  decode.success(
    Ok(ConsumerCreateResponse(stream_name:, name:, created:, timestamp:)),
  )
}

@internal
pub fn consumer_get_info_request(
  stream stream: String,
  consumer_name consumer_name: String,
) -> NatsMessage {
  NatsMessage(
    subject: "$JS.API.CONSUMER.INFO." <> stream <> "." <> consumer_name,
    reply_to: None,
    headers: [],
    payload: bit_array.from_string(""),
  )
}

@internal
pub fn consumer_get_info_response_decoder() -> Decoder(
  Result(ConsumerGetInfoResponse, StreamApiError),
) {
  use <- decode_stream_api_error()
  use stream_name <- decode.field("stream_name", decode.string)
  use name <- decode.field("name", decode.string)
  use config <- decode.field("config", consumer_config_decoder())
  use created <- decode.field("created", decode_timestamp())
  use timestamp <- decode.field("ts", decode_timestamp())
  use num_pending <- decode.optional_field("num_pending", 0, decode.int)
  use num_ack_pending <- decode.optional_field("num_ack_pending", 0, decode.int)
  use num_redelivered <- decode.optional_field("num_redelivered", 0, decode.int)
  use num_waiting <- decode.optional_field("num_waiting", 0, decode.int)
  decode.success(
    Ok(ConsumerGetInfoResponse(
      stream_name:,
      name:,
      config:,
      created:,
      timestamp:,
      num_pending:,
      num_ack_pending:,
      num_redelivered:,
      num_waiting:,
    )),
  )
}

@internal
pub fn consumer_delete_request(
  stream stream: String,
  consumer_name consumer_name: String,
) -> NatsMessage {
  NatsMessage(
    subject: "$JS.API.CONSUMER.DELETE." <> stream <> "." <> consumer_name,
    reply_to: None,
    headers: [],
    payload: bit_array.from_string(""),
  )
}

@internal
pub fn consumer_delete_response_decoder() -> Decoder(
  Result(ConsumerDeleteResponse, StreamApiError),
) {
  use <- decode_stream_api_error()
  use success <- decode.field("success", decode.bool)
  decode.success(Ok(ConsumerDeleteResponse(success)))
}

fn consumer_config_decoder() -> Decoder(ConsumerConfig) {
  use durable_name <- decode.optional_field(
    "durable_name",
    None,
    decode.optional(decode.string),
  )
  use description <- decode.optional_field(
    "description",
    None,
    decode.optional(decode.string),
  )
  use deliver_policy <- decode.field("deliver_policy", deliver_policy_decoder())
  use ack_policy <- decode.field("ack_policy", ack_policy_decoder())
  use ack_wait <- decode.optional_field(
    "ack_wait",
    None,
    decode.optional(duration_from_ns_decoder()),
  )
  use max_deliver <- decode.optional_field("max_deliver", -1, decode.int)
  use max_ack_pending <- decode.optional_field(
    "max_ack_pending",
    None,
    decode.optional(decode.int),
  )
  use max_waiting <- decode.optional_field(
    "max_waiting",
    None,
    decode.optional(decode.int),
  )
  use backoff <- decode.optional_field(
    "backoff",
    None,
    decode.optional(decode.list(duration_from_ns_decoder())),
  )
  use inactive_threshold <- decode.optional_field(
    "inactive_threshold",
    None,
    decode.optional(duration_from_ns_decoder()),
  )
  use replay_policy <- decode.field("replay_policy", replay_policy_decoder())
  use deliver_subject <- decode.optional_field(
    "deliver_subject",
    None,
    decode.optional(decode.string),
  )
  use deliver_group <- decode.optional_field(
    "deliver_group",
    None,
    decode.optional(decode.string),
  )
  use flow_control <- decode.optional_field("flow_control", False, decode.bool)
  use idle_heartbeat <- decode.optional_field(
    "idle_heartbeat",
    None,
    decode.optional(duration_from_ns_decoder()),
  )
  use ratelimit <- decode.optional_field(
    "ratelimit",
    None,
    decode.optional(decode.int),
  )
  use headers_only <- decode.optional_field("headers_only", False, decode.bool)
  decode.success(ConsumerConfig(
    description:,
    durable: durable_name != None,
    deliver_policy:,
    ack_policy:,
    ack_wait:,
    max_deliver:,
    max_ack_pending:,
    max_waiting:,
    backoff:,
    inactive_threshold:,
    replay_policy:,
    deliver_subject:,
    deliver_group:,
    flow_control:,
    idle_heartbeat:,
    ratelimit:,
    headers_only:,
  ))
}

fn deliver_policy_decoder() -> Decoder(DeliverPolicy) {
  decode.then(decode.string, fn(policy) {
    case policy {
      "all" -> decode.success(DeliverAll)
      "new" -> decode.success(DeliverNew)
      "last" -> decode.success(DeliverLast)
      "by_start_sequence" -> {
        use seq <- decode.field("opt_start_seq", decode.int)
        decode.success(DeliverByStartSequence(seq))
      }
      "by_start_time" -> {
        use ts <- decode.field("opt_start_time", decode_timestamp())
        decode.success(DeliverByStartTime(ts))
      }
      _ -> decode.failure(DeliverAll, "deliver_policy")
    }
  })
}

fn ack_policy_decoder() -> Decoder(AckPolicy) {
  decode.then(decode.string, fn(policy) {
    case policy {
      "none" -> decode.success(NoAck)
      "all" -> decode.success(AckAll)
      "explicit" -> decode.success(AckExplicit)
      _ -> decode.failure(AckExplicit, "ack_policy")
    }
  })
}

fn replay_policy_decoder() -> Decoder(ReplayPolicy) {
  decode.then(decode.string, fn(policy) {
    case policy {
      "instant" -> decode.success(Instant)
      "original" -> decode.success(Original)
      _ -> decode.success(Instant)
    }
  })
}

fn ack_policy_to_json(ack_policy: AckPolicy) -> json.Json {
  case ack_policy {
    NoAck -> json.string("none")
    AckAll -> json.string("all")
    AckExplicit -> json.string("explicit")
  }
}

fn replay_policy_to_json(replay_policy: ReplayPolicy) -> json.Json {
  case replay_policy {
    Instant -> json.string("instant")
    Original -> json.string("original")
  }
}

fn deliver_policy_to_json(policy: DeliverPolicy) -> List(#(String, json.Json)) {
  case policy {
    DeliverAll -> [#("deliver_policy", json.string("all"))]
    DeliverNew -> [#("deliver_policy", json.string("new"))]
    DeliverLast -> [#("deliver_policy", json.string("last"))]
    DeliverByStartSequence(seq) -> [
      #("deliver_policy", json.string("by_start_sequence")),
      #("opt_start_seq", json.int(seq)),
    ]
    DeliverByStartTime(timestamp) -> [
      #("deliver_policy", json.string("last")),
      #(
        "opt_start_time",
        json.string(timestamp.to_rfc3339(timestamp, calendar.utc_offset)),
      ),
    ]
  }
}

@internal
pub fn consumer_pull_next_messages(
  stream stream: String,
  consumer consumer: String,
  expires expires: Option(Duration),
  batch batch: Option(Int),
  max_bytes max_bytes: Option(Int),
) -> NatsMessage {
  NatsMessage(
    subject: "$JS.API.CONSUMER.MSG.NEXT." <> stream <> "." <> consumer,
    reply_to: None,
    headers: [],
    payload: json_payload(json.object(
      []
      |> optional_field("expires", expires, duration_to_json)
      |> optional_field("batch", batch, json.int)
      |> optional_field("max_bytes", max_bytes, json.int),
    )),
  )
}

// ── Publish ─────────────────────────────────────────────────────────────────

pub type PublishOptions {
  PublishOptions(
    msg_id: Option(String),
    expected_stream: Option(String),
    expected_last_seq: Option(Int),
    expected_last_subject_seq: Option(Int),
    timeout: Option(Int),
  )
}

pub fn default_publish_options() -> PublishOptions {
  PublishOptions(
    msg_id: None,
    expected_stream: None,
    expected_last_seq: None,
    expected_last_subject_seq: None,
    timeout: None,
  )
}

pub type PubAck {
  PubAck(stream: String, seq: Int, duplicate: Bool)
}

pub fn publish(
  js: JetstreamContext,
  subject: String,
  payload: BitArray,
  options: PublishOptions,
) -> Result(PubAck, JetstreamError) {
  let headers =
    []
    |> add_optional_header("Nats-Msg-Id", options.msg_id)
    |> add_optional_header("Nats-Expected-Stream", options.expected_stream)
    |> add_optional_int_header(
      "Nats-Expected-Last-Sequence",
      options.expected_last_seq,
    )
    |> add_optional_int_header(
      "Nats-Expected-Last-Subject-Sequence",
      options.expected_last_subject_seq,
    )

  let message = guppy.NatsMessage(subject:, reply_to: None, headers:, payload:)

  let timeout = option.unwrap(options.timeout, js.request_timeout)

  use msg <- make_request_with_timeout(js, message, timeout)
  use decoded_result <- result.try(decode_response(js, msg, pub_ack_decoder()))
  decoded_result
  |> result.map_error(map_api_error)
}

@internal
pub fn pub_ack_decoder() -> Decoder(Result(PubAck, StreamApiError)) {
  use <- decode_stream_api_error()
  use stream <- decode.field("stream", decode.string)
  use seq <- decode.field("seq", decode.int)
  use duplicate <- decode.optional_field("duplicate", False, decode.bool)
  decode.success(Ok(PubAck(stream:, seq:, duplicate:)))
}

fn add_optional_header(
  headers: List(#(String, String)),
  key: String,
  value: Option(String),
) -> List(#(String, String)) {
  case value {
    Some(v) -> [#(key, v), ..headers]
    None -> headers
  }
}

fn add_optional_int_header(
  headers: List(#(String, String)),
  key: String,
  value: Option(Int),
) -> List(#(String, String)) {
  case value {
    Some(v) -> [#(key, int.to_string(v)), ..headers]
    None -> headers
  }
}

// ── Delivery & Ack ──────────────────────────────────────────────────────────

pub type DeliveryInfo {
  DeliveryInfo(
    stream_seq: Int,
    consumer_seq: Int,
    timestamp: Timestamp,
    delivery_count: Int,
    pending: Int,
  )
}

// $JS.ACK.nmea.guppy_example.1.1029024.940949.1777365454785427545.0
@internal
pub fn parse_ack(value: String) -> Result(DeliveryInfo, Nil) {
  case string.split(value, ".") {
    [
      "$JS",
      "ACK",
      _stream_name,
      _consumer_name,
      delivery_count,
      stream_seq,
      consumer_seq,
      timestamp,
      pending,
    ] -> {
      use stream_seq <- result.try(int.parse(stream_seq))
      use consumer_seq <- result.try(int.parse(consumer_seq))
      use timestamp <- result.try(int.parse(timestamp))
      use delivery_count <- result.try(int.parse(delivery_count))
      use pending <- result.try(int.parse(pending))

      let timestamp =
        timestamp.from_unix_seconds_and_nanoseconds(
          timestamp / 1_000_000_000,
          timestamp % 1_000_000_000,
        )
      Ok(DeliveryInfo(
        stream_seq:,
        consumer_seq:,
        timestamp:,
        delivery_count:,
        pending:,
      ))
    }
    _ -> Error(Nil)
  }
}

pub type AckAction {
  Ack
  Nak
  NakWithDelay(delay: duration.Duration)
  Progress
  Term
}

@internal
pub fn ack_action_to_payload(action: AckAction) -> BitArray {
  case action {
    Ack -> <<>>
    Nak -> <<"-NAK">>
    NakWithDelay(delay:) -> {
      let delay_ns = duration.to_milliseconds(delay) * 1_000_000
      let delay_str = int.to_string(delay_ns)
      <<"-NAK {\"delay\": ":utf8, delay_str:utf8, "}":utf8>>
    }
    Progress -> <<"+WIP">>
    Term -> <<"+TERM">>
  }
}

// ── Internal Helpers ─────────────────────────────────────────────────────────

fn make_request(
  js: JetstreamContext,
  msg: guppy.NatsMessage,
  next: fn(guppy.NatsMessage) -> Result(a, JetstreamError),
) -> Result(a, JetstreamError) {
  case guppy.request(js.conn, msg, js.request_timeout) {
    Ok(response) -> next(response)
    Error(err) -> Error(ConnectionError(err))
  }
}

fn make_request_with_timeout(
  js: JetstreamContext,
  msg: guppy.NatsMessage,
  timeout: Int,
  next: fn(guppy.NatsMessage) -> Result(a, JetstreamError),
) -> Result(a, JetstreamError) {
  case guppy.request(js.conn, msg, timeout) {
    Ok(response) -> next(response)
    Error(err) -> Error(ConnectionError(err))
  }
}

fn decode_response(js: JetstreamContext, msg: guppy.NatsMessage, decoder) {
  js.log(msg |> string.inspect)
  js.log(msg.payload |> bit_array.to_string |> result.unwrap("<<binary>>"))
  json.parse_bits(msg.payload, decoder)
  |> result.map_error(ResponseDecodeError(_, msg.payload))
}

fn map_api_error(err: StreamApiError) {
  case err.err_code {
    10_059 -> StreamNotFound
    10_058 -> StreamAlreadyExistsWithDifferentConfig
    10_014 -> ConsumerNotFound
    10_071 -> WrongLastSequence
    10_072 -> WrongLastSubjectSequence
    10_074 -> MessageTooLarge
    _ ->
      ApiError(
        code: err.code,
        description: err.description,
        err_code: err.err_code,
      )
  }
}

fn decode_stream_api_error(
  callback: fn() -> Decoder(Result(a, StreamApiError)),
) -> Decoder(Result(a, StreamApiError)) {
  use error <- decode.optional_field("error", None, {
    use code <- decode.field("code", decode.int)
    use description <- decode.optional_field("description", "", decode.string)
    use err_code <- decode.optional_field("err_code", -1, decode.int)
    decode.success(Some(StreamApiError(code, description, err_code)))
  })
  case error {
    Some(error) -> decode.success(Error(error))
    None -> callback()
  }
}

fn decode_timestamp() {
  decode.then(decode.string, fn(ts_string) {
    case timestamp.parse_rfc3339(ts_string) {
      Ok(ts) -> decode.success(ts)
      Error(_) -> decode.failure(timestamp.from_unix_seconds(0), "Timestamp")
    }
  })
}

fn duration_to_json(duration: Duration) -> json.Json {
  let #(seconds, nano_seconds) = duration.to_seconds_and_nanoseconds(duration)
  json.int(seconds * 1_000_000_000 + nano_seconds)
}

fn duration_from_ns_decoder() -> Decoder(Duration) {
  decode.then(decode.int, fn(ns) { decode.success(duration.nanoseconds(ns)) })
}

fn optional_field(
  options: List(#(b, json.Json)),
  key: b,
  value: Option(a),
  mapper: fn(a) -> json.Json,
) -> List(#(b, json.Json)) {
  case value {
    Some(value) -> [#(key, mapper(value)), ..options]
    None -> options
  }
}

fn json_payload(json: json.Json) {
  json.to_string(json)
  |> bit_array.from_string
}
