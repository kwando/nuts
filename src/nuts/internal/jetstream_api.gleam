import gleam/bit_array
import gleam/dynamic/decode.{type Decoder}
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import gleam/time/calendar
import gleam/time/duration.{type Duration}
import gleam/time/timestamp.{type Timestamp}
import nuts.{type NatsMessage, NatsMessage}

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

pub type StreamNamesResponse {
  StreamNamesResponse(
    total: Int,
    offset: Int,
    limit: Int,
    streams: List(String),
  )
}

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

pub type StreamConfig {
  StreamConfig(
    name: String,
    description: Option(String),
    subjects: List(String),
  )
}

fn stream_config_decoder() -> Decoder(StreamConfig) {
  use name <- decode.field("name", decode.string)
  use description <- decode.field("description", decode.optional(decode.string))
  use subjects <- decode.field("subjects", decode.list(decode.string))
  decode.success(StreamConfig(name:, description:, subjects:))
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

fn decode_timestamp() {
  decode.then(decode.string, fn(ts_string) {
    case timestamp.parse_rfc3339(ts_string) {
      Ok(ts) -> decode.success(ts)
      Error(_) -> decode.failure(timestamp.from_unix_seconds(0), "Timestamp")
    }
  })
}

pub type StreamGetInfoResponse {
  StreamGetInfoResponse(config: StreamConfig, state: StreamState)
}

pub fn stream_get_info_response_decoder() -> Decoder(
  Result(StreamGetInfoResponse, StreamApiError),
) {
  use <- decode_stream_api_error()
  use config <- decode.field("config", stream_config_decoder())
  use state <- decode.field("state", stream_state_decoder())
  decode.success(Ok(StreamGetInfoResponse(config:, state:)))
}

pub type Retention {
  Limits
  Interest
  Workqueue
}

pub type Storage {
  File
  Memory
}

pub type DiscardPolicy {
  DiscardNew
  DiscardOld
}

pub type StreamCreateRequest {
  StreamCreateRequest(
    stream_name: String,
    description: Option(String),
    subjects: List(String),
    retention: Retention,
    discard_policy: DiscardPolicy,
    max_consumers: Int,
    max_msgs: Int,
    max_bytes: Int,
    max_age: Int,
    storage: Storage,
    num_replicas: Int,
  )
}

pub fn stream_create_request(request: StreamCreateRequest) -> NatsMessage {
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

pub fn stream_create_response_decoder() -> Decoder(
  Result(StreamCreateResponse, StreamApiError),
) {
  use <- decode_stream_api_error()
  use name <- decode.subfield(["config", "name"], decode.string)
  decode.success(Ok(StreamCreated(name)))
}

pub fn stream_delete_request(stream_name: String) -> NatsMessage {
  NatsMessage(
    subject: "$JS.API.STREAM.DELETE." <> stream_name,
    reply_to: None,
    headers: [],
    payload: bit_array.from_string(""),
  )
}

pub type StreamDeleteResponse {
  StreamDeleteResponse(success: Bool)
}

pub fn stream_delete_response_decoder() {
  use <- decode_stream_api_error()
  use success <- decode.field("success", decode.bool)
  decode.success(Ok(StreamDeleteResponse(success)))
}

fn discard_policy_to_string(discard_policy: DiscardPolicy) -> String {
  case discard_policy {
    DiscardNew -> "new"
    DiscardOld -> "old"
  }
}

pub type StreamApiError {
  StreamApiError(code: Int, description: String, err_code: Int)
}

pub type StreamCreateResponse {
  StreamCreated(name: String)
}

pub type DeliverPolicy {
  All
  New
  Last
  ByStartSequence(Int)
  ByStartTime(Timestamp)
}

pub type AckPolicy {
  NoAck
  AckAll
  AckExplicit
}

fn ack_policy_to_json(ack_policy: AckPolicy) -> json.Json {
  case ack_policy {
    NoAck -> json.string("none")
    AckAll -> json.string("all")
    AckExplicit -> json.string("explicit")
  }
}

pub type ReplayPolicy {
  Instant
  Original
}

fn replay_policy_to_json(replay_policy: ReplayPolicy) -> json.Json {
  case replay_policy {
    Instant -> json.string("instant")
    Original -> json.string("original")
  }
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
  )
}

pub fn default_consumer_config() -> ConsumerConfig {
  ConsumerConfig(
    description: None,
    durable: False,
    deliver_policy: All,
    ack_policy: AckExplicit,
    ack_wait: None,
    max_deliver: -1,
    max_ack_pending: None,
    max_waiting: None,
    backoff: None,
    inactive_threshold: None,
    replay_policy: Instant,
  )
}

pub fn consumer_create_request(
  stream stream: String,
  consumer_name consumer_name: String,
  config config: ConsumerConfig,
) -> nuts.NatsMessage {
  let subject = "$JS.API.CONSUMER.CREATE." <> stream <> "." <> consumer_name
  let durable_field = case config.durable {
    True -> [#("durable_name", json.string(consumer_name))]
    False -> []
  }
  nuts.NatsMessage(
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
            ),
          ),
        ),
      ]),
    ),
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

pub type ConsumerGetInfoResponse {
  ConsumerGetInfoResponse(
    stream_name: String,
    name: String,
    created: Timestamp,
    timestamp: Timestamp,
    num_pending: Int,
    num_ack_pending: Int,
    num_redelivered: Int,
    num_waiting: Int,
  )
}

pub fn consumer_get_info_response_decoder() -> Decoder(
  Result(ConsumerGetInfoResponse, StreamApiError),
) {
  use <- decode_stream_api_error()
  use stream_name <- decode.field("stream_name", decode.string)
  use name <- decode.field("name", decode.string)
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
      created:,
      timestamp:,
      num_pending:,
      num_ack_pending:,
      num_redelivered:,
      num_waiting:,
    )),
  )
}

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

fn duration_to_json(duration: Duration) -> json.Json {
  let #(seconds, nano_seconds) = duration.to_seconds_and_nanoseconds(duration)
  json.int(seconds * 1_000_000_000 + nano_seconds)
}

fn deliver_policy_to_json(policy: DeliverPolicy) -> List(#(String, json.Json)) {
  case policy {
    All -> [#("deliver_policy", json.string("all"))]
    New -> [#("deliver_policy", json.string("new"))]
    Last -> [#("deliver_policy", json.string("last"))]
    ByStartSequence(seq) -> [
      #("deliver_policy", json.string("by_start_sequence")),
      #("opt_start_seq", json.int(seq)),
    ]
    ByStartTime(timestamp) -> [
      #("deliver_policy", json.string("last")),
      #(
        "opt_start_time",
        json.string(timestamp.to_rfc3339(timestamp, calendar.utc_offset)),
      ),
    ]
  }
}

fn storage_to_string(storage: Storage) -> String {
  case storage {
    File -> "file"
    Memory -> "memory"
  }
}

fn retention_to_string(retention: Retention) -> String {
  case retention {
    Limits -> "limits"
    Interest -> "interest"
    Workqueue -> "workqueue"
  }
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

pub type DeliveryInfo {
  DeliveryInfo(
    stream_seq: Int,
    consumer_seq: Int,
    timestamp: Timestamp,
    delivery_count: Int,
    pending: Int,
  )
}

// $JS.ACK.nmea.nuts_example.1.1029024.940949.1777365454785427545.0
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
