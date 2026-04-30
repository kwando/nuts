import gleam/dynamic/decode
import gleam/json
import gleam/option.{type Option, None, Some}
import gleam/time/duration.{type Duration}
import nuts

pub type RetentionPolicy {
  Limits
  Interest
  WorkQueue
}

pub type DiscardPolicy {
  Old
  New
}

pub type StorageType {
  File
  Memory
}

pub type StreamConfig {
  StreamConfig(
    name: String,
    subjects: List(String),
    retention: RetentionPolicy,
    max_consumers: Int,
    max_msgs: Int,
    max_bytes: Int,
    max_age: Duration,
    max_msgs_per_subject: Int,
    max_msg_size: Int,
    discard: DiscardPolicy,
    storage: StorageType,
    num_replicas: Int,
    duplicate_window: Duration,
    description: Option(String),
    allow_rollup: Bool,
    allow_direct: Bool,
  )
}

pub type StreamState {
  StreamState(
    messages: Int,
    bytes: Int,
    first_seq: Int,
    first_ts: String,
    last_seq: Int,
    last_ts: String,
    consumer_count: Int,
  )
}

pub type StreamCreateResponse {
  StreamCreateResponse(config: StreamConfig)
}

pub type StreamUpdateResponse {
  StreamUpdateResponse(config: StreamConfig)
}

pub type StreamInfoResponse {
  StreamInfoResponse(
    config: StreamConfig,
    state: StreamState,
    created: Option(String),
  )
}

pub type StreamNamesResponse {
  StreamNamesResponse(streams: List(String))
}

pub type StreamPurgeResult {
  StreamPurgeResult(success: Bool, purged: Int)
}

pub type PubAck {
  PubAck(stream: String, seq: Int, domain: Option(String), duplicate: Bool)
}

pub type JsonApiError {
  JsonApiError(code: Int, err_code: Int, description: String)
}

// ----------------------------------------- Subject builders -----------------------------------------

pub fn create_stream_subject(name: String) -> String {
  "$JS.API.STREAM.CREATE." <> name
}

pub fn stream_info_subject(name: String) -> String {
  "$JS.API.STREAM.INFO." <> name
}

pub fn update_stream_subject(name: String) -> String {
  "$JS.API.STREAM.UPDATE." <> name
}

pub fn stream_names_subject() -> String {
  "$JS.API.STREAM.NAMES"
}

pub fn purge_stream_subject(name: String) -> String {
  "$JS.API.STREAM.PURGE." <> name
}

// ----------------------------------------- JSON encoding -----------------------------------------

pub fn stream_config_to_json(config: StreamConfig) -> json.Json {
  let StreamConfig(
    name:,
    subjects:,
    retention:,
    max_consumers:,
    max_msgs:,
    max_bytes:,
    max_age:,
    max_msgs_per_subject:,
    max_msg_size:,
    discard:,
    storage:,
    num_replicas:,
    duplicate_window:,
    description:,
    allow_rollup:,
    allow_direct:,
  ) = config
  json.object(
    [
      #("name", json.string(name)),
      #("subjects", json.array(subjects, of: json.string)),
      #("retention", json.string(retention_to_string(retention))),
      #("max_consumers", json.int(max_consumers)),
      #("max_msgs", json.int(max_msgs)),
      #("max_bytes", json.int(max_bytes)),
      #("max_age", json.int(duration.to_milliseconds(max_age) * 1_000_000)),
      #("max_msgs_per_subject", json.int(max_msgs_per_subject)),
      #("max_msg_size", json.int(max_msg_size)),
      #("discard", json.string(discard_to_string(discard))),
      #("storage", json.string(storage_to_string(storage))),
      #("num_replicas", json.int(num_replicas)),
      #(
        "duplicate_window",
        json.int(duration.to_milliseconds(duplicate_window) * 1_000_000),
      ),
      #("allow_rollup_hdrs", json.bool(allow_rollup)),
      #("allow_direct", json.bool(allow_direct)),
    ]
    |> optional("description", description, json.string),
  )
}

fn retention_to_string(retention: RetentionPolicy) -> String {
  case retention {
    Limits -> "limits"
    Interest -> "interest"
    WorkQueue -> "workqueue"
  }
}

fn discard_to_string(discard: DiscardPolicy) -> String {
  case discard {
    Old -> "old"
    New -> "new"
  }
}

fn storage_to_string(storage: StorageType) -> String {
  case storage {
    File -> "file"
    Memory -> "memory"
  }
}

// ----------------------------------------- Response decoding -----------------------------------------

pub fn decode_jetstream_response(
  bits: BitArray,
  decoder: decode.Decoder(a),
) -> Result(a, nuts.NatsError) {
  case check_jetstream_error(bits) {
    Some(error) -> Error(nuts.ProtocolError(error.description))
    None ->
      case json.parse_bits(bits, decoder) {
        Ok(value) -> Ok(value)
        Error(decode_err) -> Error(nuts.JsonDecodeError(decode_err, bits))
      }
  }
}

fn check_jetstream_error(bits: BitArray) -> Option(JsonApiError) {
  let error_decoder = {
    use error <- decode.optional_field(
      "error",
      None,
      decode.optional(json_api_error_decoder()),
    )
    decode.success(error)
  }
  case json.parse_bits(bits, error_decoder) {
    Ok(None) -> None
    Ok(Some(error)) -> Some(error)
    Error(_) -> None
  }
}

fn json_api_error_decoder() -> decode.Decoder(JsonApiError) {
  use code <- decode.field("code", decode.int)
  use err_code <- decode.field("err_code", decode.int)
  use description <- decode.field("description", decode.string)
  decode.success(JsonApiError(code:, err_code:, description:))
}

pub fn stream_create_response_decoder() -> decode.Decoder(StreamCreateResponse) {
  use config <- decode.field("config", stream_config_decoder())
  decode.success(StreamCreateResponse(config:))
}

pub fn stream_update_response_decoder() -> decode.Decoder(StreamUpdateResponse) {
  use config <- decode.field("config", stream_config_decoder())
  decode.success(StreamUpdateResponse(config:))
}

pub fn stream_info_response_decoder() -> decode.Decoder(StreamInfoResponse) {
  use config <- decode.field("config", stream_config_decoder())
  use state <- decode.field("state", stream_state_decoder())
  use created <- decode.optional_field(
    "created",
    None,
    decode.optional(decode.string),
  )
  decode.success(StreamInfoResponse(config:, state:, created:))
}

fn stream_config_decoder() -> decode.Decoder(StreamConfig) {
  use name <- decode.field("name", decode.string)
  use subjects <- decode.field("subjects", decode.list(decode.string))
  use retention <- decode.field("retention", decode.string)
  use max_consumers <- decode.field("max_consumers", decode.int)
  use max_msgs <- decode.field("max_msgs", decode.int)
  use max_bytes <- decode.field("max_bytes", decode.int)
  use max_age <- decode.field("max_age", decode.int)
  use max_msgs_per_subject <- decode.field("max_msgs_per_subject", decode.int)
  use max_msg_size <- decode.field("max_msg_size", decode.int)
  use discard <- decode.field("discard", decode.string)
  use storage <- decode.field("storage", decode.string)
  use num_replicas <- decode.field("num_replicas", decode.int)
  use duplicate_window <- decode.field("duplicate_window", decode.int)
  use description <- decode.optional_field(
    "description",
    None,
    decode.optional(decode.string),
  )
  use allow_rollup <- decode.optional_field(
    "allow_rollup_hdrs",
    False,
    decode.bool,
  )
  use allow_direct <- decode.optional_field("allow_direct", False, decode.bool)

  let retention = case retention {
    "limits" -> Limits
    "interest" -> Interest
    "workqueue" -> WorkQueue
    _ -> Limits
  }
  let discard = case discard {
    "old" -> Old
    "new" -> New
    _ -> Old
  }
  let storage = case storage {
    "file" -> File
    "memory" -> Memory
    _ -> File
  }

  decode.success(StreamConfig(
    name:,
    subjects:,
    retention:,
    max_consumers:,
    max_msgs:,
    max_bytes:,
    max_age: duration.milliseconds(max_age / 1_000_000),
    max_msgs_per_subject:,
    max_msg_size:,
    discard:,
    storage:,
    num_replicas:,
    duplicate_window: duration.milliseconds(duplicate_window / 1_000_000),
    description:,
    allow_rollup:,
    allow_direct:,
  ))
}

fn stream_state_decoder() -> decode.Decoder(StreamState) {
  use messages <- decode.field("messages", decode.int)
  use bytes <- decode.field("bytes", decode.int)
  use first_seq <- decode.field("first_seq", decode.int)
  use first_ts <- decode.field("first_ts", decode.string)
  use last_seq <- decode.field("last_seq", decode.int)
  use last_ts <- decode.field("last_ts", decode.string)
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

pub fn stream_names_decoder() -> decode.Decoder(StreamNamesResponse) {
  use streams <- decode.field("streams", decode.list(decode.string))
  decode.success(StreamNamesResponse(streams:))
}

pub fn stream_purge_decoder() -> decode.Decoder(StreamPurgeResult) {
  use success <- decode.field("success", decode.bool)
  use purged <- decode.field("purged", decode.int)
  decode.success(StreamPurgeResult(success:, purged:))
}

pub fn pub_ack_decoder() -> decode.Decoder(PubAck) {
  use stream <- decode.field("stream", decode.string)
  use seq <- decode.field("seq", decode.int)
  use domain <- decode.optional_field(
    "domain",
    None,
    decode.optional(decode.string),
  )
  use duplicate <- decode.optional_field("duplicate", False, decode.bool)
  decode.success(PubAck(stream:, seq:, domain:, duplicate:))
}

// ----------------------------------------- Helpers -----------------------------------------

fn optional(
  list: List(#(String, json.Json)),
  key: String,
  maybe: Option(a),
  mapper: fn(a) -> json.Json,
) -> List(#(String, json.Json)) {
  case maybe {
    option.Some(value) -> [#(key, mapper(value)), ..list]
    option.None -> list
  }
}
