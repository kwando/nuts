import gleam/bit_array
import gleam/dynamic/decode
import gleam/json
import gleam/option.{type Option, None, Some}
import gleam/time/duration
import guppy/jetstream
import simplifile

pub fn consumer_get_info_request_subject_test() {
  let msg =
    jetstream.consumer_get_info_request(
      stream: "my_stream",
      consumer_name: "my_consumer",
    )
  assert msg.subject == "$JS.API.CONSUMER.INFO.my_stream.my_consumer"
}

pub fn consumer_delete_request_subject_test() {
  let msg =
    jetstream.consumer_delete_request(
      stream: "my_stream",
      consumer_name: "my_consumer",
    )
  assert msg.subject == "$JS.API.CONSUMER.DELETE.my_stream.my_consumer"
}

pub fn consumer_delete_response_decoder_test() {
  let payload = bit_array.from_string("{\"success\":true}")
  let assert Ok(result) =
    json.parse_bits(payload, jetstream.consumer_delete_response_decoder())
  let assert Ok(response) = result
  assert response.success == True
}

pub fn consumer_delete_response_error_test() {
  let payload =
    bit_array.from_string(
      "{\"error\":{\"code\":404,\"description\":\"consumer not found\",\"err_code\":10014}}",
    )
  let assert Ok(result) =
    json.parse_bits(payload, jetstream.consumer_delete_response_decoder())
  let assert Error(err) = result
  assert err.err_code == 10_014
}

pub fn stream_update_request_subject_test() {
  let request =
    jetstream.StreamOptions(
      stream_name: "events",
      description: Some("updated stream"),
      subjects: ["foo.>", "bar.>"],
      retention: jetstream.Limits,
      max_consumers: -1,
      max_msgs: -1,
      max_bytes: -1,
      max_age: 0,
      storage: jetstream.Memory,
      num_replicas: 1,
      discard_policy: jetstream.DiscardOld,
    )
  let msg = jetstream.stream_update_request(request)
  assert msg.subject == "$JS.API.STREAM.UPDATE.events"
  let assert Ok(str) = bit_array.to_string(msg.payload)
  let assert Ok(decoded) = json.parse(str, update_request_decoder())
  assert decoded.name == "events"
  assert decoded.description == Some("updated stream")
  assert decoded.subjects == ["foo.>", "bar.>"]
}

pub fn stream_update_response_decoder_test() {
  let assert Ok(payload) =
    simplifile.read_bits("test/fixtures/stream_info_response.json")
  let assert Ok(result) =
    json.parse_bits(payload, jetstream.stream_update_response_decoder())
  let assert Ok(info) = result
  assert info.config.name == "my_stream"
}

pub fn consumer_get_info_response_decoder_test() {
  let assert Ok(payload) =
    simplifile.read_bits("test/fixtures/consumer_info_response.json")
  let assert Ok(result) =
    json.parse_bits(payload, jetstream.consumer_get_info_response_decoder())
  let assert Ok(info) = result
  assert info.stream_name == "my_stream"
  assert info.name == "my_stream_consumer"
  assert info.config.durable == True
  assert info.config.ack_policy == jetstream.AckExplicit
  assert info.config.replay_policy == jetstream.Instant
  assert info.config.deliver_policy == jetstream.DeliverAll
  assert info.config.max_deliver == 10
  assert info.num_pending == 0
  assert info.num_ack_pending == 0
  assert info.num_redelivered == 0
  assert info.num_waiting == 0
}

pub fn consumer_get_info_response_error_test() {
  let payload =
    bit_array.from_string(
      "{\"error\":{\"code\":404,\"description\":\"consumer not found\",\"err_code\":10014}}",
    )
  let assert Ok(result) =
    json.parse_bits(payload, jetstream.consumer_get_info_response_decoder())
  let assert Error(err) = result
  assert err.err_code == 10_014
}

pub fn default_config_subject_test() {
  let config = jetstream.default_consumer_config()
  let msg =
    jetstream.consumer_create_request(
      stream: "events",
      consumer_name: "worker-1",
      config:,
    )
  assert msg.subject == "$JS.API.CONSUMER.CREATE.events.worker-1"
}

pub fn default_config_has_no_optional_fields_test() {
  let config = jetstream.default_consumer_config()
  let msg =
    jetstream.consumer_create_request(
      stream: "events",
      consumer_name: "worker-1",
      config:,
    )
  let parsed = decode_payload(msg.payload)
  assert parsed.stream_name == "events"
  assert parsed.config.ack_policy == "explicit"
  assert parsed.config.replay_policy == "instant"
  assert parsed.config.max_deliver == -1
  assert parsed.config.durable_name == None
  assert parsed.config.description == None
  assert parsed.config.ack_wait == None
  assert parsed.config.max_ack_pending == None
  assert parsed.config.max_waiting == None
  assert parsed.config.backoff == None
  assert parsed.config.inactive_threshold == None
}

pub fn durable_includes_durable_name_in_json_test() {
  let config =
    jetstream.ConsumerConfig(
      ..jetstream.default_consumer_config(),
      durable: True,
      description: Some("my worker"),
    )
  let msg =
    jetstream.consumer_create_request(
      stream: "orders",
      consumer_name: "order-worker",
      config:,
    )
  assert msg.subject == "$JS.API.CONSUMER.CREATE.orders.order-worker"
  let parsed = decode_payload(msg.payload)
  assert parsed.config.durable_name == Some("order-worker")
  assert parsed.config.description == Some("my worker")
}

pub fn all_optional_fields_test() {
  let config =
    jetstream.ConsumerConfig(
      description: Some("my worker"),
      durable: True,
      deliver_policy: jetstream.DeliverByStartSequence(42),
      ack_policy: jetstream.AckAll,
      ack_wait: Some(duration.seconds(30)),
      max_deliver: 5,
      max_ack_pending: Some(100),
      max_waiting: Some(10),
      backoff: Some([
        duration.seconds(5),
        duration.seconds(30),
        duration.seconds(300),
      ]),
      inactive_threshold: Some(duration.minutes(5)),
      replay_policy: jetstream.Original,
      deliver_subject: Some("_INBOX.push.test"),
      deliver_group: Some("my-group"),
      flow_control: True,
      idle_heartbeat: Some(duration.seconds(10)),
      ratelimit: Some(1_000_000),
      headers_only: True,
      mem_storage: False,
    )
  let msg =
    jetstream.consumer_create_request(
      stream: "orders",
      consumer_name: "order-worker",
      config:,
    )
  assert msg.subject == "$JS.API.CONSUMER.CREATE.orders.order-worker"
  let parsed = decode_payload(msg.payload)
  assert parsed.stream_name == "orders"
  assert parsed.config.durable_name == Some("order-worker")
  assert parsed.config.description == Some("my worker")
  assert parsed.config.ack_policy == "all"
  assert parsed.config.replay_policy == "original"
  assert parsed.config.max_deliver == 5
  assert parsed.config.deliver_policy == Some("by_start_sequence")
  assert parsed.config.opt_start_seq == Some(42)
  assert parsed.config.ack_wait == Some(30_000_000_000)
  assert parsed.config.max_ack_pending == Some(100)
  assert parsed.config.max_waiting == Some(10)
  assert parsed.config.inactive_threshold == Some(300_000_000_000)
  let assert Some(backoff) = parsed.config.backoff
  assert backoff == [5_000_000_000, 30_000_000_000, 300_000_000_000]
  assert parsed.config.deliver_subject == Some("_INBOX.push.test")
  assert parsed.config.deliver_group == Some("my-group")
  assert parsed.config.flow_control == True
  assert parsed.config.idle_heartbeat == Some(10_000_000_000)
  assert parsed.config.ratelimit == Some(1_000_000)
  assert parsed.config.headers_only == True
  assert parsed.config.mem_storage == False
}

pub fn ephemeral_omits_durable_name_test() {
  let config =
    jetstream.ConsumerConfig(
      ..jetstream.default_consumer_config(),
      durable: False,
    )
  let msg =
    jetstream.consumer_create_request(
      stream: "events",
      consumer_name: "ephemeral-1",
      config:,
    )
  assert msg.subject == "$JS.API.CONSUMER.CREATE.events.ephemeral-1"
  let parsed = decode_payload(msg.payload)
  assert parsed.config.durable_name == None
}

pub fn push_fields_default_to_none_test() {
  let config = jetstream.default_consumer_config()
  let msg =
    jetstream.consumer_create_request(
      stream: "events",
      consumer_name: "push-worker",
      config:,
    )
  let parsed = decode_payload(msg.payload)
  assert parsed.config.deliver_subject == None
  assert parsed.config.deliver_group == None
  assert parsed.config.flow_control == False
  assert parsed.config.idle_heartbeat == None
  assert parsed.config.ratelimit == None
  assert parsed.config.headers_only == False
}

pub fn push_fields_serialized_when_set_test() {
  let config =
    jetstream.ConsumerConfig(
      ..jetstream.default_consumer_config(),
      deliver_subject: Some("_INBOX.push.abc123"),
      deliver_group: Some("my-group"),
      flow_control: True,
      idle_heartbeat: Some(duration.seconds(10)),
      ratelimit: Some(1_000_000),
      headers_only: True,
    )
  let msg =
    jetstream.consumer_create_request(
      stream: "events",
      consumer_name: "push-worker",
      config:,
    )
  let parsed = decode_payload(msg.payload)
  assert parsed.config.deliver_subject == Some("_INBOX.push.abc123")
  assert parsed.config.deliver_group == Some("my-group")
  assert parsed.config.flow_control == True
  assert parsed.config.idle_heartbeat == Some(10_000_000_000)
  assert parsed.config.ratelimit == Some(1_000_000)
  assert parsed.config.headers_only == True
}

fn decode_payload(payload: BitArray) -> DecodedMessage {
  let assert Ok(str) = bit_array.to_string(payload)
  let assert Ok(decoded) = json.parse(str, dynamic())
  decoded
}

type DecodedMessage {
  DecodedMessage(stream_name: String, config: DecodedConfig)
}

type DecodedUpdateRequest {
  DecodedUpdateRequest(
    name: String,
    description: Option(String),
    subjects: List(String),
  )
}

fn update_request_decoder() -> decode.Decoder(DecodedUpdateRequest) {
  use name <- decode.field("name", decode.string)
  use description <- decode.optional_field(
    "description",
    None,
    decode.optional(decode.string),
  )
  use subjects <- decode.field("subjects", decode.list(decode.string))
  decode.success(DecodedUpdateRequest(name:, description:, subjects:))
}

type DecodedConfig {
  DecodedConfig(
    durable_name: Option(String),
    description: Option(String),
    ack_policy: String,
    replay_policy: String,
    max_deliver: Int,
    deliver_policy: Option(String),
    opt_start_seq: Option(Int),
    ack_wait: Option(Int),
    max_ack_pending: Option(Int),
    max_waiting: Option(Int),
    backoff: Option(List(Int)),
    inactive_threshold: Option(Int),
    deliver_subject: Option(String),
    deliver_group: Option(String),
    flow_control: Bool,
    idle_heartbeat: Option(Int),
    ratelimit: Option(Int),
    headers_only: Bool,
    mem_storage: Bool,
  )
}

fn dynamic() -> decode.Decoder(DecodedMessage) {
  use stream_name <- decode.field("stream_name", decode.string)
  use config <- decode.field("config", config_decoder())
  decode.success(DecodedMessage(stream_name:, config:))
}

fn config_decoder() -> decode.Decoder(DecodedConfig) {
  use ack_policy <- decode.field("ack_policy", decode.string)
  use replay_policy <- decode.field("replay_policy", decode.string)
  use max_deliver <- decode.field("max_deliver", decode.int)
  use deliver_policy <- decode.optional_field(
    "deliver_policy",
    None,
    decode.optional(decode.string),
  )
  use opt_start_seq <- decode.optional_field(
    "opt_start_seq",
    None,
    decode.optional(decode.int),
  )
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
  use ack_wait <- decode.optional_field(
    "ack_wait",
    None,
    decode.optional(decode.int),
  )
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
    decode.optional(decode.list(decode.int)),
  )
  use inactive_threshold <- decode.optional_field(
    "inactive_threshold",
    None,
    decode.optional(decode.int),
  )
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
    decode.optional(decode.int),
  )
  use ratelimit <- decode.optional_field(
    "ratelimit",
    None,
    decode.optional(decode.int),
  )
  use headers_only <- decode.optional_field("headers_only", False, decode.bool)
  use mem_storage <- decode.optional_field("mem_storage", False, decode.bool)
  decode.success(DecodedConfig(
    durable_name:,
    description:,
    ack_policy:,
    replay_policy:,
    max_deliver:,
    deliver_policy:,
    opt_start_seq:,
    ack_wait:,
    max_ack_pending:,
    max_waiting:,
    backoff:,
    inactive_threshold:,
    deliver_subject:,
    deliver_group:,
    flow_control:,
    idle_heartbeat:,
    ratelimit:,
    headers_only:,
    mem_storage:,
  ))
}
