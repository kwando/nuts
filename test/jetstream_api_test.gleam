import gleam/bit_array
import gleam/dynamic/decode
import gleam/json
import gleam/option.{type Option, None, Some}
import gleam/time/duration
import nuts/internal/jetstream_api
import simplifile

pub fn consumer_get_info_request_subject_test() {
  let msg =
    jetstream_api.consumer_get_info_request(
      stream: "my_stream",
      consumer_name: "my_consumer",
    )
  assert msg.subject == "$JS.API.CONSUMER.INFO.my_stream.my_consumer"
}

pub fn consumer_get_info_response_decoder_test() {
  let assert Ok(payload) =
    simplifile.read_bits("test/fixtures/consumer_info_response.json")
  let assert Ok(result) =
    json.parse_bits(payload, jetstream_api.consumer_get_info_response_decoder())
  let assert Ok(info) = result
  assert info.stream_name == "my_stream"
  assert info.name == "my_stream_consumer"
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
    json.parse_bits(payload, jetstream_api.consumer_get_info_response_decoder())
  let assert Error(err) = result
  assert err.err_code == 10_014
}

pub fn default_config_subject_test() {
  let config = jetstream_api.default_consumer_config()
  let msg =
    jetstream_api.consumer_create_request(
      stream: "events",
      consumer_name: "worker-1",
      config:,
    )
  assert msg.subject == "$JS.API.CONSUMER.CREATE.events.worker-1"
}

pub fn default_config_has_no_optional_fields_test() {
  let config = jetstream_api.default_consumer_config()
  let msg =
    jetstream_api.consumer_create_request(
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
    jetstream_api.ConsumerConfig(
      ..jetstream_api.default_consumer_config(),
      durable: True,
      description: Some("my worker"),
    )
  let msg =
    jetstream_api.consumer_create_request(
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
    jetstream_api.ConsumerConfig(
      description: Some("my worker"),
      durable: True,
      deliver_policy: jetstream_api.ByStartSequence(42),
      ack_policy: jetstream_api.AckAll,
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
      replay_policy: jetstream_api.Original,
    )
  let msg =
    jetstream_api.consumer_create_request(
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
}

pub fn ephemeral_omits_durable_name_test() {
  let config =
    jetstream_api.ConsumerConfig(
      ..jetstream_api.default_consumer_config(),
      durable: False,
    )
  let msg =
    jetstream_api.consumer_create_request(
      stream: "events",
      consumer_name: "ephemeral-1",
      config:,
    )
  assert msg.subject == "$JS.API.CONSUMER.CREATE.events.ephemeral-1"
  let parsed = decode_payload(msg.payload)
  assert parsed.config.durable_name == None
}

fn decode_payload(payload: BitArray) -> DecodedMessage {
  let assert Ok(str) = bit_array.to_string(payload)
  let assert Ok(decoded) = json.parse(str, dynamic())
  decoded
}

type DecodedMessage {
  DecodedMessage(stream_name: String, config: DecodedConfig)
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
  ))
}
