import gleam/int
import gleam/json
import gleam/option.{type Option}
import gleam/result
import gleam/string
import gleam/time/calendar
import gleam/time/duration
import gleam/time/timestamp.{type Timestamp}

pub type ConsumerRequest {
  CreateConsumer(stream_name: String, config: ConsumerConfig)
  PullNextMessages(batch: Int, expires: duration.Duration)
}

pub fn create_durable_consumer(
  stream stream_name: String,
  durable_name name: String,
) {
  CreateConsumer(
    stream_name:,
    config: ConsumerConfig(
      deliver_policy: New,
      ack_policy: AckExplicit,
      replay_policy: Instant,
      durable_name: option.Some(name),
      description: option.None,
    ),
  )
}

pub fn create_consumer_topic(
  stream_name stream_name: String,
  consumer_name consumer_name: String,
) -> String {
  "$JS.API.CONSUMER.CREATE." <> stream_name <> "." <> consumer_name
}

pub fn consumer_request_to_json(
  consumer_request: ConsumerRequest,
) -> json.Json {
  case consumer_request {
    CreateConsumer(stream_name:, config:) ->
      json.object([
        #("stream_name", json.string(stream_name)),
        #("config", consumer_config_to_json(config)),
      ])
    PullNextMessages(batch:, expires:) -> {
      json.object([
        #("batch", json.int(batch)),
        #("expires", json.int(duration.to_milliseconds(expires) * 1_000_000)),
      ])
    }
  }
}

pub type DeliverPolicy {
  All
  Last
  New
  ByStartSeq(Int)
  ByStartTime(Timestamp)
  LastPerSubject
}

pub type AckPolicy {
  AckAll
  AckNone
  AckExplicit
}

pub type ReplayPolicy {
  Instant
  Original
}

pub type ConsumerConfig {
  ConsumerConfig(
    deliver_policy: DeliverPolicy,
    ack_policy: AckPolicy,
    replay_policy: ReplayPolicy,
    durable_name: Option(String),
    description: Option(String),
  )
}

fn consumer_config_to_json(consumer_config: ConsumerConfig) -> json.Json {
  let ConsumerConfig(
    deliver_policy:,
    ack_policy:,
    replay_policy:,
    durable_name:,
    description:,
  ) = consumer_config
  json.object(
    [
      #(
        "ack_policy",
        json.string(case ack_policy {
          AckAll -> "all"
          AckNone -> "none"
          AckExplicit -> "explicit"
        }),
      ),
      #(
        "replay_policy",
        json.string(case replay_policy {
          Instant -> "instant"
          Original -> "original"
        }),
      ),
      ..deliver_policy_to_properties(deliver_policy)
    ]
    |> optional("durable_name", durable_name, json.string)
    |> optional("description", description, json.string),
  )
}

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

fn deliver_policy_to_properties(
  deliver_policy: DeliverPolicy,
) -> List(#(String, json.Json)) {
  case deliver_policy {
    All -> [#("deliver_policy", json.string("all"))]
    Last -> [#("deliver_policy", json.string("last"))]
    New -> [#("deliver_policy", json.string("new"))]
    ByStartSeq(seq) -> [
      #("deliver_policy", json.string("by_start_sequence")),
      #("opt_start_seq", json.int(seq)),
    ]
    ByStartTime(time) -> [
      #("deliver_policy", json.string("by_start_time")),
      #(
        "opt_start_time",
        json.string(timestamp.to_rfc3339(time, calendar.utc_offset)),
      ),
    ]
    LastPerSubject -> [#("deliver_policy", json.string("last_per_subject"))]
  }
}

pub type DeliveryInfo {
  DeliveryInfo(stream_seq: Int, consumer_seq: Int, timestamp: Timestamp)
}

// $JS.ACK.nmea.nuts_example.1.1029024.940949.1777365454785427545.0
pub fn parse_ack(value: String) -> Result(DeliveryInfo, Nil) {
  case string.split(value, ".") {
    [
      "$JS",
      "ACK",
      _stream_name,
      _consumer_name,
      _,
      stream_seq,
      consumer_seq,
      timestamp,
      _,
    ] -> {
      use stream_seq <- result.try(int.parse(stream_seq))
      use consumer_seq <- result.try(int.parse(consumer_seq))
      use timestamp <- result.try(int.parse(timestamp))

      let timestamp =
        timestamp.from_unix_seconds_and_nanoseconds(
          timestamp / 1_000_000_000,
          timestamp % 1_000_000_000,
        )
      Ok(DeliveryInfo(stream_seq:, consumer_seq:, timestamp:))
    }
    _ -> Error(Nil)
  }
}
