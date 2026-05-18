import gleam/bit_array
import gleam/erlang/process
import gleam/io
import gleam/option
import gleam/otp/actor
import gleam/result
import gleam/string
import gleam/time/calendar
import gleam/time/duration
import gleam/time/timestamp
import gleam_community/ansi
import guppy
import guppy/jetstream
import guppy/jetstream/simple_consumer

pub fn main() {
  let assert Ok(actor.Started(_, conn)) =
    guppy.start(
      guppy.new("100.121.244.19", 4222),
      //|> guppy.with_logger(guppy.default_logger("nats: ")),
    )

  let _ =
    setup(conn, fn(msg, info: jetstream.DeliveryInfo) {
      io.println_error(
        msg.subject |> ansi.green
        <> " "
        <> info.timestamp
        |> truncate_timestamp
        |> timestamp.to_rfc3339(calendar.utc_offset)
        |> string.pad_end(24, " ")
        |> ansi.yellow
        <> " "
        <> msg.payload |> bit_array.to_string |> result.unwrap("<<binary>>"),
      )
      simple_consumer.ack()
    })

  process.sleep_forever()
}

fn truncate_timestamp(ts: timestamp.Timestamp) {
  let #(seconds, nano_seconds) = timestamp.to_unix_seconds_and_nanoseconds(ts)
  timestamp.from_unix_seconds_and_nanoseconds(
    seconds,
    { nano_seconds / 1_000_000 } * 1_000_000,
  )
}

fn setup(
  conn: process.Subject(guppy.Message),
  handler: fn(guppy.NatsMessage, jetstream.DeliveryInfo) ->
    simple_consumer.HandlerReply,
) -> Result(actor.Started(Nil), actor.StartError) {
  let js = jetstream.new_context(conn)
  process.sleep(500)
  let assert Ok(_) =
    jetstream.create_stream(
      js,
      jetstream.StreamOptions(
        stream_name: "nmea",
        description: option.None,
        subjects: ["naboo.nmea"],
        retention: jetstream.Limits,
        discard_policy: jetstream.DiscardOld,
        max_consumers: -1,
        max_msgs: 0,
        max_bytes: 1024 * 1024 * 32,
        max_age: 0,
        storage: jetstream.Memory,
        num_replicas: 0,
      ),
    )
    |> echo

  let assert Ok(_) =
    jetstream.create_consumer(
      js,
      "nmea",
      "guppy_example",
      jetstream.ConsumerConfig(
        ..jetstream.default_consumer_config(),
        durable: False,
        deliver_policy: jetstream.DeliverAll,
        ack_policy: jetstream.AckExplicit,
        max_deliver: 10,
        inactive_threshold: option.Some(duration.seconds(30)),
      ),
    )
    |> echo

  let _ =
    simple_consumer.start(
      conn,
      stream_name: "nmea",
      consumer_name: "guppy_example",
      max_messages: 5000,
      threshold: 2500,
      handler:,
      logger: option.None,
    )
}
