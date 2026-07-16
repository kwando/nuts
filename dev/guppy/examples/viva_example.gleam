import friendly_id
import gleam/bit_array
import gleam/erlang/process
import gleam/io
import gleam/option
import gleam/otp/actor
import gleam/result
import gleam/string
import gleam/time/duration
import gleam/time/timestamp
import gleam_community/ansi
import guppy
import guppy/jetstream
import guppy/jetstream/simple_consumer

pub fn main() {
  let assert Ok(actor.Started(_, conn)) =
    guppy.new("100.85.25.88", 4222)
    |> guppy.with_logger(guppy.default_logger("nats"))
    |> guppy.start

  process.sleep(1000)

  let js_conn = jetstream.new_context(conn)

  let name_gen = friendly_id.new_generator()
  let consumer_name = friendly_id.generate(name_gen)
  let stream = "viva-raw-stream"
  let assert Ok(_) =
    jetstream.create_consumer(
      js_conn,
      consumer_name:,
      stream:,
      config: jetstream.ConsumerConfig(
        ..jetstream.default_consumer_config(),
        durable: False,
        description: option.Some("viva raw stream consumer"),
        deliver_policy: jetstream.DeliverByStartTime(
          timestamp.system_time() |> timestamp.add(duration.minutes(-5)),
        ),
        inactive_threshold: option.Some(duration.milliseconds(10)),
        //jetstream.DeliverAll,
      //jetstream.DeliverLastPerSubject(["viva.station.*"]),
      ),
    )

  let assert Ok(_) =
    simple_consumer.start(
      conn,
      stream_name: stream,
      consumer_name:,
      max_messages: 1000,
      threshold: 500,
      handler: fn(msg, _info) {
        msg
        |> payload_to_string
        |> ansi.magenta
        |> io.println
        simple_consumer.ack()
      },
      logger: option.None,
    )

  process.sleep_forever()
}

fn payload_to_string(msg: guppy.NatsMessage) {
  msg.payload
  |> bit_array.to_string()
  |> result.lazy_unwrap(fn() { string.inspect(msg.payload) })
}
