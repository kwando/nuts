import gleam/bit_array
import gleam/bool
import gleam/erlang/process.{type Subject}
import gleam/erlang/reference.{type Reference}
import gleam/int
import gleam/io
import gleam/json
import gleam/option
import gleam/otp/actor
import gleam/result
import gleam/string
import gleam/time/calendar
import gleam/time/duration
import gleam/time/timestamp
import gleam_community/ansi
import nuts
import nuts/internal/jetstream
import nuts/internal/stream_consumer

pub fn cyan_logger(msg) {
  io.println_error(msg |> ansi.cyan)
}

pub fn main() {
  io.println("hello world")

  let nuts_name = process.new_name("nuts")

  nuts.new("100.121.244.19", 4222)
  //|> nuts.with_logger(nuts.Logger(
  //  info: cyan_logger,
  //  debug: cyan_logger,
  //  warning: cyan_logger,
  //))
  |> nuts.start(nuts_name, _)

  let subject = process.named_subject(nuts_name)
  let stream_name = "victron"
  let inbox_name = "my_inbox_" <> int.random(1_000_000_000) |> int.to_string
  let consumer_name = "nuts_example"
  let batch_size = 100

  let create_consumer_json =
    jetstream.create_durable_consumer(
      stream: stream_name,
      durable_name: consumer_name,
    )
    |> jetstream.consumer_request_to_json

  create_consumer_json
  |> json.to_string
  |> io.println

  let assert Ok(subscription) = nuts.subscribe(subject, inbox_name)

  nuts.new_message(
    jetstream.create_consumer_topic(stream_name, consumer_name),
    create_consumer_json
      |> json.to_string
      |> bit_array.from_string,
  )
  |> nuts.reply_to(option.Some(inbox_name))
  |> nuts.publish(subject, _)
  |> echo

  stream_consumer.start(
    stream_consumer.Context(
      inbox_name:,
      stream_name:,
      consumer_name:,
      nats_server: subject,
      batch_size:,
      poll_expire: duration.seconds(5),
    ),
    fn(msg, info) {
      io.println(
        int.to_string(info.stream_seq)
        <> " "
        <> msg.subject |> ansi.cyan
        <> " "
        <> info.timestamp
        |> timestamp.to_rfc3339(calendar.utc_offset)
        |> ansi.green
        <> " "
        <> msg.payload |> bit_array.to_string |> result.unwrap("<<binary>>"),
      )
    },
  )

  //receive_loop(Context(
  //  inbox_name:,
  //  stream_name:,
  //  consumer_name:,
  //  nats_server: subject,
  //  subject: nuts.get_subject(subscription),
  //  pending_messages: 0,
  //))

  process.sleep_forever()
}
