import friendly_id
import gleam/erlang/process
import gleam/io
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/string
import gleam_community/ansi
import guppy
import guppy/internal/basic_consumer
import guppy/jetstream
import guppy/kv

pub fn main() {
  let generator = friendly_id.new_generator()
  let assert Ok(actor.Started(_, conn)) =
    guppy.start(
      guppy.new("100.85.25.88", 4222)
      |> guppy.with_logger(guppy.default_logger("nats")),
    )

  process.sleep(1000)
  let ctx =
    kv.new_context(conn)
    |> kv.with_logger(fn(line) { io.println(line |> ansi.cyan) })

  let bucket_name = "guppy-example"
  let stream_name = "KV_" <> bucket_name
  // delete bucket if it already exists
  let _ = kv.delete_bucket(ctx, bucket_name)
  let assert Ok(_) =
    kv.create_bucket(
      ctx,
      kv.BucketConfig(..kv.default_bucket_config(bucket_name), history: 10),
    )
  let bucket = kv.get_bucket(ctx, bucket_name)

  let consumer_name = friendly_id.generate(generator)
  let _ = kv.put(bucket, "bar", <<"baz">>)
  // setup a watcher
  let assert Ok(_consumer_info) =
    jetstream.create_consumer(
      jetstream.new_context(conn),
      consumer_name:,
      stream: stream_name,
      config: jetstream.ConsumerConfig(
        ..jetstream.default_consumer_config(),
        durable: False,
        headers_only: False,
        deliver_policy: jetstream.DeliverLast,
      ),
    )

  let assert Ok(_) =
    basic_consumer.start(
      conn,
      stream_name:,
      consumer_name:,
      max_messages: 10,
      threshold: 10,
      initial: 0,
      handler: fn(acc, msg, info) {
        echo #(acc, msg, info)
        io.println(
          parse_key_from_subject(bucket_name, msg.subject)
          |> case is_delete(msg) {
            True -> ansi.red
            False -> ansi.green
          }
          <> "="
          <> ansi.magenta(string.inspect(msg.payload)),
        )
        basic_consumer.continue(jetstream.Ack, acc + 1)
      },
      logger: option.None,
    )

  // expect KeyNotFound if there is no data
  let assert Error(kv.KeyNotFound) = kv.get(bucket, "foo")

  // set the key "foo" to "bar"
  let _ = kv.put(bucket, "foo", <<"bar">>)
  let _ = kv.delete_key(bucket, "bar")

  // key should now have been set
  let assert Ok(<<"bar">>) = kv.get(bucket, "foo")

  // the foo key wil be returned here
  let assert Ok(["foo"]) = kv.list_keys(bucket)

  let assert Ok(["foo"]) = kv.list_with_watcher(bucket)
  process.sleep_forever()
}

fn parse_key_from_subject(bucket_name: String, input: String) {
  string.replace(input, "$KV." <> bucket_name <> ".", "")
}

fn is_delete(msg: guppy.NatsMessage) {
  msg.headers |> list.key_find("KV-Operation") == Ok("DEL")
}
