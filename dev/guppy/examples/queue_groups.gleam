import gleam/erlang/process
import gleam/int
import gleam/option.{None, Some}
import guppy
import guppy/jetstream
import guppy/jetstream/simple_consumer

pub fn main() {
  let assert Ok(conn) =
    guppy.new("100.85.25.88", 4222)
    |> guppy.start

  let conn = conn.data

  process.sleep(500)

  let js_ctx = jetstream.new_context(conn)

  let stream_name = "guppy-work-queue"
  create_stream(js_ctx, stream_name)

  process.spawn(fn() { publish_loop(js_ctx) })

  let _ =
    jetstream.create_consumer(
      js_ctx,
      stream_name,
      "guppy-example",
      jetstream.ConsumerConfig(
        ..jetstream.default_consumer_config(),
        durable: True,
        deliver_group: Some("workers"),
      ),
    )
  let assert Ok(_) =
    simple_consumer.start(
      conn,
      stream_name:,
      consumer_name: "guppy-example",
      max_messages: 10,
      threshold: 5,
      handler: fn(msg, info) {
        echo msg
        echo info
        simple_consumer.ack()
      },
      logger: None,
    )

  process.sleep_forever()
}

fn publish_loop(js_ctx) {
  let assert Ok(_) = publish_job(js_ctx, <<"foo">>)
  process.sleep(int.random(1000))
  let assert Ok(_) = publish_job(js_ctx, <<"bar">>)
  process.sleep(int.random(3000))
  publish_loop(js_ctx)
}

fn create_stream(
  js_ctx: jetstream.JetstreamContext,
  stream_name: String,
) -> Nil {
  // delete stream if it already exists
  let _ = jetstream.delete_stream(js_ctx, stream_name)
  let assert Ok(_) =
    js_ctx
    |> jetstream.create_stream(jetstream.StreamOptions(
      stream_name:,
      description: Some("Work queue example"),
      subjects: ["jobs.>"],
      retention: jetstream.Workqueue,
      discard_policy: jetstream.DiscardNew,
      max_consumers: 20,
      max_msgs: 0,
      max_bytes: 0,
      max_age: 0,
      storage: jetstream.Memory,
      num_replicas: 1,
    ))
  Nil
}

fn publish_job(
  js_ctx: jetstream.JetstreamContext,
  payload: BitArray,
) -> Result(jetstream.PubAck, jetstream.JetstreamError) {
  let assert Ok(_) =
    jetstream.publish(
      js_ctx,
      "jobs.download",
      payload,
      jetstream.PublishOptions(
        msg_id: Some(int.random(1_000_000) |> int.to_string),
        expected_stream: None,
        expected_last_seq: None,
        expected_last_subject_seq: None,
        timeout: Some(1000),
      ),
    )
}
