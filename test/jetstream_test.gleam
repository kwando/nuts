import friendly_id
import gleam/bit_array
import gleam/bool
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/io
import gleam/option.{None, Some}
import gleam/result
import gleam/string
import gleam_community/ansi
import guppy.{type Message} as nats
import guppy/jetstream.{type DeliveryInfo}
import guppy/jetstream/push_consumer
import guppy/jetstream/simple_consumer
import guppy/test_logger
import guppy/test_utils

pub fn consumer_test() {
  use conn <- test_utils.with_client()
  let nats_conn = conn
  let conn = jetstream.new_context(conn)

  let assert Ok(_) = jetstream.list_stream_names(conn)

  let stream_config =
    jetstream.StreamOptions(
      stream_name: "my_stream",
      description: Some("hello world"),
      subjects: ["bar.*"],
      retention: jetstream.Limits,
      max_consumers: -1,
      max_msgs: -1,
      max_bytes: -1,
      max_age: 0,
      storage: jetstream.Memory,
      num_replicas: 1,
      discard_policy: jetstream.DiscardOld,
    )
  let assert Ok(_) = jetstream.create_stream(conn, stream_config)
  let assert Ok(_) = jetstream.create_stream(conn, stream_config)

  let consumer_name = "my_stream_consumer"
  let assert Ok(_) =
    jetstream.create_consumer(
      conn,
      stream: "my_stream",
      consumer_name:,
      config: jetstream.ConsumerConfig(
        ..jetstream.default_consumer_config(),
        durable: True,
        deliver_policy: jetstream.DeliverAll,
        ack_policy: jetstream.AckExplicit,
        max_deliver: 10,
      ),
    )

  let assert Ok(info) =
    jetstream.get_consumer_info(conn, stream: "my_stream", consumer_name:)
  assert info.stream_name == "my_stream"
  assert info.name == consumer_name
  assert info.config.durable == True
  assert info.config.ack_policy == jetstream.AckExplicit
  assert info.config.replay_policy == jetstream.Instant

  let assert Error(jetstream.ConsumerNotFound) =
    jetstream.get_consumer_info(
      conn,
      stream: "my_stream",
      consumer_name: "nonexistent_consumer",
    )

  let assert Ok(deleted) =
    jetstream.delete_consumer(conn, stream: "my_stream", consumer_name:)
  assert deleted.success == True

  let assert Error(jetstream.ConsumerNotFound) =
    jetstream.get_consumer_info(conn, stream: "my_stream", consumer_name:)

  let assert Ok(_) =
    jetstream.create_consumer(
      conn,
      stream: "my_stream",
      consumer_name:,
      config: jetstream.ConsumerConfig(
        ..jetstream.default_consumer_config(),
        durable: True,
        deliver_policy: jetstream.DeliverAll,
        ack_policy: jetstream.AckExplicit,
        max_deliver: 10,
      ),
    )

  let assert Ok(updated) =
    jetstream.update_stream(
      conn,
      jetstream.StreamOptions(
        ..stream_config,
        description: Some("updated description"),
        subjects: ["bar.*", "baz.*"],
      ),
    )
  assert updated.config.description == Some("updated description")
  assert updated.config.subjects == ["bar.*", "baz.*"]

  let gen = friendly_id.new_generator()
  process.spawn(fn() { producer_loop(nats_conn, "bar.1", 100, gen) })
  process.spawn(fn() { producer_loop(nats_conn, "bar.2", 100, gen) })
  process.spawn(fn() { producer_loop(nats_conn, "bar.3", 100, gen) })
  process.spawn(fn() { producer_loop(nats_conn, "bar.4", 100, gen) })
  process.spawn(fn() { producer_loop(nats_conn, "bar.5", 100, gen) })

  let assert Ok(_) =
    simple_consumer.start(
      nats_conn,
      "my_stream",
      consumer_name,
      1000,
      500,
      fn(msg, info: DeliveryInfo) {
        use <- bool.guard(when: True, return: simple_consumer.ack())
        io.println_error(
          msg.subject
          <> " "
          <> info.stream_seq |> int.to_string |> ansi.yellow
          <> " "
          <> info.delivery_count |> string.inspect
          <> " "
          <> msg.payload
          |> bit_array.to_string
          |> result.map(ansi.cyan)
          |> result.lazy_unwrap(fn() { ansi.red("<<binary>>") }),
        )

        simple_consumer.ack()
      },
    )

  process.sleep(200)

  let assert Error(jetstream.StreamAlreadyExistsWithDifferentConfig) =
    jetstream.create_stream(
      conn,
      jetstream.StreamOptions(
        ..stream_config,
        description: Some("this is another description"),
      ),
    )

  let assert Ok(_) = jetstream.get_info(conn, "my_stream")

  let assert Ok(_) = jetstream.delete_stream(conn, "my_stream")

  let assert Error(jetstream.StreamNotFound) =
    jetstream.delete_stream(conn, "my_stream")

  let assert Error(jetstream.StreamNotFound) =
    jetstream.get_info(conn, "my_stream")
}

fn producer_loop(conn: Subject(Message), subject: String, sleep: Int, gen) {
  let _ =
    nats.publish(
      conn,
      nats.new_message(
        subject,
        friendly_id.generate(gen) |> bit_array.from_string,
      ),
    )
  process.sleep(int.random(sleep))
  producer_loop(conn, subject, sleep, gen)
}

pub fn stream_publish_test() {
  use conn <- test_utils.with_client()
  let js = jetstream.new_context(conn)

  // Create a memory-backed stream that captures messages on the "pub.test" subject.
  let stream_config =
    jetstream.StreamOptions(
      stream_name: "pub_test_stream",
      description: None,
      subjects: ["pub.test"],
      retention: jetstream.Limits,
      max_consumers: -1,
      max_msgs: -1,
      max_bytes: -1,
      max_age: 0,
      storage: jetstream.Memory,
      num_replicas: 1,
      discard_policy: jetstream.DiscardOld,
    )
  let assert Ok(_) = jetstream.create_stream(js, stream_config)

  // Test 1: Basic synchronous publish.
  // jetstream.publish sends the message and waits for a PubAck from the server.
  // The response tells us which stream stored the message and the assigned sequence number.
  let assert Ok(ack) =
    jetstream.publish(
      js,
      "pub.test",
      <<"hello">>,
      jetstream.default_publish_options(),
    )
  assert ack.stream == "pub_test_stream"
  assert ack.seq == 1
  assert ack.duplicate == False

  // Test 2: Message deduplication via the Nats-Msg-Id header.
  // When two publishes carry the same msg_id within the stream's duplicate window,
  // JetStream stores the first and rejects the second as a duplicate.
  let dedup_opts =
    jetstream.PublishOptions(
      ..jetstream.default_publish_options(),
      msg_id: Some("my-msg-id"),
    )
  let assert Ok(ack2) =
    jetstream.publish(js, "pub.test", <<"hello2">>, dedup_opts)
  assert ack2.seq == 2
  assert ack2.duplicate == False

  let assert Ok(ack3) =
    jetstream.publish(js, "pub.test", <<"hello2">>, dedup_opts)
  assert ack3.duplicate == True

  // Test 3: Optimistic concurrency control using expected_last_seq.
  // The server rejects the publish if the stream's current sequence does not match
  // the expected value. Here we deliberately pass a stale sequence (999) to force a failure.
  let bad_opts =
    jetstream.PublishOptions(
      ..jetstream.default_publish_options(),
      expected_last_seq: Some(999),
    )
  let assert Error(jetstream.WrongLastSequence) =
    jetstream.publish(js, "pub.test", <<"hello3">>, bad_opts)

  // Clean up: remove the test stream so the test is self-contained.
  let assert Ok(_) = jetstream.delete_stream(js, "pub_test_stream")
}

pub fn push_consumer_test() {
  use conn <- test_utils.with_client()
  let js = jetstream.new_context(conn)

  let stream_name = "push_consumer_test_stream"
  let _ = jetstream.delete_stream(js, stream_name)
  let assert Ok(_) =
    jetstream.create_stream(
      js,
      jetstream.StreamOptions(
        stream_name:,
        description: None,
        subjects: ["push.test"],
        retention: jetstream.Limits,
        max_consumers: -1,
        max_msgs: -1,
        max_bytes: -1,
        max_age: 0,
        storage: jetstream.Memory,
        num_replicas: 1,
        discard_policy: jetstream.DiscardOld,
      ),
    )

  // Create the consumer with a known deliver subject
  let consumer_name = "push_test_consumer"
  let deliver_subject = "_INBOX.push_test." <> nats.random_string(10)
  let assert Ok(_) =
    jetstream.create_consumer(
      js,
      stream: stream_name,
      consumer_name:,
      config: jetstream.ConsumerConfig(
        ..jetstream.default_consumer_config(),
        durable: True,
        deliver_policy: jetstream.DeliverAll,
        ack_policy: jetstream.AckExplicit,
        max_deliver: 10,
        deliver_subject: Some(deliver_subject),
      ),
    )

  // Publish messages before starting the consumer
  let assert Ok(_) =
    jetstream.publish(
      js,
      "push.test",
      <<"hello push">>,
      jetstream.default_publish_options(),
    )
  let assert Ok(_) =
    jetstream.publish(
      js,
      "push.test",
      <<"world push">>,
      jetstream.default_publish_options(),
    )

  // Start push consumer that will receive all published messages
  let collector = process.new_subject()
  let logger = test_logger.test_logger()
  let assert Ok(_) =
    push_consumer.start(
      conn,
      deliver_subject:,
      deliver_group: None,
      handler: fn(msg, _info: DeliveryInfo) {
        process.send(collector, msg.payload)
        push_consumer.ack()
      },
      logger: Some(logger |> test_logger.to_guppy_logger),
    )

  let assert Ok(payload1) = process.receive(collector, 5000)
    as "first message not received"
  assert payload1 == <<"hello push">>

  let assert Ok(payload2) = process.receive(collector, 5000)
    as "second message not received"
  assert payload2 == <<"world push">>

  // Test publishing additional messages to a push consumer
  let assert Ok(_) =
    jetstream.publish(
      js,
      "push.test",
      <<"third message">>,
      jetstream.default_publish_options(),
    )

  let assert Ok(payload3) = process.receive(collector, 5000)
    as "third message not received"
  assert payload3 == <<"third message">>

  // Clean up
  let assert Ok(_) =
    jetstream.delete_consumer(js, stream: stream_name, consumer_name:)
  let assert Ok(_) = jetstream.delete_stream(js, stream_name)
}
