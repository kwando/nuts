import gleam/bit_array
import gleam/erlang/process
import gleam/list
import gleam/option.{None}
import gleam/time/duration
import gleeunit/should
import nuts as nats
import nuts/internal/stream
import nuts/jetstream
import nuts/test_utils

pub fn create_stream_test() {
  use port <- test_utils.with_nats_server()
  let options = nats.new("127.0.0.1", port)
  let name = process.new_name("nats-jetstream-test")
  let conn = process.named_subject(name)

  let assert Ok(_) = nats.start(name, options)
  assert test_utils.await_connected(conn, 5)

  let config =
    stream.StreamConfig(
      name: "TEST_STREAM",
      subjects: ["test.subject.>"],
      retention: stream.Limits,
      max_consumers: -1,
      max_msgs: -1,
      max_bytes: -1,
      max_age: duration.milliseconds(0),
      max_msgs_per_subject: -1,
      max_msg_size: -1,
      discard: stream.Old,
      storage: stream.Memory,
      num_replicas: 1,
      duplicate_window: duration.milliseconds(120_000),
      description: None,
    )

  let assert Ok(response) = jetstream.create_stream(conn, config)
  response.config.name |> should.equal("TEST_STREAM")
}

pub fn stream_info_test() {
  use port <- test_utils.with_nats_server()
  let options = nats.new("127.0.0.1", port)
  let name = process.new_name("nats-jetstream-test")
  let conn = process.named_subject(name)

  let assert Ok(_) = nats.start(name, options)
  assert test_utils.await_connected(conn, 5)

  let config =
    stream.StreamConfig(
      name: "INFO_TEST",
      subjects: ["info.test.>"],
      retention: stream.Limits,
      max_consumers: -1,
      max_msgs: -1,
      max_bytes: -1,
      max_age: duration.milliseconds(0),
      max_msgs_per_subject: -1,
      max_msg_size: -1,
      discard: stream.Old,
      storage: stream.Memory,
      num_replicas: 1,
      duplicate_window: duration.milliseconds(120_000),
      description: None,
    )

  let assert Ok(_) = jetstream.create_stream(conn, config)
  let assert Ok(response) = jetstream.stream_info(conn, "INFO_TEST")
  response.config.name |> should.equal("INFO_TEST")
  response.config.subjects |> should.equal(["info.test.>"])
  response.state.messages |> should.equal(0)
}

pub fn update_stream_test() {
  use port <- test_utils.with_nats_server()
  let options = nats.new("127.0.0.1", port)
  let name = process.new_name("nats-jetstream-test")
  let conn = process.named_subject(name)

  let assert Ok(_) = nats.start(name, options)
  assert test_utils.await_connected(conn, 5)

  let config =
    stream.StreamConfig(
      name: "UPDATE_TEST",
      subjects: ["update.test.>"],
      retention: stream.Limits,
      max_consumers: -1,
      max_msgs: 100,
      max_bytes: -1,
      max_age: duration.milliseconds(0),
      max_msgs_per_subject: -1,
      max_msg_size: -1,
      discard: stream.Old,
      storage: stream.Memory,
      num_replicas: 1,
      duplicate_window: duration.milliseconds(120_000),
      description: None,
    )

  let assert Ok(_) = jetstream.create_stream(conn, config)

  let updated =
    stream.StreamConfig(
      name: "UPDATE_TEST",
      subjects: ["update.test.>"],
      retention: stream.Limits,
      max_consumers: -1,
      max_msgs: 200,
      max_bytes: -1,
      max_age: duration.milliseconds(0),
      max_msgs_per_subject: -1,
      max_msg_size: -1,
      discard: stream.New,
      storage: stream.Memory,
      num_replicas: 1,
      duplicate_window: duration.milliseconds(120_000),
      description: None,
    )

  let assert Ok(response) = jetstream.update_stream(conn, updated)
  response.config.max_msgs |> should.equal(200)
  response.config.discard |> should.equal(stream.New)
}

pub fn list_stream_names_test() {
  use port <- test_utils.with_nats_server()
  let options = nats.new("127.0.0.1", port)
  let name = process.new_name("nats-jetstream-test")
  let conn = process.named_subject(name)

  let assert Ok(_) = nats.start(name, options)
  assert test_utils.await_connected(conn, 5)

  let config =
    stream.StreamConfig(
      name: "NAMES_TEST",
      subjects: ["names.test.>"],
      retention: stream.Limits,
      max_consumers: -1,
      max_msgs: -1,
      max_bytes: -1,
      max_age: duration.milliseconds(0),
      max_msgs_per_subject: -1,
      max_msg_size: -1,
      discard: stream.Old,
      storage: stream.Memory,
      num_replicas: 1,
      duplicate_window: duration.milliseconds(120_000),
      description: None,
    )

  let assert Ok(_) = jetstream.create_stream(conn, config)
  let assert Ok(names) = jetstream.list_stream_names(conn)

  list.any(names.streams, fn(n) { n == "NAMES_TEST" })
  |> should.be_true
}

pub fn purge_stream_test() {
  use port <- test_utils.with_nats_server()
  let options = nats.new("127.0.0.1", port)
  let name = process.new_name("nats-jetstream-test")
  let conn = process.named_subject(name)

  let assert Ok(_) = nats.start(name, options)
  assert test_utils.await_connected(conn, 5)

  let config =
    stream.StreamConfig(
      name: "PURGE_TEST",
      subjects: ["purge.test.>"],
      retention: stream.Limits,
      max_consumers: -1,
      max_msgs: -1,
      max_bytes: -1,
      max_age: duration.milliseconds(0),
      max_msgs_per_subject: -1,
      max_msg_size: -1,
      discard: stream.Old,
      storage: stream.Memory,
      num_replicas: 1,
      duplicate_window: duration.milliseconds(120_000),
      description: None,
    )

  let assert Ok(_) = jetstream.create_stream(conn, config)

  let assert Ok(result) = jetstream.purge_stream(conn, "PURGE_TEST")
  result.success |> should.be_true
  result.purged |> should.equal(0)
}

pub fn create_duplicate_idempotent_test() {
  use port <- test_utils.with_nats_server()
  let options = nats.new("127.0.0.1", port)
  let name = process.new_name("nats-jetstream-test")
  let conn = process.named_subject(name)

  let assert Ok(_) = nats.start(name, options)
  assert test_utils.await_connected(conn, 5)

  let config =
    stream.StreamConfig(
      name: "DUP_TEST",
      subjects: ["dup.>"],
      retention: stream.Limits,
      max_consumers: -1,
      max_msgs: -1,
      max_bytes: -1,
      max_age: duration.milliseconds(0),
      max_msgs_per_subject: -1,
      max_msg_size: -1,
      discard: stream.Old,
      storage: stream.Memory,
      num_replicas: 1,
      duplicate_window: duration.milliseconds(120_000),
      description: None,
    )

  let assert Ok(response) = jetstream.create_stream(conn, config)
  response.config.name |> should.equal("DUP_TEST")

  let assert Ok(dup) = jetstream.create_stream(conn, config)
  dup.config.name |> should.equal("DUP_TEST")
}

pub fn stream_info_not_found_error_test() {
  use port <- test_utils.with_nats_server()
  let options = nats.new("127.0.0.1", port)
  let name = process.new_name("nats-jetstream-test")
  let conn = process.named_subject(name)

  let assert Ok(_) = nats.start(name, options)
  assert test_utils.await_connected(conn, 5)

  let assert Error(_) = jetstream.stream_info(conn, "DOES_NOT_EXIST")
}

pub fn update_stream_not_found_error_test() {
  use port <- test_utils.with_nats_server()
  let options = nats.new("127.0.0.1", port)
  let name = process.new_name("nats-jetstream-test")
  let conn = process.named_subject(name)

  let assert Ok(_) = nats.start(name, options)
  assert test_utils.await_connected(conn, 5)

  let config =
    stream.StreamConfig(
      name: "NONEXISTENT",
      subjects: ["nonexistent.>"],
      retention: stream.Limits,
      max_consumers: -1,
      max_msgs: 100,
      max_bytes: -1,
      max_age: duration.milliseconds(0),
      max_msgs_per_subject: -1,
      max_msg_size: -1,
      discard: stream.Old,
      storage: stream.Memory,
      num_replicas: 1,
      duplicate_window: duration.milliseconds(120_000),
      description: None,
    )

  let assert Error(_) = jetstream.update_stream(conn, config)
}

pub fn purge_stream_not_found_error_test() {
  use port <- test_utils.with_nats_server()
  let options = nats.new("127.0.0.1", port)
  let name = process.new_name("nats-jetstream-test")
  let conn = process.named_subject(name)

  let assert Ok(_) = nats.start(name, options)
  assert test_utils.await_connected(conn, 5)

  let assert Error(_) = jetstream.purge_stream(conn, "DOES_NOT_EXIST")
}

pub fn publish_test() {
  use port <- test_utils.with_nats_server()
  let options = nats.new("127.0.0.1", port)
  let name = process.new_name("nats-jetstream-pub-test")
  let conn = process.named_subject(name)

  let assert Ok(_) = nats.start(name, options)
  assert test_utils.await_connected(conn, 5)

  let config =
    stream.StreamConfig(
      name: "PUB_TEST",
      subjects: ["pub.test.>"],
      retention: stream.Limits,
      max_consumers: -1,
      max_msgs: -1,
      max_bytes: -1,
      max_age: duration.milliseconds(0),
      max_msgs_per_subject: -1,
      max_msg_size: -1,
      discard: stream.Old,
      storage: stream.Memory,
      num_replicas: 1,
      duplicate_window: duration.milliseconds(120_000),
      description: None,
    )

  let assert Ok(_) = jetstream.create_stream(conn, config)

  let assert Ok(ack) =
    jetstream.publish(
      conn,
      subject: "pub.test.hello",
      headers: [],
      payload: bit_array.from_string("hello world"),
      timeout: 5000,
    )

  ack.stream |> should.equal("PUB_TEST")
  should.be_true(ack.seq > 0)
  ack.domain |> should.equal(None)
  ack.duplicate |> should.equal(False)
}
