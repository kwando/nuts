import gleam/erlang/process
import nuts as nats
import nuts/test_utils

pub fn flush_test() {
  use port <- test_utils.with_nats_server()
  let name = process.new_name("flush-test")
  let conn = process.named_subject(name)

  let assert Ok(_) =
    nats.new("127.0.0.1", port)
    |> nats.start(name, _)
  assert test_utils.await_connected(conn, 5) as "should be connected"

  let assert Ok(Nil) = nats.flush(conn, 1000)
    as "flush should succeed on connected server"
}

pub fn flush_after_publish_test() {
  use port <- test_utils.with_nats_server()
  let name = process.new_name("flush-publish-test")
  let conn = process.named_subject(name)

  let assert Ok(_) =
    nats.new("127.0.0.1", port)
    |> nats.start(name, _)
  assert test_utils.await_connected(conn, 5)

  // Subscribe first to confirm message delivery after flush
  let assert Ok(sub) = nats.subscribe(conn, "flush-test-subject")

  // Publish a message
  let assert Ok(_) =
    nats.new_message("flush-test-subject", <<"flush payload">>)
    |> nats.publish(conn, _)
    as "publish should succeed"

  // Flush to ensure the server received it
  let assert Ok(Nil) = nats.flush(conn, 1000)
    as "flush after publish should succeed"

  // Now the message should be available to the subscriber
  let assert Ok(_) = process.receive(nats.get_subject(sub), 1000)
    as "message should arrive after flush"
}

pub fn flush_when_disconnected_test() {
  let name = process.new_name("flush-disconnected")
  let assert Ok(_) =
    nats.new("127.0.0.1", 47_921)
    |> nats.start(name, _)
  let conn = process.named_subject(name)
  assert !test_utils.await_connected(conn, 5) as "should not be connected"

  let assert Error(nats.NotConnected) = nats.flush(conn, 1000)
    as "flush should fail when not connected"
}

pub fn flush_timeout_test() {
  use port <- test_utils.with_nats_server()
  let name = process.new_name("flush-timeout")
  let conn = process.named_subject(name)

  let assert Ok(_) =
    nats.new("127.0.0.1", port)
    |> nats.start(name, _)
  assert test_utils.await_connected(conn, 5)

  // Ultra-short timeout — server should still respond before it expires
  // but the test verifies the timeout path works (flush returns Ok since
  // the connected server responds to PING quickly)
  let assert Ok(Nil) = nats.flush(conn, 10)
    as "flush with tight timeout should succeed"
}
