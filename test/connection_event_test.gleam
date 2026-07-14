import gleam/erlang/process
import gleam/otp/actor.{Started}
import guppy as nats
import guppy/test_utils

pub fn connected_event_test() {
  use conn <- test_utils.with_client()
  assert nats.is_connected(conn)
}

pub fn closed_event_test() {
  let event_subject = process.new_subject()
  let options =
    nats.new(test_utils.nats_host, test_utils.nats_port)
    |> nats.on_connection_event(process.send(event_subject, _))

  let assert Ok(Started(_, conn)) = nats.start(options)
  let assert Ok(_) = nats.await_connected(conn, 1000)

  let assert Ok(nats.Connected) = process.receive(event_subject, 1000)
    as "should receive Connected event"

  nats.shutdown(conn)

  let assert Ok(nats.Closed) = process.receive(event_subject, 2000)
    as "should receive Closed event"
}
