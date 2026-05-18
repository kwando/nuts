import gleam/erlang/process
import gleam/otp/actor.{Started}
import guppy as nats
import guppy/test_utils

pub fn connected_closed_event_test() {
  let event_subject = process.new_subject()
  let options =
    nats.new("127.0.0.1", 6789)
    |> nats.on_connection_event(event_subject)

  let assert Ok(Started(_, conn)) = nats.start(options)
  assert test_utils.await_connected(conn, 10)

  let assert Ok(nats.Connected) = process.receive(event_subject, 1000)
    as "should receive Connected event"

  nats.shutdown(conn)

  let assert Ok(nats.Closed) = process.receive(event_subject, 1000)
    as "should receive Closed event"
}
