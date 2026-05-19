import gleam/erlang/process
import gleam/otp/actor.{Started}
import guppy as nats
import guppy/test_utils

pub fn drain_subscription_test() {
  let event_subject = process.new_subject()
  let assert Ok(Started(_, conn)) =
    nats.new(test_utils.nats_host, test_utils.nats_port)
    |> nats.on_connection_event(process.send(event_subject, _))
    |> nats.start()

  let assert Ok(nats.Connected) = process.receive(event_subject, 5000)
    as "should be connected"

  let assert Ok(sub) = nats.subscribe(conn, "drain.test")
  let subject = nats.get_subject(sub)

  let assert Ok(_) =
    nats.new_message("drain.test", <<"first">>)
    |> nats.publish(conn, _)

  let assert Ok(msg) = process.receive(subject, 1000)
    as "first message not received"
  assert msg.payload == <<"first">>

  let assert Ok(Nil) = nats.drain_subscription(conn, sub, 500)
    as "drain should succeed"

  let assert Ok(_) =
    nats.new_message("drain.test", <<"second">>)
    |> nats.publish(conn, _)

  let assert Error(Nil) = process.receive(subject, 500)
    as "should not receive message after drain"
}

pub fn drain_connection_test() {
  let event_subject = process.new_subject()
  let assert Ok(Started(_, conn)) =
    nats.new(test_utils.nats_host, test_utils.nats_port)
    |> nats.on_connection_event(process.send(event_subject, _))
    |> nats.start()

  let assert Ok(nats.Connected) = process.receive(event_subject, 5000)
    as "should be connected"

  let assert Ok(sub) = nats.subscribe(conn, "drain.conn")
  let subject = nats.get_subject(sub)

  let assert Ok(_) =
    nats.new_message("drain.conn", <<"hello">>)
    |> nats.publish(conn, _)

  let assert Ok(msg) = process.receive(subject, 1000)
    as "message not received before drain"
  assert msg.payload == <<"hello">>

  let assert Ok(Nil) = nats.drain_connection(conn, 500)
    as "drain connection should succeed"

  let assert Ok(nats.Closed) = process.receive(event_subject, 2000)
    as "should receive Closed event after drain"

  assert !nats.is_connected(conn) as "should not be connected after drain"
}

pub fn drain_not_connected_test() {
  let assert Ok(Started(_, conn)) =
    nats.new("127.0.0.1", 47_921)
    |> nats.start()

  let assert Error(nats.NotConnected) = nats.drain_connection(conn, 500)
    as "should fail when not connected"
}
