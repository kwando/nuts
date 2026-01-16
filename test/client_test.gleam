import gleam/erlang/process
import gleam/option.{None}
import gleeunit/should
import nuts

pub fn client_test() {
  let assert Ok(started) =
    nuts.new("127.0.0.1", 6789)
    |> nuts.nkey_seed(
      "SUALHP366GCQN53R7X3MJF4BCNEK6WTKATRZ7QAMDC7UTVBMC2WYUDKK64",
    )
    |> nuts.start()

  let nats = started.data

  await_connected(nats)

  let assert Ok(Nil) =
    nats
    |> nuts.publish(nuts.new_message("foo", <<"hello">>))

  let assert Ok(me) =
    nats
    |> nuts.subscribe(">")

  let assert Ok(Nil) =
    nats
    |> nuts.publish(nuts.new_message("foo", <<"world">>))
    as "publish should work"

  let assert Ok(nuts.ReceivedMessage(
    _,
    _,
    nuts.NatsMessage("foo", [], <<"world">>, None),
  )) = process.receive(me, 500)

  nats
  |> nuts.shutdown()
}

pub fn disconnected_client_test() {
  let assert Ok(started) =
    nuts.new("127.0.0.1", 24_823)
    |> nuts.start()

  let nats = started.data

  nats
  |> nuts.is_connected()
  |> should.equal(False)

  let assert Error(nuts.NotConnected) =
    nats
    |> nuts.publish(nuts.new_message("foo", <<"hello">>))

  let assert Ok(_) =
    nats
    |> nuts.subscribe("foo")

  nats
  |> nuts.shutdown()
}

fn await_connected(conn: process.Subject(nuts.Message)) {
  case nuts.is_connected(conn) {
    True -> Nil
    False -> {
      process.sleep(10)
      await_connected(conn)
    }
  }
}
