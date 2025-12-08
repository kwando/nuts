import gleam/erlang/process
import gleeunit/should
import nuts

pub fn client_test() {
  let assert Ok(nats) =
    nuts.new("127.0.0.1", 6789)
    |> nuts.nkey("SUALHP366GCQN53R7X3MJF4BCNEK6WTKATRZ7QAMDC7UTVBMC2WYUDKK64")
    |> nuts.start()

  assert nats
    |> nuts.is_connected()
    as "should be connected"

  let assert Ok(_) =
    nats
    |> nuts.publish_bits("foo", <<"hello">>)

  let me = process.new_subject()
  nats
  |> nuts.subscribe(">", fn(event) {
    process.send(me, event)
    Ok(Nil)
  })

  let assert Ok(_) =
    nats
    |> nuts.publish_bits("foo", <<"world">>)
    as "publish should work"

  assert Ok(nuts.Message("foo", [], <<"world">>)) == process.receive(me, 500)

  nats
  |> nuts.shutdown()
}

pub fn disconnected_client_test() {
  let assert Ok(nats) = nuts.start(nuts.new("127.0.0.1", 24_823))

  nats
  |> nuts.is_connected()
  |> should.equal(False)

  nats
  |> nuts.publish_bits("foo", <<"hello">>)
  |> should.be_error

  nats
  |> nuts.subscribe("foo", fn(_event) { Ok(Nil) })

  nats
  |> nuts.publish_bits("foo", <<"hello">>)
  |> should.be_error

  nats
  |> nuts.shutdown()
}
