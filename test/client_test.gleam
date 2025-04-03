import gleam/erlang/process
import gleeunit/should
import nuts

pub fn client_test() {
  let assert Ok(nats) = nuts.new("127.0.0.1", 6789) |> nuts.start()
  nats
  |> nuts.is_connected()
  |> should.equal(True)

  nats
  |> nuts.publish_bits("foo", <<"hello">>)
  |> should.be_ok

  let me = process.new_subject()
  nats
  |> nuts.subscribe(">", fn(topic, headers, payload) {
    process.send(me, #(topic, headers, payload))
    Ok(Nil)
  })

  nats
  |> nuts.publish_bits("foo", <<"world">>)
  |> should.be_ok

  process.receive(me, 500)
  |> should.be_ok
  |> should.equal(#("foo", [], <<"world">>))

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
  |> nuts.subscribe("foo", fn(_msg, _headers, _body) { Ok(Nil) })

  nats
  |> nuts.publish_bits("foo", <<"hello">>)
  |> should.be_error

  nats
  |> nuts.shutdown()
}
