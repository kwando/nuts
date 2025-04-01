import gleam/erlang/process
import gleeunit/should
import nuts_client

pub fn client_test() {
  let assert Ok(nats) = nuts_client.start(nuts_client.Config("127.0.0.1", 6789))

  nats
  |> nuts_client.publish_bits("foo", <<"hello">>)

  let me = process.new_subject()
  nats
  |> nuts_client.subscribe(">", fn(topic, headers, payload) {
    process.send(me, #(topic, headers, payload))
    Ok(Nil)
  })

  nats
  |> nuts_client.publish_bits("foo", <<"world">>)

  process.receive(me, 500)
  |> should.be_ok
  |> should.equal(#("foo", [], <<"world">>))

  nats
  |> nuts_client.shutdown()
}
