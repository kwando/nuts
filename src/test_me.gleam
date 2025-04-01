import gleam/erlang/process
import gleeunit/should
import nuts_client

pub fn main() {
  let client =
    nuts_client.start(nuts_client.Config("127.0.0.1", 6789))
    |> should.be_ok

  client
  |> nuts_client.subscribe(">", fn(topic, _headers, payload) {
    echo #(topic, payload)
    Ok(Nil)
  })

  process.sleep_forever()
}
