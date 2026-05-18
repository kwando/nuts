import gleam/bool
import gleam/erlang/process
import gleam/otp/actor
import guppy as nats

pub fn await_connected(subject: process.Subject(nats.Message), attempts: Int) {
  use <- bool.guard(when: attempts == 0, return: False)
  case nats.is_connected(subject) {
    True -> True
    False -> {
      process.sleep(100)
      await_connected(subject, attempts - 1)
    }
  }
}

pub fn with_client(callback: fn(process.Subject(nats.Message)) -> a) -> a {
  let assert Ok(actor.Started(_, conn)) =
    nats.new("127.0.0.1", 6789)
    |> nats.start()

  await_connected(conn, 20)
  callback(conn)
}
