import gleam/bool
import gleam/erlang/process
import gleam/otp/actor
import guppy as nats

pub const nats_host = "127.0.0.1"

pub const nats_port = 6789

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
  let event_subject = process.new_subject()
  let assert Ok(actor.Started(_, conn)) =
    nats.new(nats_host, nats_port)
    |> nats.on_connection_event(process.send(event_subject, _))
    |> nats.start()

  let assert Ok(nats.Connected) = process.receive(event_subject, 5000)
    as "timeout waiting for Connected event"

  callback(conn)
}
