import gleam/bool
import gleam/erlang/process
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
