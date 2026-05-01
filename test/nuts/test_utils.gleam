import child_process
import child_process/stdio
import exception
import gleam/bool
import gleam/erlang/process
import gleam/int
import gleam/string
import nuts as nats
import simplifile

pub fn with_nats_server_on_port(port: Int, callback: fn() -> a) -> a {
  let self = process.new_subject()
  let assert Ok(server) =
    child_process.from_file("/opt/homebrew/bin/nats-server")
    |> child_process.arg2("--port", int.to_string(port))
    |> child_process.arg("--jetstream")
    |> child_process.arg2("--store_dir", "/tmp/jetstream")
    |> child_process.arg("-VV")
    |> child_process.spawn(
      stdio.lines(fn(line) {
        case string.contains(line, "Listening for client connections") {
          False -> Nil
          True -> process.send(self, True)
        }
        Nil
      }),
    )

  let assert Ok(_) = process.receive(self, 2000)
  exception.defer(
    fn() {
      child_process.stop(server)
      child_process.kill(server)
      let _ = simplifile.delete("/tmp/jetstream")
    },
    callback,
  )
}

pub fn with_nats_server(callback: fn(Int) -> a) -> a {
  let port = int.random(5000) + 30_000
  with_nats_server_on_port(port, fn() { callback(port) })
}

pub type Context {
  Context(
    port: Int,
    conn: process.Subject(nats.Message),
    name: process.Name(nats.Message),
  )
}

pub fn with_context(next) {
  use port <- with_nats_server()
  let name = process.new_name("flush-test")

  let options = nats.new("127.0.0.1", port)
  let conn = process.named_subject(name)

  let assert Ok(_) = nats.start(name, options)
  assert await_connected(conn, 5)

  next(Context(port, conn, name))
}

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
