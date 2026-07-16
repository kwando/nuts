import gleam/erlang/process
import gleam/io
import gleam/otp/actor
import gleam/string
import gleam_community/ansi
import guppy

pub fn main() {
  let assert Ok(actor.Started(_, conn)) =
    guppy.start(guppy.new("100.121.244.19", 4222))

  let assert Ok(#(me, _)) = guppy.subscribe(conn, "naboo.victron")

  loop(me)
}

fn loop(subject: process.Subject(guppy.NatsMessage)) {
  case process.receive(subject, 1000) {
    Error(_) -> loop(subject)
    Ok(event) -> {
      io.println(
        ansi.green(event.subject) <> " " <> event.payload |> string.inspect,
      )
      loop(subject)
    }
  }
}
