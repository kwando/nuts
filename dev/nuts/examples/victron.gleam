import gleam/erlang/process
import gleam/io
import gleam/otp/actor
import gleam/string
import gleam_community/ansi
import nuts

pub fn main() {
  let assert Ok(actor.Started(_, conn)) =
    nuts.start(nuts.new("100.121.244.19", 4222))

  let assert Ok(me) = nuts.subscribe(conn, "naboo.victron")

  loop(me |> nuts.get_subject)
}

fn loop(subject: process.Subject(nuts.NatsMessage)) {
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
