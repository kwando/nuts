import gleam/erlang/process
import gleam/io
import gleam/otp/actor
import gleam/string
import gleam_community/ansi
import guppy

pub fn main() {
  let assert Ok(actor.Started(_, nats)) =
    guppy.start(
      guppy.new("127.0.0.1", 6789)
      |> guppy.with_logger(guppy.Logger(
        info: io.println_error,
        debug: io.println_error,
        warning: io.println_error,
      ))
      |> guppy.with_ping_interval(30_000)
      |> guppy.with_ping_timeout(10_000),
    )

  let assert Ok(me) = guppy.subscribe(nats, ">")

  loop(me |> guppy.get_subject)
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
