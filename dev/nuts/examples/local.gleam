import gleam/erlang/process
import gleam/io
import gleam/otp/actor
import gleam/string
import gleam_community/ansi
import nuts

pub fn main() {
  let assert Ok(actor.Started(_, nats)) =
    nuts.start(
      nuts.new("127.0.0.1", 6789)
      |> nuts.with_logger(nuts.Logger(
        info: io.println_error,
        debug: io.println_error,
        warning: io.println_error,
      ))
      |> nuts.with_ping_interval(30_000)
      |> nuts.with_ping_timeout(10_000),
    )

  let assert Ok(me) = nuts.subscribe(nats, ">")

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
