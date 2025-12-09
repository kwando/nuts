import gleam/erlang/process
import gleam/io
import gleam/string
import gleam_community/ansi
import nuts

pub fn main() {
  let assert Ok(nats) = nuts.start(nuts.new("127.0.0.1", 4222))

  let assert Ok(me) = nuts.subscribe(nats, ">")

  loop(me)
}

fn loop(subject: process.Subject(nuts.Event)) -> a {
  case process.receive(subject, 1000) {
    Error(_) -> loop(subject)
    Ok(event) -> {
      io.println(
        ansi.green(event.topic) <> " " <> event.payload |> string.inspect,
      )
      loop(subject)
    }
  }
}
