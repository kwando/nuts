import gleam/bit_array
import gleam/erlang/process
import gleam/io
import gleam/result
import gleam/string
import gleam_community/ansi
import nuts

pub fn main() {
  let name = process.new_name("local-nats")
  let assert Ok(_started) = nuts.start(name, nuts.new("127.0.0.1", 4222))
  let nats = process.named_subject(name)

  let assert Ok(me) = nuts.subscribe(nats, ">")

  loop(me |> nuts.get_subject)
}

fn loop(subject: process.Subject(nuts.NatsMessage)) {
  case process.receive(subject, 1000) {
    Error(_) -> loop(subject)
    Ok(event) -> {
      io.println(
        ansi.green(event.subject)
        <> " "
        <> event.payload
        |> bit_array.to_string
        |> result.lazy_unwrap(fn() {
          string.inspect(event.reply_to)
          |> ansi.yellow
        }),
      )
      loop(subject)
    }
  }
}
