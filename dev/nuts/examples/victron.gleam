import gleam/bit_array
import gleam/erlang/process
import gleam/io
import nuts

pub fn main() {
  let assert Ok(nats) = nuts.start(nuts.new("100.121.244.19", 4222))

  let assert Ok(me) = nuts.subscribe(nats, "naboo.victron")
  loop(me)
}

fn loop(subject: process.Subject(nuts.Event)) -> a {
  case process.receive(subject, 1000) {
    Error(_) -> loop(subject)
    Ok(evt) -> {
      let assert Ok(msg) = bit_array.to_string(evt.payload)
      io.println(evt.topic <> ": " <> msg)
      loop(subject)
    }
  }
}
