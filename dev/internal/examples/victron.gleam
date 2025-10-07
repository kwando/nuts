import gleam/bit_array
import gleam/erlang/process
import gleam/io
import nuts

pub fn main() {
  let assert Ok(nats) = nuts.start(nuts.new("100.121.244.19", 4222))

  let me: process.Subject(#(String, String)) = process.new_subject()
  nuts.subscribe(nats, "naboo.victron", fn(msg) {
    let assert Ok(payload_str) = bit_array.to_string(msg.payload)
    process.send(me, #(msg.topic, payload_str))
    Ok(Nil)
  })

  loop(me)
}

fn loop(subject: process.Subject(#(String, String))) -> a {
  case process.receive(subject, 1000) {
    Error(_) -> loop(subject)
    Ok(#(_topic, msg)) -> {
      io.println(msg)
      loop(subject)
    }
  }
}
