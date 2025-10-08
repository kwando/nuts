import gleam/bit_array
import gleam/erlang/process
import gleam/io
import gleam_community/ansi
import nuts

pub fn main() {
  let assert Ok(nats) = nuts.start(nuts.new("127.0.0.1", 4222))

  let me: process.Subject(#(String, String)) = process.new_subject()
  nuts.subscribe(nats, ">", fn(msg) {
    let assert Ok(payload_str) = bit_array.to_string(msg.payload)
    process.send(me, #(msg.topic, payload_str))
    Ok(Nil)
  })

  loop(me)
}

fn loop(subject: process.Subject(#(String, String))) -> a {
  case process.receive(subject, 1000) {
    Error(_) -> loop(subject)
    Ok(#(topic, msg)) -> {
      io.println(ansi.green(topic) <> " " <> msg)
      loop(subject)
    }
  }
}
