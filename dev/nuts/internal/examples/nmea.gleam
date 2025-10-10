import gleam/bit_array
import gleam/dict
import gleam/erlang/process
import gleam/io
import gleam/string
import gleam_community/ansi
import nuts

//cog:embed banner.txt
const banner = "\u{47}\u{6C}\u{65}\u{61}\u{6D}\u{20}\u{4E}\u{4D}\u{45}\u{41}\u{20}\u{4E}\u{41}\u{54}\u{53}\u{20}\u{43}\u{6F}\u{6E}\u{6E}\u{65}\u{63}\u{74}\u{6F}\u{72}\u{20}\u{76}\u{31}\u{2E}\u{30}\u{A}"

pub fn main() {
  let assert Ok(nats) = nuts.start(nuts.new("100.121.244.19", 4222))

  let me: process.Subject(#(String, String)) = process.new_subject()
  nuts.subscribe(nats, ">", fn(msg) {
    let assert Ok(payload_str) = bit_array.to_string(msg.payload)
    process.send(me, #(msg.topic, payload_str))
    Ok(Nil)
  })

  loop(dict.new(), me)
}

fn loop(state, subject: process.Subject(#(String, String))) -> a {
  case process.receive(subject, 1000) {
    Error(_) -> loop(state, subject)
    Ok(#(_topic, msg)) -> {
      let assert Ok(#(kind, _data)) = string.split_once(msg, ",")

      let state = dict.insert(state, kind, msg)
      dict.fold(state, "\u{1b}[2J\u{1b}[H" <> banner, fn(acc, _, value) {
        acc <> format_nmea(value) <> "\n"
      })
      |> io.print

      loop(state, subject)
    }
  }
}

fn format_nmea(msg) {
  case msg {
    "$IIMWV" <> _ -> ansi.yellow(msg)
    "$GP" <> _ -> ansi.green(msg)
    "$II" <> _ -> ansi.blue(msg)
    "$VWVHW" <> _ -> ansi.magenta(msg)
    _ -> ansi.bg_red(msg)
  }
}
