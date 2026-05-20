import gleam/erlang/process
import gleam/otp/actor
import guppy
import guppy/kv

pub fn main() {
  let assert Ok(actor.Started(_, conn)) =
    guppy.start(guppy.new("10.0.0.7", 4222))

  process.sleep(1000)
  let kv = kv.new_context(conn)

  case kv.get_entry(kv, "merciless", "foo") {
    Ok(entry) -> {
      echo entry
      Nil
    }
    Error(error) -> {
      echo error
      Nil
    }
  }

  kv.list_keys(kv, "merciless")
  |> echo
}
