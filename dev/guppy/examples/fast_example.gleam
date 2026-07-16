import gleam/erlang/process
import gleam/otp/actor
import guppy

pub fn main() {
  let assert Ok(actor.Started(_, conn)) =
    guppy.start(
      guppy.new("100.85.25.88", 4222)
      |> guppy.with_logger(guppy.default_logger("nats")),
    )
  let assert Ok(#(subscription, _)) = guppy.subscribe(conn, "hello")
  let _ = guppy.await_connected(conn, 5000)

  let assert Ok(_) =
    guppy.publish(conn, guppy.new_message("hello", <<"world">>))
  process.receive(subscription, 1000) |> echo
}
