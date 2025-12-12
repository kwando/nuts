import gleam/erlang/process
import nuts
import service

pub fn main() {
  let assert Ok(conn) =
    nuts.new("127.0.0.1", 4223)
    |> nuts.nkey("SUALHP366GCQN53R7X3MJF4BCNEK6WTKATRZ7QAMDC7UTVBMC2WYUDKK64")
    |> nuts.start()

  service.start(conn, "foo.bar", fn(event) {
    echo event
    Ok(<<"AWESOME">>)
  })

  process.sleep_forever()
}
