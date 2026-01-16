import gleam/erlang/process
import nuts
import service

pub fn service_test() {
  let assert Ok(started) =
    nuts.new("127.0.0.1", 4223)
    |> nuts.nkey_seed(
      "SUALHP366GCQN53R7X3MJF4BCNEK6WTKATRZ7QAMDC7UTVBMC2WYUDKK64",
    )
    |> nuts.start()

  let conn = started.data

  let pid =
    service.start(conn, "foo.bar", fn(event) {
      echo event
      Ok(<<"AWESOME">>)
    })

  assert process.is_alive(pid)
}
