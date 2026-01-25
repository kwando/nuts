import gleam/erlang/process
import gleam/list
import gleam/option.{Some}
import nuts

pub fn nuts_test() {
  let assert Ok(started) =
    nuts.new("127.0.0.1", 4222)
    |> nuts.client_name("nuts_test")
    |> nuts.logger(fn(_) { Nil })
    |> nuts.start

  let conn = started.data

  await_connected(conn)

  let assert Ok(Nil) =
    nuts.publish(
      conn,
      nuts.NatsMessage(
        subject: "foo.bar",
        headers: [],
        payload: <<"awesome">>,
        reply_to: option.None,
      ),
    )

  let client_count = 3
  let msgs = process.new_subject()
  list.range(1, client_count)
  |> list.each(fn(_) {
    process.spawn(fn() {
      let assert Ok(sub) = nuts.subscribe(conn, "foo.baz")
      let assert Ok(_msg) = process.receive(sub, 1000)
        as "should receive a message on sub"

      process.send(msgs, Nil)
    })
  })
  process.sleep(10)

  let assert Ok(Nil) =
    nuts.new_message("foo.baz", <<"AWESOME2">>)
    |> nuts.reply_to(Some("hello_world"))
    |> nuts.set_header("encoding", "text/plain")
    |> nuts.publish(conn, _)

  let assert Ok(_) = receive_count(msgs, client_count, 1000)
  nuts.shutdown(conn)
}

fn receive_count(subject, count: Int, timeout: Int) {
  case count {
    0 -> Ok(Nil)
    n -> {
      case process.receive(subject, timeout) {
        Error(_) -> Error("timeout")
        Ok(_) -> {
          receive_count(subject, n - 1, timeout)
        }
      }
    }
  }
}

pub fn nkey_test() {
  let assert Ok(started) =
    nuts.new("127.0.0.1", 6789)
    |> nuts.nkey_seed(
      "SUALHP366GCQN53R7X3MJF4BCNEK6WTKATRZ7QAMDC7UTVBMC2WYUDKK64",
    )
    |> nuts.logger(fn(_) { Nil })
    |> nuts.start()

  let conn = started.data
  await_connected(conn)

  let assert Ok(_) =
    nuts.new_message("test.nkey_test", <<"wibble">>)
    |> nuts.publish(conn, _)
    as "should be published"
}

fn await_connected(conn: process.Subject(nuts.Message)) {
  case nuts.is_connected(conn) {
    True -> Nil
    False -> {
      process.sleep(10)
      await_connected(conn)
    }
  }
}
