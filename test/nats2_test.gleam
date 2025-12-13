import gleam/erlang/process
import gleam/list
import gleam/option.{Some}
import nuts2 as nats

pub fn nats_test() {
  let assert Ok(started) =
    nats.new("127.0.0.1", 4222)
    |> nats.client_name("nats_test")
    |> nats.start

  let conn = started.data

  process.sleep(100)

  let assert Ok(Nil) =
    nats.publish(
      conn,
      nats.NatsMessage(
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
      let assert Ok(sub) = nats.subscribe(conn, "foo.baz")
      let assert Ok(_msg) = process.receive(sub, 1000)
        as "should receive a message on sub"

      process.send(msgs, Nil)
    })
  })
  process.sleep(10)

  let assert Ok(Nil) =
    nats.new_message("foo.baz", <<"AWESOME2">>)
    |> nats.reply_to(Some("hello_world"))
    |> nats.set_header("encoding", "text/plain")
    |> nats.publish(conn, _)

  let assert Ok(_) = receive_count(msgs, client_count, 1000)
  nats.shutdown(conn)
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
