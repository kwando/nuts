import gleam/erlang/process
import gleam/option.{Some}
import nuts/test_utils
import nuts_next as nats

pub fn main_test() {
  use port <- test_utils.with_nats_server()
  let options = nats.new("127.0.0.1", port)
  let name = process.new_name("nats-test")
  let nats_conn = process.named_subject(name)

  let assert Ok(_) = nats.start(name, options)
  assert test_utils.await_connected(nats_conn, 5)

  let assert Ok(_) =
    nats.new_message("foo", <<"foo">>)
    |> nats.publish(nats_conn, _)

  let assert Ok(message_subject) = nats.subscribe(nats_conn, "foo")
  let assert Ok(message_subject2) = nats.subscribe(nats_conn, "foo")

  let assert Ok(_) =
    nats.new_message("foo", <<"bar">>)
    |> nats.publish(nats_conn, _)
  let assert Ok(_) =
    nats.new_message("foo", <<"baz">>)
    |> nats.add_header("content-type", "text/plain")
    |> nats.reply_to(Some("INBOX-2387131"))
    |> nats.publish(nats_conn, _)

  let assert Ok(_) = process.receive(message_subject |> nats.get_subject, 1000)
    as "message not received"
  let assert Ok(_) = process.receive(message_subject |> nats.get_subject, 1000)
    as "message not received"
  let assert Ok(_) = process.receive(message_subject2 |> nats.get_subject, 1000)
    as "message not received"
  let assert Ok(_) = process.receive(message_subject2 |> nats.get_subject, 1000)
    as "message not received"
}

pub fn bad_server_test() {
  let name = process.new_name("test")
  let assert Ok(_) =
    nats.new("127.0.0.1", 47_921)
    |> nats.start(name, _)

  let conn = process.named_subject(name)
  assert !test_utils.await_connected(conn, 5) as "should not be connected"

  let assert Ok(sub) = nats.subscribe(conn, "foo")
  let assert Ok(Nil) = nats.unsubscribe(conn, sub)
}

pub fn reconnect_test() {
  let #(port, subscription, conn) = {
    use port <- test_utils.with_nats_server()
    let name = process.new_name("adsa")

    let assert Ok(_) =
      nats.new("127.0.0.1", port)
      |> nats.start(name, _)

    let conn = process.named_subject(name)
    assert test_utils.await_connected(conn, 5)

    let assert Ok(subscription) =
      conn
      |> nats.subscribe("wobble")

    #(port, subscription, conn)
  }
  process.sleep(1000)

  {
    use port <- test_utils.with_nats_server_on_port(port)

    let assert Ok(_) =
      nats.new_message("wibble", <<"wobble">>)
      |> nats.publish(conn, _)
      as "publish should work"
  }
}

pub fn main() {
  //main_test()
  //bad_server_test()
  reconnect_test()
}
