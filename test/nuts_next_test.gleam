import gleam/bit_array
import gleam/erlang/process
import gleam/int
import gleam/option.{None, Some}
import gleam/otp/actor.{Started}
import nuts as nats
import nuts/test_utils

pub fn integration_test() {
  use port <- test_utils.with_nats_server()
  let options = nats.new("127.0.0.1", port)
  let assert Ok(Started(_, nats_conn)) = nats.start(options)

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
  let assert Ok(Started(_, conn)) =
    nats.new("127.0.0.1", 47_921)
    |> nats.start()

  assert !test_utils.await_connected(conn, 5) as "should not be connected"

  let assert Ok(sub) = nats.subscribe(conn, "foo")
    as "should be able to subscribe when server is offline"
  let assert Ok(Nil) = nats.unsubscribe(conn, sub)
}

pub fn nkey_authorization_test() {
  let options =
    nats.new("127.0.0.1", 6789)
    |> nats.nkey_seed(
      "SUALHP366GCQN53R7X3MJF4BCNEK6WTKATRZ7QAMDC7UTVBMC2WYUDKK64",
    )

  let assert Ok(Started(_, nats_conn)) = nats.start(options)
  assert test_utils.await_connected(nats_conn, 5) as "not connected to NATS"
}

pub fn user_pass_authorization_test() {
  use port <- test_utils.with_nats_server_with_auth("alice", "secret")
  let options =
    nats.new("127.0.0.1", port)
    |> nats.username("alice")
    |> nats.password("secret")

  let assert Ok(Started(_, nats_conn)) = nats.start(options)
  assert test_utils.await_connected(nats_conn, 5) as "not connected to NATS"
}

pub fn invalid_user_pass_authorization_test() {
  use port <- test_utils.with_nats_server_with_auth("alice", "secret")
  let options =
    nats.new("127.0.0.1", port)
    |> nats.username("alice")
    |> nats.password("wrongpassword")

  let assert Ok(Started(_, nats_conn)) = nats.start(options)
  assert !test_utils.await_connected(nats_conn, 10) as "connected to NATS"
}

pub fn reconnect_and_resubscribe_test() {
  let #(port, subscription, conn) = {
    use port <- test_utils.with_nats_server()
    let assert Ok(Started(_, conn)) =
      nats.new("127.0.0.1", port)
      |> nats.start()

    assert test_utils.await_connected(conn, 5)

    // make a subscription
    let assert Ok(subscription) =
      conn
      |> nats.subscribe("wobble")

    #(port, subscription, conn)
  }
  assert !test_utils.await_connected(conn, 5) as "should not be connected"

  // start the NATS server again
  {
    use <- test_utils.with_nats_server_on_port(port)
    assert test_utils.await_connected(conn, 20)

    let expected_payload =
      int.random(10_000_000) |> int.to_string |> bit_array.from_string
    let assert Ok(_) =
      nats.new_message("wobble", expected_payload)
      |> nats.publish(conn, _)
      as "publish on reconnected server should work"

    let assert Ok(nats.NatsMessage(
      subject: "wobble",
      reply_to: option.None,
      headers: [],
      payload: actual_payload,
    )) = process.receive(nats.get_subject(subscription), 1000)
      as "subscription should be resubscribed"

    assert expected_payload == actual_payload
  }
}

pub fn request_reply_test() {
  use <- test_utils.with_nats_server_on_port(6789)
  let options = nats.new("127.0.0.1", 6789)

  let assert Ok(Started(_, nats_conn)) = nats.start(options)
  assert test_utils.await_connected(nats_conn, 5)

  let echo_ready = process.new_subject()
  process.spawn(fn() {
    let assert Ok(service_sub) = nats.subscribe(nats_conn, "echo")
    let service_subject = nats.get_subject(service_sub)
    process.send(echo_ready, True)
    echo_service(nats_conn, service_subject)
  })

  let assert Ok(True) = process.receive(echo_ready, 2000)
    as "echo service did not start"

  let assert Ok(reply) =
    nats.new_message("echo", <<"hello world">>)
    |> nats.request(nats_conn, _, 1000)
    as "request should succeed"

  assert reply.payload == <<"hello world">>
  assert reply.reply_to == option.None
}

fn echo_service(
  conn: process.Subject(nats.Message),
  messages: process.Subject(nats.NatsMessage),
) {
  case process.receive(messages, 5000) {
    Ok(msg) -> {
      case msg.reply_to {
        Some(inbox) -> {
          let _ =
            nats.new_message(inbox, msg.payload)
            |> nats.publish(conn, _)
          echo_service(conn, messages)
        }
        None -> echo_service(conn, messages)
      }
    }
    Error(Nil) -> echo_service(conn, messages)
  }
}

pub fn request_timeout_test() {
  use port <- test_utils.with_nats_server()
  let options = nats.new("127.0.0.1", port)

  let assert Ok(Started(_, nats_conn)) = nats.start(options)
  assert test_utils.await_connected(nats_conn, 5)

  assert Error(nats.NoResponders)
    == nats.new_message("no-one-is-here", <<"ping">>)
    |> nats.request(nats_conn, _, 200)
    as "request should return no responders"
}

pub fn request_not_connected_test() {
  let assert Ok(Started(_, conn)) =
    nats.new("127.0.0.1", 47_921)
    |> nats.start()

  let assert Error(nats.NotConnected) =
    nats.new_message("foo", <<"bar">>)
    |> nats.request(conn, _, 200)
    as "should not be connected"
}

pub fn named_connection_test() {
  let name = process.new_name("test_name")
  let assert Ok(_) =
    nats.new("127.0.0.1", 6789)
    |> nats.with_name(name)
    |> nats.start()

  let conn = process.named_subject(name)
  assert test_utils.await_connected(conn, 10) as "should be connected"
}

pub fn ping_keeps_connection_alive_test() {
  use port <- test_utils.with_nats_server()
  let options =
    nats.new("127.0.0.1", port)
    |> nats.with_ping_interval(200)
    |> nats.with_ping_timeout(100)

  let assert Ok(Started(_, conn)) = nats.start(options)
  assert test_utils.await_connected(conn, 10)

  process.sleep(3000)

  assert nats.is_connected(conn)
    as "connection should still be alive after ping interval"
}
