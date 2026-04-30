import gleam/erlang/process
import gleam/option
import nuts as nats
import nuts/test_utils

pub fn request_test() {
  use port <- test_utils.with_nats_server()
  let options = nats.new("127.0.0.1", port)
  let name = process.new_name("nats-test")
  let nats_conn = process.named_subject(name)

  let assert Ok(_) = nats.start(name, options)

  assert test_utils.await_connected(nats_conn, 5)

  let _ = echo_service(nats_conn, "test.echo")

  let assert Ok(msg) =
    nats.request(
      nats_conn,
      subject: "test.echo",
      headers: [],
      payload: <<"HELLO">>,
      timeout: 3000,
    )

  assert msg.payload == <<"HELLO">>
  process.sleep(1000)
}

fn echo_service(nats_conn, subject) {
  let start_subject = process.new_subject()
  process.spawn(fn() {
    let assert Ok(subscription) = nats.subscribe(nats_conn, subject)
    let subject = subscription |> nats.get_subject
    process.send(start_subject, Nil)
    echo_service_loop(nats_conn, subject)
  })
  process.receive(start_subject, 1000)
}

fn echo_service_loop(nats_conn, subject: process.Subject(nats.NatsMessage)) {
  case process.receive(subject, 1000) {
    Ok(msg) -> {
      let _ = case msg.reply_to {
        option.Some(reply_to) -> {
          nats.new_message(reply_to, msg.payload)
          |> nats.publish(nats_conn, _)
        }
        option.None -> echo_service_loop(nats_conn, subject)
      }

      echo_service_loop(nats_conn, subject)
    }
    Error(_) -> echo_service_loop(nats_conn, subject)
  }
}

pub fn main() {
  request_test()
}
