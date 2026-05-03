import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/io
import gleam/option.{None, Some}
import gleam/otp/actor
import gleam_community/ansi
import nuts.{type Message} as nats
import nuts/internal/stream_api
import nuts/jetstream
import nuts/test_utils

pub fn consumer_test() {
  let assert Ok(actor.Started(_, conn)) =
    nats.new("127.0.0.1", 6789)
    |> nats.with_logger(
      nats.Logger(
        info: fn(line) { io.println_error(ansi.cyan(line)) },
        debug: fn(line) { io.println_error(ansi.cyan(line)) },
        warning: fn(line) { io.println_error(ansi.cyan(line)) },
      ),
    )
    |> nats.start()

  assert test_utils.await_connected(conn, 100)
  let nats_conn = conn
  let conn = jetstream.new_context(conn)

  let assert Ok(_) = jetstream.list_stream_names(conn)

  let stream_config =
    stream_api.StreamCreateRequest(
      stream_name: "my_stream",
      description: Some("hello world"),
      subjects: ["bar.*"],
      retention: stream_api.Limits,
      max_consumers: -1,
      max_msgs: -1,
      max_bytes: -1,
      max_age: 0,
      storage: stream_api.Memory,
      num_replicas: 1,
      discard_policy: stream_api.DiscardOld,
    )
  let assert Ok(_) = jetstream.create_stream(conn, stream_config)
  let assert Ok(_) = jetstream.create_stream(conn, stream_config)

  let consumer_name = "my_stream_consumer"
  let assert Ok(_) =
    jetstream.create_consumer(
      conn
        |> jetstream.with_logger(fn(line) { io.println_error(ansi.cyan(line)) }),
      description: None,
      stream: "my_stream",
      consumer_name:,
      deliver_policy: stream_api.All,
      ack_policy: stream_api.AckExplicit,
      replay_policy: stream_api.Instant,
    )

  let assert Ok(_) = start_consumer(nats_conn, "my_stream", consumer_name)

  process.sleep(2000)

  let assert Error(jetstream.StreamAlreadyExistsWithDifferentConfig) =
    jetstream.create_stream(
      conn,
      stream_api.StreamCreateRequest(
        ..stream_config,
        description: Some("this is another description"),
      ),
    )

  let assert Ok(_) = jetstream.get_info(conn, "my_stream")

  let assert Ok(_) = jetstream.delete_stream(conn, "my_stream")

  let assert Error(jetstream.StreamNotFound) =
    jetstream.delete_stream(conn, "my_stream")

  let assert Error(jetstream.StreamNotFound) =
    jetstream.get_info(conn, "my_stream")
}

fn start_consumer(
  conn: Subject(Message),
  stream_name: String,
  consumer_name: String,
) {
  actor.new_with_initialiser(1000, fn(self) {
    let inbox = "consumer" <> int.random(382_982) |> int.to_base36
    let assert Ok(subscription) = nats.subscribe(conn, inbox)

    let _ =
      nats.new_message(
        "$JS.API.CONSUMER.MSG.NEXT." <> stream_name <> "." <> consumer_name,
        <<>>,
      )
      |> nats.reply_to(Some(inbox))
      |> nats.publish(conn, _)

    actor.initialised(self)
    |> actor.selecting(
      process.new_selector()
      |> process.select(nats.get_subject(subscription)),
    )
    |> Ok
  })
  |> actor.on_message(fn(state, msg) {
    echo msg
    actor.continue(state)
  })
  |> actor.start
}

pub fn main() {
  consumer_test()
}
