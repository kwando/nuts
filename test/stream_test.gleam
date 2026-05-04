import gleam/bit_array
import gleam/bool
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/io
import gleam/json
import gleam/list
import gleam/option.{None, Some}
import gleam/otp/actor
import gleam/result
import gleam/string
import gleam_community/ansi
import nuts.{type Message} as nats
import nuts/internal/jetstream_api
import nuts/jetstream
import nuts/test_utils

pub fn consumer_test() {
  let assert Ok(actor.Started(_, conn)) =
    nats.new("127.0.0.1", 6789)
    //|> nats.with_logger(
    //  nats.Logger(
    //    info: fn(line) { io.println_error(ansi.cyan(line)) },
    //    debug: fn(line) { io.println_error(ansi.cyan(line)) },
    //    warning: fn(line) { io.println_error(ansi.cyan(line)) },
    //  ),
    //)
    |> nats.start()

  assert test_utils.await_connected(conn, 100)
  let nats_conn = conn
  let conn = jetstream.new_context(conn)

  let assert Ok(_) = jetstream.list_stream_names(conn)

  let stream_config =
    jetstream_api.StreamCreateRequest(
      stream_name: "my_stream",
      description: Some("hello world"),
      subjects: ["bar.*"],
      retention: jetstream_api.Limits,
      max_consumers: -1,
      max_msgs: -1,
      max_bytes: -1,
      max_age: 0,
      storage: jetstream_api.Memory,
      num_replicas: 1,
      discard_policy: jetstream_api.DiscardOld,
    )
  let assert Ok(_) = jetstream.create_stream(conn, stream_config)
  let assert Ok(_) = jetstream.create_stream(conn, stream_config)

  let consumer_name = "my_stream_consumer"
  let assert Ok(_) =
    jetstream.create_consumer(
      conn,
      description: None,
      stream: "my_stream",
      consumer_name:,
      deliver_policy: jetstream_api.All,
      ack_policy: jetstream_api.AckExplicit,
      replay_policy: jetstream_api.Instant,
    )

  process.spawn(fn() { producer_loop(nats_conn, "bar.1", 100) })
  process.spawn(fn() { producer_loop(nats_conn, "bar.2", 100) })
  process.spawn(fn() { producer_loop(nats_conn, "bar.3", 100) })
  process.spawn(fn() { producer_loop(nats_conn, "bar.4", 100) })
  process.spawn(fn() { producer_loop(nats_conn, "bar.5", 100) })

  let assert Ok(_) =
    start_consumer(nats_conn, "my_stream", consumer_name, 1000, 500)

  process.sleep(2000)

  let assert Error(jetstream.StreamAlreadyExistsWithDifferentConfig) =
    jetstream.create_stream(
      conn,
      jetstream_api.StreamCreateRequest(
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

fn producer_loop(conn: Subject(Message), subject: String, sleep: Int) {
  nats.publish(conn, nats.new_message(subject, <<"3189131+13">>))
  process.sleep(int.random(sleep))
  producer_loop(conn, subject, sleep)
}

type ConsumerState {
  ConsumerState(
    polls: List(PollRequest),
    next_poll_id: Int,
    self: Subject(ConsumerMessage),
    pending: Int,
  )
}

type PollRequest {
  PollRequest(pending: Int, inbox: String)
}

type ConsumerMessage {
  Poll(batch: Int)
  NatsMessage(nats.NatsMessage)
}

fn start_consumer(
  conn: Subject(Message),
  stream_name: String,
  consumer_name: String,
  max_messages: Int,
  threshold: Int,
) {
  let inbox_prefix = "consumer." <> int.random(382_982) |> int.to_base36 <> "."
  actor.new_with_initialiser(1000, fn(self) {
    let assert Ok(subscription) = nats.subscribe(conn, inbox_prefix <> "*")

    process.send(self, Poll(max_messages))

    actor.initialised(ConsumerState(
      polls: [],
      next_poll_id: 1,
      self:,
      pending: 0,
    ))
    |> actor.selecting(
      process.new_selector()
      |> process.select_map(nats.get_subject(subscription), NatsMessage)
      |> process.select(self),
    )
    |> Ok
  })
  |> actor.on_message(fn(state, msg) {
    case msg {
      Poll(batch) -> {
        use <- bool.guard(
          when: list.length(state.polls) >= 2,
          return: actor.continue(state),
        )
        let inbox = inbox_prefix <> state.next_poll_id |> int.to_base36
        let _ =
          nats.new_message(
            "$JS.API.CONSUMER.MSG.NEXT." <> stream_name <> "." <> consumer_name,
            [
              #("batch", json.int(batch)),
              #("expires", json.int(30 * 1_000_000_000)),
            ]
              |> json.object
              |> json.to_string
              |> bit_array.from_string,
          )
          |> nats.reply_to(Some(inbox))
          |> nats.publish(conn, _)

        actor.continue(
          ConsumerState(
            ..state,
            polls: list.append(state.polls, [
              PollRequest(pending: batch, inbox: inbox),
            ]),
            next_poll_id: state.next_poll_id + 1,
            pending: state.pending + batch,
          ),
        )
      }
      NatsMessage(msg) ->
        case string.starts_with(msg.subject, inbox_prefix) {
          True -> {
            let updated_state = case
              msg.headers |> list.key_find("Nats-Status")
            {
              Ok("408" <> _) -> {
                case
                  msg.headers
                  |> list.key_find("Nats-Pending-Messages")
                  |> result.try(int.parse)
                {
                  Ok(pending_messages) -> {
                    let updated_pending = state.pending - pending_messages

                    process.send(
                      state.self,
                      Poll(max_messages - updated_pending),
                    )

                    ConsumerState(
                      ..state,
                      pending: updated_pending,
                      polls: list.filter(state.polls, fn(poll) {
                        poll.inbox != msg.subject
                      }),
                    )
                  }
                  Error(_) -> state
                }
              }

              Ok(_) | Error(_) -> todo
            }
            //io.println_error(
            //  string.inspect(msg) |> ansi.yellow
            //  <> "\n  "
            //  <> int.to_string(updated_state.pending)
            //  <> " polls="
            //  <> string.inspect(updated_state.polls),
            //)
            actor.continue(updated_state)
          }
          False -> {
            let updated_pending = state.pending - 1
            let updated_polls = case state.polls {
              [] -> []
              [PollRequest(pending: 1, ..), ..rest] -> rest
              [PollRequest(pending: pending, inbox: inbox), ..rest] -> {
                [PollRequest(pending: pending - 1, inbox: inbox), ..rest]
              }
            }

            let _ = case msg.reply_to {
              Some("$JS.ACK." <> _ as reply) -> {
                let assert Ok(_) =
                  nats.publish(conn, nats.new_message(reply, <<>>))
              }
              None | Some(_) -> todo
            }

            //io.println_error(
            //  string.inspect(msg)
            //  <> "\n  "
            //  <> int.to_string(updated_pending)
            //  <> " polls="
            //  <> string.inspect(updated_polls),
            //)
            case updated_pending <= threshold {
              True ->
                actor.send(state.self, Poll(max_messages - updated_pending))
              False -> Nil
            }
            actor.continue(
              ConsumerState(
                ..state,
                pending: updated_pending,
                polls: updated_polls,
              ),
            )
          }
        }
    }
  })
  |> actor.start
}

pub fn main() {
  consumer_test()
}
