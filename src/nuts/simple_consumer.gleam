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
import gleam/time/duration.{type Duration}
import nuts.{type Message} as nats
import nuts/internal/jetstream_api.{type DeliveryInfo}

type ConsumerState {
  ConsumerState(
    polls: List(PollRequest),
    next_poll_id: Int,
    self: Subject(ConsumerMessage),
    pending: Int,
    handler: fn(nats.NatsMessage, DeliveryInfo) -> HandlerReply,
  )
}

type PollRequest {
  PollRequest(pending: Int, inbox: String)
}

type ConsumerMessage {
  Poll(batch: Int)
  NatsMessage(nats.NatsMessage)
}

pub opaque type HandlerReply {
  Ack
  Retry
  RetryLater(Duration)
  Terminate
}

pub fn start(
  conn: Subject(Message),
  stream_name: String,
  consumer_name: String,
  max_messages: Int,
  threshold: Int,
  handler: fn(nats.NatsMessage, DeliveryInfo) -> HandlerReply,
) -> Result(actor.Started(Nil), actor.StartError) {
  let inbox_prefix = "consumer." <> nats.random_string(10) <> "."
  actor.new_with_initialiser(1000, fn(self) {
    let assert Ok(subscription) = nats.subscribe(conn, inbox_prefix <> "*")

    process.send(self, Poll(max_messages))

    actor.initialised(ConsumerState(
      polls: [],
      next_poll_id: 1,
      self:,
      pending: 0,
      handler:,
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
              Ok(status) -> {
                io.println_error("unhandled status: " <> status)
                state
              }

              Error(_) -> {
                io.println_error("unexpected message: " <> string.inspect(msg))
                state
              }
            }

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

            // send publish
            let _ = case msg.reply_to {
              Some(ack) -> {
                let assert Ok(info) = jetstream_api.parse_ack(ack)
                let ack_action = case state.handler(msg, info) {
                  Ack -> jetstream_api.Ack
                  Retry -> jetstream_api.Nak
                  RetryLater(delay) -> jetstream_api.NakWithDelay(delay)
                  Terminate -> jetstream_api.Term
                }
                let _ =
                  nats.publish(
                    conn,
                    nats.new_message(
                      ack,
                      jetstream_api.ack_action_to_payload(ack_action),
                    ),
                  )
              }
              None -> Ok(Nil)
            }

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

pub fn retry_later(duration: Duration) {
  RetryLater(duration)
}

pub fn ack() {
  Ack
}

pub fn nak() {
  Retry
}
