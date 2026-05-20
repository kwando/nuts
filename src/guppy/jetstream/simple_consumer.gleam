//// A pull-based JetStream consumer actor that manages message polling
//// and acknowledgment for a NATS JetStream consumer.
////
//// The consumer automatically polls for messages, maintains an in-flight
//// message window, and triggers new polls when the pending count drops
/// below a configurable threshold.
import gleam/bit_array
import gleam/bool
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import gleam/string
import gleam/time/duration.{type Duration}
import guppy.{type Logger, type Message, type NatsMessage} as nats
import guppy/jetstream.{type DeliveryInfo}

type ConsumerState {
  ConsumerState(
    polls: List(PollRequest),
    next_poll_id: Int,
    self: Subject(ConsumerMessage),
    pending: Int,
    handler: fn(NatsMessage, DeliveryInfo) -> HandlerReply,
    logger: Logger,
  )
}

type PollRequest {
  PollRequest(pending: Int, inbox: String)
}

type ConsumerMessage {
  Poll(batch: Int)
  NatsMessage(NatsMessage)
}

/// The possible replies a message handler can return to control how a
/// delivered JetStream message is acknowledged.
pub opaque type HandlerReply {
  /// Acknowledge the message as successfully processed.
  Ack

  /// Negative acknowledgement requesting immediate redelivery.
  Retry

  /// Negative acknowledgement requesting redelivery after the given duration.
  RetryLater(Duration)

  /// Terminate the consumer and stop processing further messages.
  Terminate
}

/// Start a new simple JetStream consumer actor.
///
/// The consumer will subscribe to the NATS connection, automatically poll
/// for messages from the specified stream and consumer, and invoke the
/// provided handler for each received message.
///
/// ## Parameters
///
/// - `conn` - The NATS connection subject used to publish and receive messages.
/// - `stream_name` - The name of the JetStream stream to consume from.
/// - `consumer_name` - The name of the JetStream consumer on the stream.
/// - `max_messages` - The maximum number of messages to keep in flight at once.
/// - `threshold` - When pending messages drop to or below this threshold, a new poll is triggered.
/// - `handler` - A function called for each received message that decides how the message should be acknowledged.
pub fn start(
  conn: Subject(Message),
  stream_name stream_name: String,
  consumer_name consumer_name: String,
  max_messages max_messages: Int,
  threshold threshold: Int,
  handler handler: fn(NatsMessage, DeliveryInfo) -> HandlerReply,
  logger logger: Option(Logger),
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
      logger: option.lazy_unwrap(logger, nats.noop_logger),
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
                state.logger.warning("unhandled status: " <> status)
                state
              }

              Error(_) -> {
                state.logger.warning(
                  "unexpected message: " <> string.inspect(msg),
                )
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
                let assert Ok(info) = jetstream.parse_ack(ack)
                let ack_action = case state.handler(msg, info) {
                  Ack -> jetstream.Ack
                  Retry -> jetstream.Nak
                  RetryLater(delay) -> jetstream.NakWithDelay(delay)
                  Terminate -> jetstream.Term
                }
                let _ =
                  nats.publish(
                    conn,
                    nats.new_message(
                      ack,
                      jetstream.ack_action_to_payload(ack_action),
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

/// Request message redelivery after the given duration.
///
/// ## Parameters
///
/// - `duration` - The delay before the message should be redelivered.
pub fn retry_later(duration: Duration) {
  RetryLater(duration)
}

/// Acknowledge successful message processing.
pub fn ack() {
  Ack
}

/// Request immediate redelivery of the message.
pub fn nak() {
  Retry
}
