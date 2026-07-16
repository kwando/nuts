//// A pull-based JetStream consumer actor that manages message polling
//// and acknowledgment for a NATS JetStream consumer.
////
//// The consumer automatically polls for messages, maintains an in-flight
//// message window, and triggers new polls when the pending count drops
//// below a configurable threshold.

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
import guppy.{type Logger, type Message, type NatsMessage} as nats
import guppy/jetstream.{type DeliveryInfo}

type ConsumerState(acc) {
  ConsumerState(
    polls: List(PollRequest),
    next_poll_id: Int,
    self: Subject(ConsumerMessage),
    acc: acc,
    pending: Int,
    handler: fn(acc, NatsMessage, DeliveryInfo) -> HandlerReply(acc),
    logger: Logger,
    stream_name: String,
    inbox_prefix: String,
    consumer_name: String,
    threshold: Int,
    max_messages: Int,
    conn: Subject(Message),
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
pub opaque type HandlerReply(acc) {
  Continue(jetstream.AckAction, acc)
  Stop(jetstream.AckAction)
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
  initial acc: acc,
  handler handler: fn(acc, NatsMessage, DeliveryInfo) -> HandlerReply(acc),
  logger logger: Option(Logger),
) -> Result(actor.Started(Nil), actor.StartError) {
  let inbox_prefix = "consumer." <> nats.random_string(10) <> "."
  actor.new_with_initialiser(1000, fn(self) {
    let assert Ok(#(subject, _)) = nats.subscribe(conn, inbox_prefix <> "*")

    process.send(self, Poll(max_messages))

    actor.initialised(ConsumerState(
      polls: [],
      next_poll_id: 1,
      self:,
      pending: 0,
      handler:,
      acc:,
      stream_name:,
      inbox_prefix:,
      consumer_name:,
      logger: option.lazy_unwrap(logger, nats.noop_logger),
      threshold:,
      max_messages:,
      conn:,
    ))
    |> actor.selecting(
      process.new_selector()
      |> process.select_map(subject, NatsMessage)
      |> process.select(self),
    )
    |> Ok
  })
  |> actor.on_message(handle_message)
  |> actor.start
}

fn handle_message(state: ConsumerState(acc), msg: ConsumerMessage) {
  case msg {
    Poll(batch) -> {
      use <- bool.guard(
        when: list.length(state.polls) >= 2,
        return: actor.continue(state),
      )
      let inbox = state.inbox_prefix <> state.next_poll_id |> int.to_base36
      let _ =
        nats.new_message(
          "$JS.API.CONSUMER.MSG.NEXT."
            <> state.stream_name
            <> "."
            <> state.consumer_name,
          [
            #("batch", json.int(batch)),
            #("expires", json.int(30 * 1_000_000_000)),
          ]
            |> json.object
            |> json.to_string
            |> bit_array.from_string,
        )
        |> nats.reply_to(Some(inbox))
        |> nats.publish(state.conn, _)

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
      case string.starts_with(msg.subject, state.inbox_prefix) {
        True -> {
          let updated_state = case msg.headers |> list.key_find("Nats-Status") {
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
                    Poll(state.max_messages - updated_pending),
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
          case msg.reply_to {
            Some(ack_subject) -> {
              let assert Ok(info) = jetstream.parse_ack(ack_subject)

              let outcome = state.handler(state.acc, msg, info)
              case outcome {
                Continue(ack_action, updated_acc) -> {
                  publish_ack(state, ack_subject, ack_action)

                  state
                  |> decrement_pending
                  |> maybe_schedule_poll()
                  |> fn(state) { ConsumerState(..state, acc: updated_acc) }
                  |> actor.continue()
                }

                Stop(ack_action) -> {
                  publish_ack(state, ack_subject, ack_action)
                  actor.stop()
                }
              }
            }
            None -> {
              state
              |> decrement_pending
              |> maybe_schedule_poll()
              |> actor.continue()
            }
          }
        }
      }
  }
}

fn decrement_pending(state: ConsumerState(_)) {
  let updated_polls = case state.polls {
    [] -> []
    [PollRequest(pending: 1, ..), ..rest] -> rest
    [PollRequest(pending: pending, inbox: inbox), ..rest] -> {
      [PollRequest(pending: pending - 1, inbox: inbox), ..rest]
    }
  }
  ConsumerState(..state, polls: updated_polls, pending: state.pending - 1)
}

fn maybe_schedule_poll(state: ConsumerState(acc)) -> ConsumerState(acc) {
  case state.pending <= state.threshold {
    True -> {
      actor.send(state.self, Poll(state.max_messages - state.pending))
      ConsumerState(..state, pending: state.pending)
    }
    False -> state
  }
}

fn publish_ack(
  state: ConsumerState(acc),
  ack: String,
  ack_action: jetstream.AckAction,
) -> Nil {
  let _ =
    nats.publish(
      state.conn,
      nats.new_message(ack, jetstream.ack_action_to_payload(ack_action)),
    )
  Nil
}

pub fn continue(ack: jetstream.AckAction, acc: acc) {
  Continue(ack, acc)
}

pub fn stop(ack: jetstream.AckAction) {
  Stop(ack)
}
