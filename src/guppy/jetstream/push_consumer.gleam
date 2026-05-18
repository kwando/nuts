//// A push-based JetStream consumer actor that receives messages
//// delivered by the NATS server to a configured deliver subject.
////
//// Unlike the pull consumer, this consumer does not need to poll for
//// messages — the NATS server pushes them to the deliver subject
//// configured on the JetStream consumer.
////
//// The JetStream consumer must already exist on the server with a
//// `deliver_subject` set. Use `jetstream.create_consumer()` to create
//// one before calling `start()`.

///
/// Flow control and idle heartbeat messages are handled automatically
/// without invoking the handler.
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import gleam/string
import gleam/time/duration.{type Duration}
import guppy.{type Logger, type Message, type NatsMessage} as nats
import guppy/jetstream.{type DeliveryInfo}

type PushConsumerState {
  PushConsumerState(
    self: Subject(PushConsumerMessage),
    handler: fn(NatsMessage, DeliveryInfo) -> HandlerReply,
    deliver_subject: String,
    logger: Logger,
    conn: Subject(Message),
    last_consumer_seq: Option(Int),
  )
}

type PushConsumerMessage {
  NatsMsg(NatsMessage)
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

/// Start a new push-based JetStream consumer actor.
///
/// The consumer will subscribe to the given `deliver_subject` on the
/// NATS connection and invoke the handler for each received message.
///
/// The JetStream consumer should already exist on the server with the
/// matching `deliver_subject`. Use `jetstream.create_consumer()` prior
/// to calling this function.
///
/// ## Parameters
///
/// - `conn` - The NATS connection subject used to publish and subscribe.
/// - `deliver_subject` - The subject to receive messages on (must match the consumer's config).
/// - `deliver_group` - Optional queue group for competing consumer semantics.
/// - `handler` - A function called for each received message that decides how it should be acknowledged.
pub fn start(
  conn: Subject(Message),
  deliver_subject deliver_subject: String,
  deliver_group deliver_group: Option(String),
  handler handler: fn(NatsMessage, DeliveryInfo) -> HandlerReply,
  logger logger: Option(Logger),
) -> Result(actor.Started(Nil), actor.StartError) {
  actor.new_with_initialiser(1000, fn(self) {
    case
      {
        case deliver_group {
          Some(group) ->
            nats.subscribe_with_queue_group(conn, deliver_subject, group)
          None -> nats.subscribe(conn, deliver_subject)
        }
      }
    {
      Ok(subscription) -> {
        let handler_subject = nats.get_subject(subscription)
        let logger = case logger {
          Some(logger) -> logger
          None -> nats.noop_logger()
        }
        actor.initialised(PushConsumerState(
          self:,
          handler:,
          deliver_subject:,
          logger:,
          conn:,
          last_consumer_seq: None,
        ))
        |> actor.selecting(
          process.new_selector()
          |> process.select_map(handler_subject, NatsMsg),
        )
        |> Ok
      }
      Error(err) -> {
        Error("failed to create subscription: " <> string.inspect(err))
      }
    }
  })
  |> actor.on_message(handle_message)
  |> actor.start
}

fn handle_message(
  state: PushConsumerState,
  msg: PushConsumerMessage,
) -> actor.Next(PushConsumerState, a) {
  case msg {
    NatsMsg(msg) -> {
      case is_flow_control(msg) {
        True -> {
          let state = track_consumer_seq_from_fc(state, msg)

          case msg.reply_to {
            Some(reply_to) -> {
              case
                nats.new_message(reply_to, <<>>)
                |> nats.publish(state.conn, _)
              {
                Error(err) ->
                  state.logger.warning(
                    "flow control response failed: " <> string.inspect(err),
                  )
                Ok(_) -> Nil
              }
            }
            None -> Nil
          }
          actor.continue(state)
        }
        False -> {
          case msg.reply_to {
            Some(ack) -> handle_jetstream_message(state, state.conn, msg, ack)
            None -> actor.continue(state)
          }
        }
      }
    }
  }
}

fn handle_jetstream_message(
  state: PushConsumerState,
  conn: Subject(Message),
  msg: NatsMessage,
  ack: String,
) -> actor.Next(PushConsumerState, a) {
  let info = jetstream.parse_ack(ack)
  let #(ack_action, state) = case info {
    Ok(info) -> {
      let state = update_last_consumer_seq(state, info.consumer_seq)
      let action = case state.handler(msg, info) {
        Ack -> jetstream.Ack
        Retry -> jetstream.Nak
        RetryLater(delay) -> jetstream.NakWithDelay(delay)
        Terminate -> jetstream.Term
      }
      #(action, state)
    }
    Error(_) -> #(jetstream.Nak, state)
  }

  case
    nats.new_message(ack, jetstream.ack_action_to_payload(ack_action))
    |> nats.publish(conn, _)
  {
    Error(err) ->
      state.logger.warning("ack publish failed: " <> string.inspect(err))
    Ok(_) -> Nil
  }

  case ack_action {
    jetstream.Term -> actor.stop()
    _ -> actor.continue(state)
  }
}

fn is_flow_control(msg: NatsMessage) -> Bool {
  list.key_find(msg.headers, "Nats-Expected-Last-Consumer-Sequence")
  |> result.is_ok
}

fn track_consumer_seq_from_fc(
  state: PushConsumerState,
  msg: NatsMessage,
) -> PushConsumerState {
  case
    msg.headers
    |> list.key_find("Nats-Expected-Last-Consumer-Sequence")
    |> result.try(int.parse)
  {
    Ok(seq) -> update_last_consumer_seq(state, seq)
    Error(_) -> state
  }
}

fn update_last_consumer_seq(
  state: PushConsumerState,
  seq: Int,
) -> PushConsumerState {
  case state.last_consumer_seq {
    Some(current) if current >= seq -> state
    _ -> {
      state.logger.debug("consumer seq: " <> int.to_string(seq))
      PushConsumerState(..state, last_consumer_seq: Some(seq))
    }
  }
}

/// Request message redelivery after the given duration.
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
