import gleam/bit_array
import gleam/bool
import gleam/erlang/process.{type Subject}
import gleam/erlang/reference.{type Reference}
import gleam/io
import gleam/json
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/result
import gleam/string
import gleam/time/duration
import nuts
import nuts/internal/jetstream

type State {
  State(
    ctx: Context,
    self: Subject(Msg),
    poll_ref: Reference,
    pending_messages: Int,
    handler: fn(nuts.NatsMessage, jetstream.DeliveryInfo) -> jetstream.AckAction,
  )
}

type Msg {
  PollExpired(poll_ref: Reference)
  NatsMsg(nuts.NatsMessage)
}

pub fn start(
  ctx: Context,
  handler: fn(nuts.NatsMessage, jetstream.DeliveryInfo) -> jetstream.AckAction,
) -> Result(actor.Started(Nil), actor.StartError) {
  actor.new_with_initialiser(1000, fn(self) {
    // setup subscription on the inbox name
    case nuts.subscribe(ctx.nats_server, ctx.inbox_name) {
      Ok(subscription) -> {
        let subject = nuts.get_subject(subscription)

        let poll_ref = reference.new()
        process.send(self, PollExpired(poll_ref))

        actor.initialised(State(
          ctx:,
          self:,
          poll_ref:,
          pending_messages: ctx.max_messages,
          handler:,
        ))
        |> actor.selecting(
          process.new_selector()
          |> process.select(self)
          |> process.select_map(subject, NatsMsg),
        )
        |> Ok
      }
      Error(_) -> Error("failed to create subscription")
    }
  })
  |> actor.on_message(handle_message)
  |> actor.start
}

fn handle_message(state: State, msg: Msg) {
  case msg {
    PollExpired(poll_ref) -> {
      use <- bool.guard(
        when: poll_ref != state.poll_ref,
        return: actor.continue(state),
      )

      state
      |> make_poll_request(state.ctx.max_messages)
      |> actor.continue
    }
    NatsMsg(msg) -> {
      case state.ctx.inbox_name == msg.subject {
        True ->
          case get_status(msg.headers) {
            Ok("100" <> _) -> {
              // heartbeat: reset poll timer, server is still alive
              let new_poll_ref = reference.new()
              process.send_after(
                state.self,
                state.ctx.poll_expire |> duration.to_milliseconds,
                PollExpired(new_poll_ref),
              )
              State(..state, poll_ref: new_poll_ref)
            }
            Ok("408" <> _) ->
              // request timeout: server has no more messages, re-poll
              state
              |> make_poll_request(state.ctx.max_messages)
            _ -> state
          }
        False -> {
          let assert Ok(delivery_info) =
            msg.reply_to
            |> option.to_result(Nil)
            |> result.try(jetstream.parse_ack)

          let ack_action = state.handler(msg, delivery_info)

          let new_pending = state.pending_messages - 1
          let _ = ack_message(state.ctx, msg, ack_action)

          case new_pending <= state.ctx.threshold_messages {
            True -> {
              let batch_size = state.ctx.max_messages - new_pending
              make_poll_request(state, batch_size)
            }
            False -> {
              State(..state, pending_messages: new_pending)
            }
          }
        }
      }
      |> actor.continue()
    }
  }
}

fn make_poll_request(state: State, batch_size: Int) {
  let new_poll_ref = reference.new()

  case new_poll(state.ctx, batch_size) {
    Ok(_) -> {
      process.send_after(
        state.self,
        state.ctx.poll_expire |> duration.to_milliseconds,
        PollExpired(new_poll_ref),
      )
      State(
        ..state,
        poll_ref: new_poll_ref,
        pending_messages: state.ctx.max_messages,
      )
    }
    Error(_err) -> {
      // sending a poll failed, retry in 1s
      process.send_after(state.self, 1000, PollExpired(new_poll_ref))
      State(..state, poll_ref: new_poll_ref)
    }
  }
}

pub type Context {
  Context(
    inbox_name: String,
    stream_name: String,
    consumer_name: String,
    nats_server: Subject(nuts.Message),
    max_messages: Int,
    threshold_messages: Int,
    poll_expire: duration.Duration,
    heartbeat: option.Option(duration.Duration),
    no_wait: Bool,
  )
}

fn new_poll(ctx: Context, batch: Int) {
  nuts.new_message(
    "$JS.API.CONSUMER.MSG.NEXT." <> ctx.stream_name <> "." <> ctx.consumer_name,
    jetstream.PullNextMessages(
      batch:,
      expires: ctx.poll_expire,
      heartbeat: ctx.heartbeat,
      no_wait: ctx.no_wait,
    )
      |> jetstream.consumer_request_to_json()
      |> json.to_string
      |> bit_array.from_string,
  )
  |> nuts.reply_to(option.Some(ctx.inbox_name))
  |> nuts.publish(ctx.nats_server, _)
}

fn ack_message(
  ctx: Context,
  message: nuts.NatsMessage,
  action: jetstream.AckAction,
) {
  case message.reply_to {
    option.Some(ack_reply) -> {
      {
        let payload = jetstream.ack_action_to_payload(action)
        case
          nuts.new_message(ack_reply, payload)
          |> nuts.publish(ctx.nats_server, _)
        {
          Ok(_) -> Ok(Nil)
          Error(err) -> {
            io.println(string.inspect(err))
            Error(Nil)
          }
        }
      }
    }
    option.None -> Error(Nil)
  }
}

fn get_status(headers: List(#(String, String))) -> Result(String, Nil) {
  list.key_find(headers, "Nats-Status")
}
