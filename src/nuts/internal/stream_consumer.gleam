import gleam/bit_array
import gleam/bool
import gleam/erlang/process.{type Subject}
import gleam/erlang/reference.{type Reference}
import gleam/io
import gleam/json
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
    handler: fn(nuts.NatsMessage, jetstream.DeliveryInfo) -> Nil,
  )
}

type Msg {
  PollExpired(poll_ref: Reference)
  NatsMsg(nuts.NatsMessage)
}

pub fn start(
  ctx: Context,
  handler: fn(nuts.NatsMessage, jetstream.DeliveryInfo) -> Nil,
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
          pending_messages: ctx.batch_size,
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
      |> make_poll_request()
      |> actor.continue
    }
    NatsMsg(msg) -> {
      case state.ctx.inbox_name == msg.subject {
        True -> {
          string.inspect(msg) |> io.println
          state
        }
        False -> {
          let assert Ok(delivery_info) =
            msg.reply_to
            |> option.to_result(Nil)
            |> result.try(jetstream.parse_ack)

          state.handler(msg, delivery_info)
          case ack_message(state.ctx, msg) {
            Ok(_) -> {
              case state.pending_messages {
                1 -> make_poll_request(state)
                _ ->
                  State(..state, pending_messages: state.pending_messages - 1)
              }
            }
            Error(_) -> {
              string.inspect(msg) |> io.println
              state
            }
          }
        }
      }
      |> actor.continue()
    }
  }
}

fn make_poll_request(state: State) {
  let new_poll_ref = reference.new()

  case new_poll(state.ctx) {
    Ok(_) -> {
      process.send_after(
        state.self,
        state.ctx.poll_expire |> duration.to_milliseconds,
        PollExpired(new_poll_ref),
      )
      State(
        ..state,
        poll_ref: new_poll_ref,
        pending_messages: state.ctx.batch_size,
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
    batch_size: Int,
    poll_expire: duration.Duration,
  )
}

fn new_poll(ctx: Context) {
  nuts.new_message(
    "$JS.API.CONSUMER.MSG.NEXT." <> ctx.stream_name <> "." <> ctx.consumer_name,
    jetstream.PullNextMessages(batch: ctx.batch_size, expires: ctx.poll_expire)
      |> jetstream.consumer_request_to_json()
      |> json.to_string
      |> bit_array.from_string,
  )
  |> nuts.reply_to(option.Some(ctx.inbox_name))
  |> nuts.publish(ctx.nats_server, _)
}

fn ack_message(ctx: Context, message: nuts.NatsMessage) {
  case message.reply_to {
    option.Some(ack_reply) -> {
      {
        case
          nuts.new_message(ack_reply, <<>>)
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
