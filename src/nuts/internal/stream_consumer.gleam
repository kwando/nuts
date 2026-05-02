import gleam/bit_array
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/json
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/result
import gleam/string
import gleam/time/duration
import nuts.{type Logger}
import nuts/internal/jetstream

type State {
  State(
    ctx: Context,
    self: Subject(Msg),
    poll_timer: process.Timer,
    pending_messages: Int,
    handler: fn(nuts.NatsMessage, jetstream.DeliveryInfo) -> jetstream.AckAction,
  )
}

type Msg {
  PollExpired
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

        let poll_timer = process.send_after(self, 0, PollExpired)

        ctx.logger.debug(
          "stream_consumer start: stream="
          <> ctx.stream_name
          <> " consumer="
          <> ctx.consumer_name
          <> " inbox="
          <> ctx.inbox_name
          <> " max_messages="
          <> string.inspect(ctx.max_messages),
        )

        actor.initialised(State(
          ctx:,
          self:,
          poll_timer:,
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
    PollExpired -> {
      state.ctx.logger.debug(
        "stream_consumer: poll_expired, re-polling max_messages="
        <> string.inspect(state.ctx.max_messages),
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
              state.ctx.logger.debug(
                "stream_consumer: heartbeat (100) from " <> ctx_name(state.ctx),
              )
              process.cancel_timer(state.poll_timer)
              let poll_timer =
                process.send_after(
                  state.self,
                  state.ctx.poll_expire |> duration.to_milliseconds,
                  PollExpired,
                )
              State(..state, poll_timer:)
            }
            Ok("408" <> _) -> {
              state.ctx.logger.debug(
                "stream_consumer: timeout (408) from "
                <> ctx_name(state.ctx)
                <> ", re-polling",
              )
              state
              |> make_poll_request(state.ctx.max_messages)
            }
            Ok(status) -> {
              state.ctx.logger.debug(
                "stream_consumer: unexpected status="
                <> status
                <> " from "
                <> ctx_name(state.ctx),
              )
              state
            }
            Error(_) -> state
          }
        False -> {
          let assert Ok(delivery_info) =
            msg.reply_to
            |> option.to_result(Nil)
            |> result.try(jetstream.parse_ack)

          // state.ctx.logger.debug(
          //   "stream_consumer: msg subject="
          //   <> msg.subject
          //   <> " delivery="
          //   <> string.inspect(delivery_info)
          //   <> " pending="
          //   <> string.inspect(state.pending_messages),
          // )

          let ack_action = state.handler(msg, delivery_info)

          //state.ctx.logger.debug(
          //  "stream_consumer: ack_action=" <> string.inspect(ack_action),
          //)

          let new_pending = state.pending_messages - 1
          let _ = ack_message(state.ctx, msg, ack_action)

          case new_pending <= state.ctx.threshold_messages {
            True -> {
              let batch_size = state.ctx.max_messages - new_pending
              state.ctx.logger.debug(
                "stream_consumer: threshold reached, fetching batch="
                <> string.inspect(batch_size),
              )
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
  case new_poll(state.ctx, batch_size) {
    Ok(_) -> {
      state.ctx.logger.debug(
        "stream_consumer: poll sent batch="
        <> string.inspect(batch_size)
        <> " to "
        <> ctx_name(state.ctx)
        <> " pending="
        <> state.pending_messages |> int.to_string,
      )
      process.cancel_timer(state.poll_timer)
      let poll_timer =
        process.send_after(
          state.self,
          state.ctx.poll_expire |> duration.to_milliseconds,
          PollExpired,
        )
      State(..state, poll_timer:, pending_messages: state.ctx.max_messages)
    }
    Error(err) -> {
      state.ctx.logger.debug(
        "stream_consumer: poll failed batch="
        <> string.inspect(batch_size)
        <> " err="
        <> string.inspect(err)
        <> ", retrying in 1s",
      )
      process.cancel_timer(state.poll_timer)
      let poll_timer = process.send_after(state.self, 1000, PollExpired)
      State(..state, poll_timer:)
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
    logger: Logger,
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
            ctx.logger.debug(
              "stream_consumer: ack failed err="
              <> string.inspect(err)
              <> " action="
              <> string.inspect(action)
              <> " stream="
              <> ctx.stream_name,
            )
            Error(Nil)
          }
        }
      }
    }
    option.None -> {
      ctx.logger.debug(
        "stream_consumer: ack skipped, no reply_to on msg stream="
        <> ctx.stream_name,
      )
      Error(Nil)
    }
  }
}

fn get_status(headers: List(#(String, String))) -> Result(String, Nil) {
  list.key_find(headers, "Nats-Status")
}

fn ctx_name(ctx: Context) -> String {
  ctx.stream_name <> "/" <> ctx.consumer_name
}
