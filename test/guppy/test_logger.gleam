import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/otp/actor
import guppy

pub type LogMessage {
  LogMessage(level: String, message: String)
}

type TestLoggerState {
  TestLoggerState(messages: List(LogMessage))
}

pub opaque type TestLoggerMessage {
  Push(LogMessage)
  GetLogs(reply: Subject(List(LogMessage)))
}

pub fn test_logger() {
  let assert Ok(actor.Started(_, subject)) =
    actor.new_with_initialiser(1000, fn(self) {
      actor.initialised(TestLoggerState([]))
      |> actor.returning(self)
      |> Ok
    })
    |> actor.on_message(fn(state: TestLoggerState, msg: TestLoggerMessage) {
      case msg {
        Push(log) -> {
          TestLoggerState([log, ..state.messages])
          |> actor.continue
        }
        GetLogs(reply:) -> {
          actor.send(reply, state.messages)
          actor.continue(state)
        }
      }
    })
    |> actor.start()

  subject
}

pub fn to_guppy_logger(logger: Subject(TestLoggerMessage)) {
  let log_message = fn(level: String) {
    fn(message: String) {
      actor.send(logger, Push(LogMessage(level, message:)))
    }
  }

  guppy.Logger(
    info: log_message("info"),
    debug: log_message("debug"),
    warning: log_message("warning"),
  )
}

pub fn get_logs(logger: Subject(TestLoggerMessage)) {
  actor.call(logger, 5000, GetLogs)
  |> list.reverse
}
