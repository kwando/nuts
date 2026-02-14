import gleam/erlang/process.{type Subject}
import gleam/option.{None, Some}
import nuts as nats

pub fn main() {
  let name = process.new_name("")
  let assert Ok(_started) =
    nats.new("127.0.0.1", 4222)
    |> nats.start(name, _)

  let conn = process.named_subject(name)

  let assert Ok(subscription) =
    conn
    |> nats.subscribe("wobble")

  use msg <- run_service(conn, subscription |> nats.get_subject)
  echo msg
  Ok(<<"nice from ">>)
}

type ServiceFunction(error) =
  fn(nats.NatsMessage) -> Reply(error)

type Reply(error) =
  Result(BitArray, error)

fn run_service(
  conn: Subject(nats.Message),
  messages: Subject(nats.NatsMessage),
  service: ServiceFunction(error),
) {
  case process.receive(messages, 1000) {
    Ok(msg) -> {
      case msg.reply_to {
        Some(inbox) -> {
          case service(msg) {
            Ok(payload) -> {
              let _ =
                nats.new_message(inbox, payload)
                |> nats.publish(conn, _)
              Nil
            }
            Error(_) -> Nil
          }
        }
        None -> Nil
      }
    }
    Error(_) -> Nil
  }
  run_service(conn, messages, service)
}
