import gleam/erlang/process
import gleam/option
import nuts

pub fn start(
  nats: process.Subject(nuts.Message),
  service_subject: String,
  callback: fn(nuts.ReceivedMessage) -> Result(BitArray, Nil),
) {
  process.spawn(fn() {
    let assert Ok(service_subject) = nuts.subscribe(nats, service_subject)
    handle(nats, service_subject, callback)
    Nil
  })
}

fn handle(
  conn: process.Subject(nuts.Message),
  subject: process.Subject(nuts.ReceivedMessage),
  handler: fn(nuts.ReceivedMessage) -> Result(BitArray, Nil),
) {
  let event = process.receive_forever(subject)
  let _ = case handler(event) {
    Ok(bits) -> {
      case event.message.reply_to {
        option.Some(inbox) -> {
          nuts.publish(conn, nuts.new_message(inbox, bits))
        }
        option.None -> todo
      }
    }
    Error(_) -> todo
  }

  handle(conn, subject, handler)
}
