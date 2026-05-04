# nuts

A NATS client library for Gleam, targeting the Erlang runtime.

[![Package Version](https://img.shields.io/hexpm/v/nuts)](https://hex.pm/packages/nuts)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/nuts/)

```sh
gleam add nuts@1
```

## Quick Start

```gleam
import gleam/erlang/process
import nuts

pub fn main() {
  let assert Ok(nats) = nuts.start(nuts.new("127.0.0.1", 4222))

  // Subscribe
  let assert Ok(sub) = nuts.subscribe(nats, "greetings")
  let subject = nuts.get_subject(sub)

  // Publish
  let msg = nuts.new_message("greetings", <<"hello">>)
  nuts.publish(nats, msg)

  // Receive
  let assert Ok(received) = process.receive(subject, 1000)

  // Cleanup
  nuts.unsubscribe(nats, sub)
  nuts.shutdown(nats)
}
```

## Features

- **Pub/Sub** with wildcard subject support
- **Request-Reply** pattern with configurable timeout
- **Headers** on messages via `add_header`
- **NKey Authentication** (Ed25519-based)
- **Auto-reconnect** with automatic resubscription
- **JetStream** stream and consumer management
- **Simple Consumer** for pull-based JetStream consumption
- **Supervised connections** via OTP supervision trees
- **Named connections** for process discovery
- **URL parsing** via `from_url`
- **Pluggable logging**

## Request-Reply

```gleam
// Send a request and wait for a response
let assert Ok(reply) =
  nuts.new_message("service.echo", <<"ping">>)
  |> nuts.request(nats, _, 1000)
```

## NKey Authentication

```gleam
let assert Ok(nats) =
  nuts.new("127.0.0.1", 4222)
  |> nuts.nkey_seed("SUALHP36...your_seed_here...")
  |> nuts.start()
```

## JetStream

```gleam
import nuts/jetstream
import nuts/jetstream_api

let js = jetstream.new_context(nats)

// Create a stream
let assert Ok(stream) = jetstream.create_stream(js,
  jetstream_api.StreamCreateRequest(
    stream_name: "events",
    subjects: ["events.>"],
    retention: jetstream_api.Limits,
    storage: jetstream_api.File,
    max_msgs_per_subject: 100,
    max_msgs: 10000,
    max_bytes: 0,
    max_age: 0,
    discard: jetstream_api.DiscardOld,
    ..jetstream_api.default_stream_config()
  )
)

// Create a consumer
let assert Ok(consumer) = jetstream.create_consumer(js,
  stream: "events",
  consumer_name: "worker-1",
  deliver_policy: jetstream_api.All,
  ack_policy: jetstream_api.AckExplicit,
  replay_policy: jetstream_api.Instant,
  max_deliver: 3,
  description: None,
)
```

## Simple Consumer

```gleam
import nuts/simple_consumer
import gleam/time

let assert Ok(_) = simple_consumer.start(
  nats, "events", "worker-1", 1000, 500,
  fn(msg, info) {
    // Process message, then return a handler reply:
    simple_consumer.ack()
    // simple_consumer.nak()
    // simple_consumer.retry_later(time.minutes(5))
  },
)
```

Further documentation can be found at <https://hexdocs.pm/nuts/>.

## Development

```sh
gleam test  # Run tests (requires a NATS server on port 6789)
```