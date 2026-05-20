# guppy

A NATS client library for Gleam, targeting the Erlang runtime.

[![Package Version](https://img.shields.io/hexpm/v/guppy)](https://hex.pm/packages/guppy)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/guppy/)

```sh
gleam add guppy@1
```

## Quick Start

```gleam
import gleam/erlang/process
import gleam/otp/actor.{Started}
import guppy

pub fn main() {
  let assert Ok(Started(_, nats)) = guppy.start(guppy.new("127.0.0.1", 4222))

  // Subscribe
  let assert Ok(sub) = guppy.subscribe(nats, "greetings")
  let subject = guppy.get_subject(sub)

  // Publish
  let msg = guppy.new_message("greetings", <<"hello">>)
  guppy.publish(nats, msg)

  // Receive
  let assert Ok(received) = process.receive(subject, 1000)

  // Cleanup
  guppy.unsubscribe(nats, sub)
  guppy.shutdown(nats)
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
  guppy.new_message("service.echo", <<"ping">>)
  |> guppy.request(nats, _, 1000)
```

## NKey Authentication

```gleam
let assert Ok(gleam.otp.actor.Started(_, nats)) =
  guppy.new("127.0.0.1", 4222)
  |> guppy.nkey_seed("SUALHP36...your_seed_here...")
  |> guppy.start()
```

## JetStream

```gleam
import gleam/option.{None}
import guppy/jetstream

let js = jetstream.new_context(nats)

// Create a stream
let assert Ok(stream) = jetstream.create_stream(
  js,
  jetstream.StreamOptions(
    stream_name: "events",
    description: None,
    subjects: ["events.>"],
    retention: jetstream.Limits,
    discard_policy: jetstream.DiscardOld,
    max_consumers: -1,
    max_msgs: 10000,
    max_bytes: 0,
    max_age: 0,
    storage: jetstream.Memory,
    num_replicas: 1,
  ),
)

// Create a consumer
let assert Ok(consumer) = jetstream.create_consumer(
  js,
  "events",
  "worker-1",
  jetstream.ConsumerConfig(
    ..jetstream.default_consumer_config(),
    max_deliver: 3,
  ),
)
```

## Simple Consumer

```gleam
import gleam/option.{None}
import guppy/jetstream/simple_consumer

let assert Ok(_) = simple_consumer.start(
  nats,
  "events",
  "worker-1",
  max_messages: 1000,
  threshold: 500,
  fn(msg, info) {
    // Process message, then return a handler reply:
    simple_consumer.ack()
    // simple_consumer.retry()
    // simple_consumer.retry_later(time.minutes(5))
  },
  None,
)
```

Further documentation can be found at <https://hexdocs.pm/guppy/>.

## Development

```sh
gleam test  # Run tests
```

Integration tests require a NATS server. Either:

- Start one manually: `nats-server --port 6789`
- Use tests that call `test_utils.with_nats_server()` which spawn a temporary server on a random port
