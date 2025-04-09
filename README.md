# nuts
NATS client library for Gleam
[![Package Version](https://img.shields.io/hexpm/v/nuts)](https://hex.pm/packages/nuts)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/nuts/)

```sh
gleam add nuts@1
```
```gleam
import nuts

pub fn main() {
  let assert Ok(nats) = nuts.start(nuts.new("127.0.0.1", 4222))
  nuts.subscribe(nats, "some.topic", fn(msg){
    echo msg
  })
}
```

Further documentation can be found at <https://hexdocs.pm/nuts>.

## Development

```sh
gleam run   # Run the project
gleam test  # Run the tests
```
