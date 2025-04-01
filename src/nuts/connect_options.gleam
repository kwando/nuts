import gleam/json
import gleam/list

pub type ConnectOption {
  Verbose(Bool)
  Pedantic(Bool)
  TlsRequired(Bool)
  Lang(String)
  Version(String)
  Headers(Bool)
  NKey(String)
  Echo(Bool)
  Protocol(Int)
}

pub const default_options: List(ConnectOption) = [
  Verbose(True),
  Pedantic(True),
  TlsRequired(False),
  Lang("gleam"),
  Version("0.0.1"),
  Protocol(0),
]

pub fn to_json(options: List(ConnectOption)) {
  json.to_string(
    json.object(
      list.map(options, fn(opt) {
        case opt {
          Verbose(v) -> #("verbose", json.bool(v))
          Echo(v) -> #("echo", json.bool(v))
          Headers(v) -> #("headers", json.bool(v))
          Lang(s) -> #("lang", json.string(s))
          NKey(s) -> #("nkey", json.string(s))
          Pedantic(v) -> #("pedantic", json.bool(v))
          TlsRequired(v) -> #("tls_required", json.bool(v))
          Version(s) -> #("version", json.string(s))
          Protocol(i) -> #("protocol", json.int(i))
        }
      }),
    ),
  )
}
