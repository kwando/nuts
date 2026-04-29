import gleam/bit_array
import gleam/dynamic/decode
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/string

const max_payload = 1_048_576

const max_line_length = 4096

/// NATS protocol headers are terminated by \r\n (2 bytes). When reading
/// HMSG bodies the reported header size includes this separator, so we
/// subtract header_line_end_size to get the actual header content length.
const header_line_end_size = 2

pub type ServerInfo {
  ServerInfo(
    server_id: String,
    server_name: String,
    version: String,
    go: String,
    host: String,
    port: Int,
    headers: Bool,
    max_payload: Int,
    proto: Int,
    client_id: Option(Int),
    auth_required: Option(Bool),
    tls_required: Option(Bool),
    tls_verify: Option(Bool),
    tls_available: Option(Bool),
    connect_urls: Option(List(String)),
    ws_connect_urls: Option(List(String)),
    ldm: Option(Bool),
    git_commit: Option(String),
    jetstream: Option(Bool),
    ip: Option(String),
    client_ip: Option(String),
    nonce: Option(BitArray),
    cluster: Option(String),
    domain: Option(String),
  )
}

pub type ServerMessage {
  Info(ServerInfo)
  Ping
  Pong
  Msg(topic: String, sid: String, reply_to: Option(String), payload: BitArray)
  Hmsg(
    topic: String,
    headers: List(#(String, String)),
    sid: String,
    reply_to: Option(String),
    payload: BitArray,
  )
  OK
  ERR(String)
}

pub type ProtocolReadResult {
  Continue(ServerMessage, BitArray)
  ProtocolError(String)
  NeedsMoreData
}

pub fn parse(buffer: BitArray) -> ProtocolReadResult {
  case buffer {
    <<"INFO ", rest:bits>> -> {
      case read_line(rest) {
        Ok(#(info_msg, buffer)) -> {
          case json.parse_bits(info_msg, server_info_decoder()) {
            Ok(server_info) -> Continue(Info(server_info), buffer)
            Error(_) -> ProtocolError("failed to decode server info")
          }
        }
        Error(_) -> NeedsMoreData
      }
    }
    <<"PING\r\n", rest:bits>> -> Continue(Ping, rest)
    <<"PONG\r\n", rest:bits>> -> Continue(Pong, rest)
    <<"+OK\r\n", rest:bits>> -> Continue(OK, rest)
    <<"-ERR", rest:bits>> -> {
      case read_line(rest) {
        Error(_) -> ProtocolError("no error message provided")
        Ok(#(error, rest)) -> {
          case bit_array.to_string(error) {
            Error(_) ->
              ProtocolError("failed to convert error message to string")
            Ok(error_message) -> {
              let error_message = string.trim_start(error_message)
              case error_message {
                "" -> ProtocolError("empty error message")
                _ -> Continue(ERR(error_message), rest)
              }
            }
          }
        }
      }
    }
    <<"MSG ", rest:bits>> -> {
      case read_line(rest) {
        Ok(#(line, rest)) -> {
          use line <- convert_to_string(line)

          case string.split(line, " ") {
            [topic, sid, byte_size] -> {
              use byte_size <- with_int(byte_size)
              case read_body(rest, byte_size) {
                BodyReadSuccess(payload, rest) -> {
                  Continue(Msg(topic:, sid:, payload:, reply_to: None), rest)
                }
                BodyReadFail -> ProtocolError("malformed body")
                BodyTooShort -> NeedsMoreData
                OverMaxPayload -> ProtocolError("payload over max_payload")
              }
            }
            [topic, sid, reply_to, byte_size] -> {
              use byte_size <- with_int(byte_size)
              case read_body(rest, byte_size) {
                BodyReadSuccess(payload, rest) -> {
                  Continue(
                    Msg(topic:, sid:, payload:, reply_to: Some(reply_to)),
                    rest,
                  )
                }
                BodyReadFail -> ProtocolError("malformed body")
                BodyTooShort -> NeedsMoreData
                OverMaxPayload -> ProtocolError("payload over max_payload")
              }
            }
            _ -> ProtocolError("bad message")
          }
        }
        Error(..) -> NeedsMoreData
      }
    }
    <<"HMSG ", rest:bits>> -> {
      case read_line(rest) {
        Error(_) -> NeedsMoreData
        Ok(#(line, rest)) -> {
          use line <- convert_to_string(line)

          case string.split(line, " ") {
            [topic, sid, reply_to, header_bytes, total_bytes] -> {
              use header_bytes <- with_int(header_bytes)
              use total_bytes <- with_int(total_bytes)

              case read_body(rest, header_bytes - header_line_end_size) {
                BodyReadSuccess(headers, rest) -> {
                  case read_body(rest, total_bytes - header_bytes) {
                    BodyReadSuccess(body, rest) -> {
                      case parse_headers(headers) {
                        Ok(headers) ->
                          Continue(
                            Hmsg(
                              topic:,
                              sid:,
                              headers:,
                              payload: body,
                              reply_to: Some(reply_to),
                            ),
                            rest,
                          )
                        Error(_) -> ProtocolError("malformed headers")
                      }
                    }
                    BodyReadFail -> ProtocolError("malformed body")
                    BodyTooShort -> NeedsMoreData
                    OverMaxPayload -> ProtocolError("payload over max_payload")
                  }
                }
                BodyReadFail -> ProtocolError("failed to read headers")
                BodyTooShort -> NeedsMoreData
                OverMaxPayload -> ProtocolError("payload over max_payload")
              }
            }
            [topic, sid, header_bytes, total_bytes] -> {
              use header_bytes <- with_int(header_bytes)
              use total_bytes <- with_int(total_bytes)

              case read_body(rest, header_bytes - header_line_end_size) {
                BodyReadSuccess(headers, rest) -> {
                  case read_body(rest, total_bytes - header_bytes) {
                    BodyReadSuccess(body, rest) -> {
                      case parse_headers(headers) {
                        Ok(headers) ->
                          Continue(
                            Hmsg(
                              topic:,
                              sid:,
                              headers:,
                              payload: body,
                              reply_to: None,
                            ),
                            rest,
                          )
                        Error(_) -> ProtocolError("malformed headers")
                      }
                    }
                    BodyReadFail -> ProtocolError("malformed body")
                    BodyTooShort -> NeedsMoreData
                    OverMaxPayload -> ProtocolError("payload over max_payload")
                  }
                }
                BodyReadFail -> ProtocolError("failed to read headers")
                BodyTooShort -> NeedsMoreData
                OverMaxPayload -> ProtocolError("payload over max_payload")
              }
            }
            x -> ProtocolError("invalid HMSG: " <> string.inspect(x))
          }
        }
      }
    }
    data -> {
      case has_crlf(data) {
        True -> ProtocolError("invalid command")
        False -> NeedsMoreData
      }
    }
  }
}

fn convert_to_string(
  bits: BitArray,
  next: fn(String) -> ProtocolReadResult,
) -> ProtocolReadResult {
  case bit_array.to_string(bits) {
    Ok(str) -> next(str)
    Error(_) -> ProtocolError("line is not a valid string")
  }
}

fn has_crlf(buffer: BitArray) {
  has_crlf_loop(buffer, 0)
}

fn has_crlf_loop(buffer: BitArray, scanned: Int) {
  case buffer {
    <<"\r\n", _:bits>> -> True
    <<_, rest:bits>> if scanned < max_line_length ->
      has_crlf_loop(rest, scanned + 1)
    _ -> False
  }
}

fn with_int(
  input: String,
  callback: fn(Int) -> ProtocolReadResult,
) -> ProtocolReadResult {
  case int.parse(input) {
    Error(_) -> ProtocolError("bad integer")
    Ok(value) -> callback(value)
  }
}

fn parse_headers(headers: BitArray) {
  case bit_array.to_string(headers) {
    Ok(headers) ->
      case string.split(headers |> string.trim_end, "\r\n") {
        ["NATS/1.0" <> status, ..rest] -> {
          let status = string.trim_start(status)
          let status_entry = case status {
            "" -> []
            _ -> [#("Nats-Status", status)]
          }

          case
            list.try_map(rest, fn(line) {
              case string.split_once(line, ":") {
                Error(_) -> Error(Nil)
                Ok(#(key, value)) -> Ok(#(key, string.trim(value)))
              }
            })
          {
            Ok(headers) -> list.append(status_entry, headers) |> Ok
            Error(_) -> Error(Nil)
          }
        }
        _ -> Error(Nil)
      }
    _ -> Error(Nil)
  }
}

/// Reads all bytes until it finds a CRLF
fn read_line(buffer: BitArray) -> Result(#(BitArray, BitArray), Nil) {
  read_line_loop(buffer, <<>>)
}

fn read_line_loop(buffer: BitArray, msg: BitArray) {
  case buffer {
    <<"\r\n", rest:bits>> -> Ok(#(msg, rest))
    <<x, rest:bits>> -> read_line_loop(rest, <<msg:bits, x>>)
    <<>> -> Error(Nil)
    _ -> Error(Nil)
  }
}

type ReadBodyResult {
  BodyReadSuccess(payload: BitArray, rest: BitArray)
  BodyReadFail
  BodyTooShort
  OverMaxPayload
}

fn read_body(buffer: BitArray, bytes_to_read: Int) {
  case bytes_to_read > max_payload {
    True -> OverMaxPayload
    False -> read_body_loop(buffer, <<>>, bytes_to_read)
  }
}

fn read_body_loop(buffer: BitArray, payload: BitArray, bytes_to_read: Int) {
  case bytes_to_read, buffer {
    0, <<"\r\n", rest:bits>> -> BodyReadSuccess(payload, rest)
    0, <<>> -> BodyTooShort
    0, <<"\r">> -> BodyTooShort
    n, <<>> if n > 0 -> BodyTooShort
    n, <<x, rest:bits>> if n > 0 ->
      read_body_loop(rest, <<payload:bits, x>>, n - 1)
    _, _ -> BodyReadFail
  }
}

fn server_info_decoder() -> decode.Decoder(ServerInfo) {
  use server_id <- decode.field("server_id", decode.string)
  use server_name <- decode.field("server_name", decode.string)
  use version <- decode.field("version", decode.string)
  use go <- decode.field("go", decode.string)
  use host <- decode.field("host", decode.string)
  use port <- decode.field("port", decode.int)
  use headers <- decode.field("headers", decode.bool)
  use max_payload <- decode.field("max_payload", decode.int)
  use proto <- decode.field("proto", decode.int)
  use client_id <- decode.optional_field(
    "client_id",
    None,
    decode.optional(decode.int),
  )
  use auth_required <- decode.optional_field(
    "auth_required",
    None,
    decode.optional(decode.bool),
  )
  use tls_required <- decode.optional_field(
    "tls_required",
    None,
    decode.optional(decode.bool),
  )
  use tls_verify <- decode.optional_field(
    "tls_verify",
    None,
    decode.optional(decode.bool),
  )
  use tls_available <- decode.optional_field(
    "tls_available",
    None,
    decode.optional(decode.bool),
  )
  use connect_urls <- decode.optional_field(
    "connect_urls",
    None,
    decode.optional(decode.list(decode.string)),
  )
  use ws_connect_urls <- decode.optional_field(
    "ws_connect_urls",
    None,
    decode.optional(decode.list(decode.string)),
  )
  use ldm <- decode.optional_field("ldm", None, decode.optional(decode.bool))
  use git_commit <- decode.optional_field(
    "git_commit",
    None,
    decode.optional(decode.string),
  )
  use jetstream <- decode.optional_field(
    "jetstream",
    None,
    decode.optional(decode.bool),
  )
  use ip <- decode.optional_field("ip", None, decode.optional(decode.string))
  use client_ip <- decode.optional_field(
    "client_ip",
    None,
    decode.optional(decode.string),
  )
  use nonce <- decode.optional_field(
    "nonce",
    None,
    decode.optional(decode.bit_array),
  )
  use cluster <- decode.optional_field(
    "cluster",
    None,
    decode.optional(decode.string),
  )
  use domain <- decode.optional_field(
    "domain",
    None,
    decode.optional(decode.string),
  )
  decode.success(ServerInfo(
    server_id:,
    server_name:,
    version:,
    go:,
    host:,
    port:,
    headers:,
    max_payload:,
    proto:,
    client_id:,
    auth_required:,
    tls_required:,
    tls_verify:,
    tls_available:,
    connect_urls:,
    ws_connect_urls:,
    ldm:,
    git_commit:,
    jetstream:,
    ip:,
    client_ip:,
    nonce:,
    cluster:,
    domain:,
  ))
}
