import gleam/bit_array
import gleam/int
import gleam/list
import gleam/option
import gleam/string

pub type ServerMessage {
  Info(BitArray)
  Ping
  Pong
  Msg(
    topic: String,
    sid: String,
    reply_to: option.Option(String),
    payload: BitArray,
  )
  Hmsg(
    topic: String,
    headers: List(#(String, String)),
    sid: String,
    reply_to: option.Option(String),
    payload: BitArray,
  )
}

pub type ProtocolReadResult {
  Continue(ServerMessage, BitArray)
  ProtocolError(String)
  NeedsMoreData
}

pub fn parse(buffer: BitArray) -> ProtocolReadResult {
  case buffer {
    <<"INFO ", rest:bits>> -> {
      case readline(rest) {
        Ok(#(info_msg, buffer)) -> Continue(Info(info_msg), buffer)
        Error(_) -> NeedsMoreData
      }
    }
    <<"PING\r\n", rest:bits>> -> Continue(Ping, rest)
    <<"PONG\r\n", rest:bits>> -> Continue(Pong, rest)
    <<"+OK\r\n", rest:bits>> -> parse(rest)
    <<"MSG ", rest:bits>> -> {
      case readline(rest) {
        Ok(#(line, rest)) -> {
          let assert Ok(line) = bit_array.to_string(line)

          case string.split(line, " ") {
            [topic, sid, byte_size] -> {
              use byte_size <- with_int(byte_size)
              case read_body(rest, <<>>, byte_size) {
                BodyReadSuccess(payload, rest) -> {
                  Continue(
                    Msg(topic:, sid:, payload:, reply_to: option.None),
                    rest,
                  )
                }
                BodyReadFail -> ProtocolError("malformed body")
                BodyTooShort -> NeedsMoreData
              }
            }
            [topic, sid, reply_to, byte_size] -> {
              use byte_size <- with_int(byte_size)
              case read_body(rest, <<>>, byte_size) {
                BodyReadSuccess(payload, rest) -> {
                  Continue(
                    Msg(topic:, sid:, payload:, reply_to: option.Some(reply_to)),
                    rest,
                  )
                }
                BodyReadFail -> ProtocolError("malformed body")
                BodyTooShort -> NeedsMoreData
              }
            }
            _ -> ProtocolError("bad message")
          }
        }
        Error(..) -> NeedsMoreData
      }
    }
    <<"HMSG ", rest:bits>> -> {
      case readline(rest) {
        Error(_) -> NeedsMoreData
        Ok(#(line, rest)) -> {
          let assert Ok(line) = bit_array.to_string(line)
          case string.split(line, " ") {
            [topic, sid, header_bytes, total_bytes] -> {
              use header_bytes <- with_int(header_bytes)
              use total_bytes <- with_int(total_bytes)

              case read_body(rest, <<>>, header_bytes - 2) {
                BodyReadSuccess(headers, rest) -> {
                  case read_body(rest, <<>>, total_bytes - header_bytes) {
                    BodyReadSuccess(body, rest) -> {
                      case parse_headers(headers) {
                        Ok(headers) ->
                          Continue(
                            Hmsg(
                              topic:,
                              sid:,
                              headers:,
                              payload: body,
                              reply_to: option.None,
                            ),
                            rest,
                          )
                        Error(_) -> ProtocolError("malformed headers")
                      }
                    }
                    BodyReadFail -> ProtocolError("malformed body")
                    BodyTooShort -> NeedsMoreData
                  }
                }
                BodyReadFail -> ProtocolError("failed to read headers")
                BodyTooShort -> NeedsMoreData
              }
            }
            _ -> ProtocolError("invalid HMSG")
          }
        }
      }
    }
    <<>> -> NeedsMoreData
    msg -> ProtocolError("unhandled protocol message: " <> string.inspect(msg))
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
  case headers {
    <<"NATS/1.0\r\n", data:bits>> -> {
      let assert Ok(data) = bit_array.to_string(data)

      data
      |> string.trim
      |> string.split("\r\n")
      |> list.try_map(fn(line) {
        case string.split_once(line, ":") {
          Error(_) -> Error(Nil)
          Ok(#(key, value)) -> Ok(#(key, string.trim(value)))
        }
      })
    }
    _ -> Error(Nil)
  }
}

/// Reads all bytes until it finds a CRLF
fn readline(buffer: BitArray) {
  readline_loop(buffer, <<>>)
}

fn readline_loop(buffer: BitArray, msg: BitArray) {
  case buffer {
    <<"\r\n", rest:bits>> -> Ok(#(msg, rest))
    <<x, rest:bits>> -> readline_loop(rest, <<msg:bits, x>>)
    <<>> -> Error(Nil)
    _ -> panic as "unaligned bytes detected"
  }
}

type ReadBodyResult {
  BodyReadSuccess(payload: BitArray, rest: BitArray)
  BodyReadFail
  BodyTooShort
}

fn read_body(buffer: BitArray, payload: BitArray, bytes_to_read: Int) {
  case bytes_to_read, buffer {
    0, <<"\r\n", rest:bits>> -> BodyReadSuccess(payload, rest)
    0, <<>> | 0, <<"\r">> -> BodyTooShort
    n, <<>> if n > 0 -> BodyTooShort
    n, <<x, rest:bits>> if n > 0 -> read_body(rest, <<payload:bits, x>>, n - 1)

    _, _ -> {
      echo #(BodyReadFail, bytes_to_read, buffer, payload)
      BodyReadFail
    }
  }
}
