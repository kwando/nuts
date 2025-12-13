import gleam/bit_array
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import nuts/connect_options.{type ConnectOptions}

pub fn ping() {
  <<"PING\r\n">>
}

pub fn pong() {
  <<"PONG\r\n">>
}

pub fn sub(
  subject topic: String,
  sid sid: String,
  queue_group queue_group: Option(String),
) {
  case queue_group {
    None -> <<"SUB ", topic:utf8, " ", sid:utf8, "\r\n">>
    Some(queue_group) -> <<
      "SUB ",
      topic:utf8,
      " ",
      sid:utf8,
      " ",
      queue_group:utf8,
      "\r\n",
    >>
  }
}

pub fn pub_(
  subject topic: String,
  reply_to reply_to: Option(String),
  payload payload: BitArray,
) {
  <<
    "PUB ",
    topic:utf8,
    " ",
    case reply_to {
      None -> ""
      Some(reply_to) -> reply_to <> " "
    }:utf8,
    { bit_array.byte_size(payload) |> int.to_string() }:utf8,
    "\r\n",
    payload:bits,
    "\r\n",
  >>
}

pub fn hpub(
  subject sub: String,
  reply_to reply_to: Option(String),
  headers headers: List(#(String, String)),
  payload payload: BitArray,
) {
  let reply_to = case reply_to {
    None -> ""
    Some(reply_to) -> " " <> reply_to
  }
  let header_bits = headers_to_bits(headers)
  let header_size = bit_array.byte_size(header_bits) + 2
  let total_bytes = bit_array.byte_size(payload) + header_size
  <<
    "HPUB ",
    sub:utf8,
    reply_to:utf8,
    " ",
    int.to_string(header_size):utf8,
    " ",
    int.to_string(total_bytes):utf8,
    "\r\n",
    header_bits:bits,
    "\r\n",
    payload:bits,
    "\r\n",
  >>
}

pub fn connect(data: ConnectOptions) {
  <<"CONNECT ", connect_options.to_json_string(data):utf8, "\r\n">>
}

pub fn unsub(sid: String, max_age: Option(Int)) {
  case max_age {
    None -> <<"UNSUB ", sid:utf8, "\r\n">>
    Some(max_age) -> <<
      "UNSUB ",
      sid:utf8,
      " ",
      int.to_string(max_age):utf8,
      "\r\n",
    >>
  }
}

fn headers_to_bits(headers: List(#(String, String))) {
  list.fold(headers, <<"NATS/1.0\r\n">>, fn(acc, header) {
    bit_array.append(acc, <<header.0:utf8, ": ", header.1:utf8, "\r\n">>)
  })
}
