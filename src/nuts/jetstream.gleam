import gleam/bit_array
import gleam/erlang/process.{type Subject}
import gleam/json
import gleam/result
import nuts
import nuts/internal/stream.{type JetStreamError}

pub fn create_stream(
  conn: Subject(nuts.Message),
  config: stream.StreamConfig,
) -> Result(stream.StreamCreateResponse, JetStreamError) {
  let subject = stream.create_stream_subject(config.name)
  let payload =
    config
    |> stream.stream_config_to_json
    |> json.to_string
    |> bit_array.from_string

  use response <- result.try(
    nuts.request(conn, subject, [], payload, 5000)
    |> result.map_error(stream.TransportError),
  )
  stream.decode_jetstream_response(
    response.payload,
    stream.stream_create_response_decoder(),
  )
}

pub fn stream_info(
  conn: Subject(nuts.Message),
  name: String,
) -> Result(stream.StreamInfoResponse, JetStreamError) {
  let subject = stream.stream_info_subject(name)

  use response <- result.try(
    nuts.request(conn, subject, [], <<>>, 5000)
    |> result.map_error(stream.TransportError),
  )
  stream.decode_jetstream_response(
    response.payload,
    stream.stream_info_response_decoder(),
  )
}

pub fn update_stream(
  conn: Subject(nuts.Message),
  config: stream.StreamConfig,
) -> Result(stream.StreamUpdateResponse, JetStreamError) {
  let subject = stream.update_stream_subject(config.name)
  let payload =
    config
    |> stream.stream_config_to_json
    |> json.to_string
    |> bit_array.from_string

  use response <- result.try(
    nuts.request(conn, subject, [], payload, 5000)
    |> result.map_error(stream.TransportError),
  )
  stream.decode_jetstream_response(
    response.payload,
    stream.stream_update_response_decoder(),
  )
}

pub fn list_stream_names(
  conn: Subject(nuts.Message),
) -> Result(stream.StreamNamesResponse, JetStreamError) {
  let subject = stream.stream_names_subject()

  use response <- result.try(
    nuts.request(conn, subject, [], <<>>, 5000)
    |> result.map_error(stream.TransportError),
  )
  stream.decode_jetstream_response(
    response.payload,
    stream.stream_names_decoder(),
  )
}

pub fn purge_stream(
  conn: Subject(nuts.Message),
  name: String,
) -> Result(stream.StreamPurgeResult, JetStreamError) {
  let subject = stream.purge_stream_subject(name)

  use response <- result.try(
    nuts.request(conn, subject, [], <<>>, 5000)
    |> result.map_error(stream.TransportError),
  )
  stream.decode_jetstream_response(
    response.payload,
    stream.stream_purge_decoder(),
  )
}

pub fn publish(
  conn: Subject(nuts.Message),
  subject subject: String,
  headers headers: List(#(String, String)),
  payload payload: BitArray,
  timeout timeout: Int,
) -> Result(stream.PubAck, JetStreamError) {
  use response <- result.try(
    nuts.request(conn, subject, headers, payload, timeout)
    |> result.map_error(stream.TransportError),
  )
  stream.decode_jetstream_response(response.payload, stream.pub_ack_decoder())
}
