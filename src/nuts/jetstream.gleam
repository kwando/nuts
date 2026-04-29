import gleam/bit_array
import gleam/erlang/process.{type Subject}
import gleam/json
import gleam/result
import nuts
import nuts/internal/stream

pub fn create_stream(
  conn: Subject(nuts.Message),
  config: stream.StreamConfig,
) -> Result(stream.StreamCreateResponse, nuts.NatsError) {
  let subject = stream.create_stream_subject(config.name)
  let payload =
    config
    |> stream.stream_config_to_json
    |> json.to_string
    |> bit_array.from_string

  use response <- result.try(nuts.request(conn, subject, [], payload, 5000))
  stream.decode_jetstream_response(
    response.payload,
    stream.stream_create_response_decoder(),
  )
}

pub fn stream_info(
  conn: Subject(nuts.Message),
  name: String,
) -> Result(stream.StreamInfoResponse, nuts.NatsError) {
  let subject = stream.stream_info_subject(name)

  use response <- result.try(nuts.request(conn, subject, [], <<>>, 5000))
  stream.decode_jetstream_response(
    response.payload,
    stream.stream_info_response_decoder(),
  )
}

pub fn update_stream(
  conn: Subject(nuts.Message),
  config: stream.StreamConfig,
) -> Result(stream.StreamUpdateResponse, nuts.NatsError) {
  let subject = stream.update_stream_subject(config.name)
  let payload =
    config
    |> stream.stream_config_to_json
    |> json.to_string
    |> bit_array.from_string

  use response <- result.try(nuts.request(conn, subject, [], payload, 5000))
  stream.decode_jetstream_response(
    response.payload,
    stream.stream_update_response_decoder(),
  )
}

pub fn list_stream_names(
  conn: Subject(nuts.Message),
) -> Result(stream.StreamNamesResponse, nuts.NatsError) {
  let subject = stream.stream_names_subject()

  use response <- result.try(nuts.request(conn, subject, [], <<>>, 5000))
  stream.decode_jetstream_response(
    response.payload,
    stream.stream_names_decoder(),
  )
}

pub fn purge_stream(
  conn: Subject(nuts.Message),
  name: String,
) -> Result(stream.StreamPurgeResult, nuts.NatsError) {
  let subject = stream.purge_stream_subject(name)

  use response <- result.try(nuts.request(conn, subject, [], <<>>, 5000))
  stream.decode_jetstream_response(
    response.payload,
    stream.stream_purge_decoder(),
  )
}
