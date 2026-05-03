import gleam/bit_array
import gleam/erlang/process.{type Subject}
import gleam/json
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import gleam/time/duration
import nuts
import nuts/internal/stream_api

pub opaque type JetstreamContext {
  JetstreamContext(
    conn: Subject(nuts.Message),
    log: fn(String) -> Nil,
    request_timeout: Int,
  )
}

pub fn new_context(conn: Subject(nuts.Message)) -> JetstreamContext {
  JetstreamContext(conn, fn(_) -> Nil { Nil }, request_timeout: 5000)
}

pub fn with_logger(
  js: JetstreamContext,
  log: fn(String) -> Nil,
) -> JetstreamContext {
  JetstreamContext(..js, log:)
}

pub type JetstreamError {
  ConnectionError(nuts.NatsError)
  ResponseDecodeError(json.DecodeError)
  ApiError(code: Int, description: String, err_code: Int)
  StreamNotFound
  StreamAlreadyExistsWithDifferentConfig
}

pub fn list_stream_names(
  js: JetstreamContext,
) -> Result(stream_api.StreamNamesResponse, JetstreamError) {
  let assert Ok(msg) =
    nuts.request(
      js.conn,
      stream_api.list_stream_names_request(None, None),
      js.request_timeout,
    )

  decode_response(js, msg, stream_api.stream_names_response_decoder())
}

pub fn create_stream(
  js: JetstreamContext,
  options: stream_api.StreamCreateRequest,
) -> Result(stream_api.StreamCreateResponse, JetstreamError) {
  use msg <- make_request(js, stream_api.stream_create_request(options))

  use api_response <- result.try(decode_response(
    js,
    msg,
    stream_api.stream_create_response_decoder(),
  ))
  api_response
  |> result.map_error(map_api_error)
}

pub fn get_info(
  js: JetstreamContext,
  stream: String,
) -> Result(stream_api.StreamGetInfoResponse, JetstreamError) {
  use resp <- make_request(
    js,
    stream_api.stream_get_info_request(
      stream:,
      deleted_details: False,
      subjects_filter: None,
      offset: 0,
    ),
  )
  js.log(resp.payload |> bit_array.to_string |> result.unwrap("<<binary>>"))

  use decoded_result <- result.try(decode_response(
    js,
    resp,
    stream_api.stream_get_info_response_decoder(),
  ))
  decoded_result
  |> result.map_error(map_api_error)
}

fn make_request(
  js: JetstreamContext,
  msg: nuts.NatsMessage,
  next: fn(nuts.NatsMessage) -> Result(a, JetstreamError),
) -> Result(a, JetstreamError) {
  case nuts.request(js.conn, msg, js.request_timeout) {
    Ok(response) -> next(response)
    Error(err) -> Error(ConnectionError(err))
  }
}

pub fn delete_stream(
  js: JetstreamContext,
  stream: String,
) -> Result(stream_api.StreamDeleteResponse, JetstreamError) {
  use msg <- make_request(js, stream_api.stream_delete_request(stream))
  use decoded_result <- result.try(decode_response(
    js,
    msg,
    stream_api.stream_delete_response_decoder(),
  ))
  decoded_result
  |> result.map_error(map_api_error)
}

pub fn create_consumer(
  js: JetstreamContext,
  stream stream: String,
  description description: Option(String),
  consumer_name consumer_name: String,
  deliver_policy deliver_policy: stream_api.DeliverPolicy,
  ack_policy ack_policy: stream_api.AckPolicy,
  replay_policy replay_policy: stream_api.ReplayPolicy,
) -> Result(stream_api.ConsumerCreateResponse, JetstreamError) {
  use msg <- make_request(
    js,
    stream_api.consumer_create_request(
      stream,
      description,
      consumer_name,
      deliver_policy,
      ack_policy,
      replay_policy,
    ),
  )
  use decoded_result <- result.try(decode_response(
    js,
    msg,
    stream_api.consumer_create_response_decoder(),
  ))
  decoded_result
  |> result.map_error(map_api_error)
}

pub fn pull_next_messages(
  js: JetstreamContext,
  stream: String,
  consumer_name: String,
) -> Result(List(nuts.NatsMessage), JetstreamError) {
  use msg <- make_request(
    js,
    stream_api.consumer_pull_next_messages(
      stream,
      consumer_name,
      expires: Some(duration.seconds(30)),
      batch: None,
      max_bytes: None,
    ),
  )
  todo
}

fn decode_response(js: JetstreamContext, msg: nuts.NatsMessage, decoder) {
  js.log(msg |> string.inspect)
  js.log(msg.payload |> bit_array.to_string |> result.unwrap("<<binary>>"))
  json.parse_bits(msg.payload, decoder)
  |> result.map_error(ResponseDecodeError)
}

fn map_api_error(err: stream_api.StreamApiError) {
  case err.err_code {
    10_059 -> StreamNotFound
    10_058 -> StreamAlreadyExistsWithDifferentConfig
    _ ->
      ApiError(
        code: err.code,
        description: err.description,
        err_code: err.err_code,
      )
  }
}
