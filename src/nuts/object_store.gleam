import gleam/bit_array
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import nuts
import nuts/internal/object_store.{type ObjectInfo, type ObjectStoreConfig}
import nuts/jetstream

pub fn create_bucket(
  conn: Subject(nuts.Message),
  config: ObjectStoreConfig,
) -> Result(ObjectStoreConfig, nuts.NatsError) {
  let stream_config = object_store.to_stream_config(config)

  use _response <- result.try(jetstream.create_stream(conn, stream_config))
  Ok(config)
}

pub fn put(
  conn: Subject(nuts.Message),
  bucket: String,
  name: String,
  data: BitArray,
) -> Result(ObjectInfo, nuts.NatsError) {
  let nuid = object_store.generate_nuid()
  let chunk_subj = object_store.chunks_subject(bucket, nuid)
  let meta_subj = object_store.meta_subject(bucket, name)

  let chunks = object_store.split_into_chunks(data)
  let digest = object_store.compute_digest(data)

  use _ <- result.try(
    chunks
    |> list.try_map(fn(chunk) {
      nuts.new_message(chunk_subj, chunk)
      |> nuts.publish(conn, _)
    })
    |> result.map(fn(_) { Nil }),
  )

  let info =
    object_store.ObjectInfo(
      name:,
      bucket:,
      nuid:,
      size: bit_array.byte_size(data),
      chunks: list.length(chunks),
      digest:,
      deleted: False,
      description: None,
    )

  let meta_payload =
    info
    |> object_store.object_info_to_json
    |> json.to_string
    |> bit_array.from_string

  nuts.new_message(meta_subj, meta_payload)
  |> nuts.add_header("Nats-Rollup", "sub")
  |> nuts.publish(conn, _)
  |> result.map(fn(_) { info })
}

pub fn list(
  conn: Subject(nuts.Message),
  bucket: String,
) -> Result(List(ObjectInfo), nuts.NatsError) {
  let msg_get_subj = object_store.msg_get_subject(bucket)
  let meta_wildcard = object_store.all_meta_subject(bucket)
  let stream_name = object_store.stream_name(bucket)

  use stream_info <- result.try(jetstream.stream_info(conn, stream_name))

  let last_seq = stream_info.state.last_seq
  let first_seq = stream_info.state.first_seq

  case stream_info.state.messages == 0 || first_seq > last_seq {
    True -> Ok([])
    False ->
      list_loop(conn, msg_get_subj, meta_wildcard, first_seq, last_seq, [])
      |> result.map(list.filter(_, fn(info) { !info.deleted }))
  }
}

fn list_loop(
  conn: Subject(nuts.Message),
  msg_get_subj: String,
  meta_wildcard: String,
  current_seq: Int,
  last_seq: Int,
  acc: List(ObjectInfo),
) -> Result(List(ObjectInfo), nuts.NatsError) {
  case current_seq > last_seq {
    True -> Ok(list.reverse(acc))
    False -> {
      let payload =
        json.object([
          #("seq", json.int(current_seq)),
          #("next_by_subj", json.string(meta_wildcard)),
        ])
        |> json.to_string
        |> bit_array.from_string

      case
        nuts.request(conn, msg_get_subj, headers: [], payload:, timeout: 5000)
      {
        Ok(response) ->
          case
            json.parse_bits(
              response.payload,
              object_store.msg_get_response_decoder(),
            )
          {
            Ok(msg_get) ->
              case object_store.decoded_msg_data(msg_get.data) {
                Ok(data) ->
                  case
                    json.parse_bits(data, object_store.object_info_decoder())
                  {
                    Ok(info) ->
                      list_loop(
                        conn,
                        msg_get_subj,
                        meta_wildcard,
                        msg_get.seq + 1,
                        last_seq,
                        [info, ..acc],
                      )
                    Error(_) ->
                      Error(nuts.ProtocolError(
                        "failed to decode object info at seq "
                        <> int.to_string(msg_get.seq),
                      ))
                  }
                Error(_) ->
                  Error(nuts.ProtocolError(
                    "failed to decode base64 data at seq "
                    <> int.to_string(msg_get.seq),
                  ))
              }
            Error(_) ->
              Error(nuts.ProtocolError(
                "failed to decode msg get response at seq "
                <> int.to_string(current_seq),
              ))
          }
        Error(_) -> Ok(list.reverse(acc))
      }
    }
  }
}

pub fn get(
  conn: Subject(nuts.Message),
  bucket: String,
  name: String,
) -> Result(BitArray, nuts.NatsError) {
  let meta_subj = object_store.meta_subject(bucket, name)
  let msg_get_subj = object_store.msg_get_subject(bucket)

  let get_meta_payload =
    json.object([#("last_by_subj", json.string(meta_subj))])
    |> json.to_string
    |> bit_array.from_string

  use response <- result.try(nuts.request(
    conn,
    msg_get_subj,
    headers: [],
    payload: get_meta_payload,
    timeout: 5000,
  ))

  use msg_get <- result.try(
    case
      json.parse_bits(response.payload, object_store.msg_get_response_decoder())
    {
      Ok(resp) -> Ok(resp)
      Error(_) -> Error(nuts.ProtocolError("failed to decode msg get response"))
    },
  )

  use info_data <- result.try(
    object_store.decoded_msg_data(msg_get.data)
    |> result.replace_error(nuts.ProtocolError(
      "failed to decode base64 metadata",
    )),
  )

  use info <- result.try(
    case json.parse_bits(info_data, object_store.object_info_decoder()) {
      Ok(info) -> Ok(info)
      Error(_) -> Error(nuts.ProtocolError("failed to decode object info"))
    },
  )

  case info.deleted {
    True -> Error(nuts.GenericError("object is deleted"))
    False ->
      case info.size {
        0 -> Ok(<<>>)
        _ -> {
          let chunk_subj = object_store.chunks_subject(bucket, info.nuid)

          case read_chunks(conn, msg_get_subj, chunk_subj, info.chunks) {
            Ok(chunks) -> {
              let data = concat_chunks(chunks)

              case object_store.verify_digest(data, info.digest) {
                True -> Ok(data)
                False -> Error(nuts.ProtocolError("digest mismatch"))
              }
            }
            Error(err) -> Error(err)
          }
        }
      }
  }
}

fn read_chunks(
  conn: Subject(nuts.Message),
  msg_get_subj: String,
  chunk_subj: String,
  total_chunks: Int,
) -> Result(List(BitArray), nuts.NatsError) {
  read_chunks_loop(conn, msg_get_subj, chunk_subj, total_chunks, [], 0, None)
}

fn read_chunks_loop(
  conn: Subject(nuts.Message),
  msg_get_subj: String,
  chunk_subj: String,
  total_chunks: Int,
  acc: List(BitArray),
  collected_size: Int,
  next_seq: Option(Int),
) -> Result(List(BitArray), nuts.NatsError) {
  case list.length(acc) >= total_chunks {
    True -> Ok(list.reverse(acc))
    False -> {
      let payload = case next_seq {
        Some(seq) ->
          json.object([
            #("seq", json.int(seq)),
            #("next_by_subj", json.string(chunk_subj)),
          ])
        None -> json.object([#("next_by_subj", json.string(chunk_subj))])
      }
      let payload = payload |> json.to_string |> bit_array.from_string

      case
        nuts.request(conn, msg_get_subj, headers: [], payload:, timeout: 5000)
      {
        Ok(response) -> {
          case
            json.parse_bits(
              response.payload,
              object_store.msg_get_response_decoder(),
            )
          {
            Ok(msg_get) ->
              case object_store.decoded_msg_data(msg_get.data) {
                Ok(data) -> {
                  let next_seq = msg_get.seq + 1
                  read_chunks_loop(
                    conn,
                    msg_get_subj,
                    chunk_subj,
                    total_chunks,
                    [data, ..acc],
                    collected_size + bit_array.byte_size(data),
                    Some(next_seq),
                  )
                }
                Error(_) ->
                  Error(nuts.ProtocolError(
                    "failed to decode base64 chunk data for seq "
                    <> int.to_string(msg_get.seq),
                  ))
              }
            Error(_) ->
              Error(nuts.ProtocolError(
                "failed to decode chunk msg get response. payload: "
                <> {
                  response.payload
                  |> bit_array.to_string
                  |> result.unwrap("<<binary>>")
                },
              ))
          }
        }
        Error(err) -> Error(err)
      }
    }
  }
}

fn concat_chunks(chunks: List(BitArray)) -> BitArray {
  list.fold(chunks, <<>>, bit_array.append)
}
