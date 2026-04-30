import gleam/bit_array
import gleam/crypto
import gleam/dynamic/decode
import gleam/json
import gleam/option.{type Option, None}
import gleam/result
import gleam/time/duration.{type Duration}
import nuts/internal/stream

const default_chunk_size = 131_072

pub type ObjectStoreConfig {
  ObjectStoreConfig(
    bucket: String,
    description: Option(String),
    storage: stream.StorageType,
    ttl: Duration,
    max_bytes: Int,
    replicas: Int,
    compression: Bool,
  )
}

pub type ObjectInfo {
  ObjectInfo(
    name: String,
    bucket: String,
    nuid: String,
    size: Int,
    chunks: Int,
    digest: String,
    deleted: Bool,
    description: Option(String),
  )
}

pub type ObjectStoreStatus {
  ObjectStoreStatus(
    bucket: String,
    description: Option(String),
    ttl: Duration,
    storage: stream.StorageType,
    replicas: Int,
    sealed: Bool,
    metadata: Option(#(String, String)),
  )
}

pub type PubAck {
  PubAck(stream: String, seq: Int, domain: Option(String), duplicate: Bool)
}

// ----------------------------------------- Subject builders -----------------------------------------

pub fn stream_name(bucket: String) -> String {
  "OBJ_" <> bucket
}

pub fn all_chunks_subject(bucket: String) -> String {
  "$O." <> bucket <> ".C.>"
}

pub fn all_meta_subject(bucket: String) -> String {
  "$O." <> bucket <> ".M.>"
}

pub fn chunks_subject(bucket: String, nuid: String) -> String {
  "$O." <> bucket <> ".C." <> nuid
}

pub fn meta_subject(bucket: String, name: String) -> String {
  "$O." <> bucket <> ".M." <> encode_name(name)
}

pub fn msg_get_subject(bucket: String) -> String {
  "$JS.API.STREAM.MSG.GET.OBJ_" <> bucket
}

// ----------------------------------------- Stream config builder -----------------------------------------

pub fn to_stream_config(config: ObjectStoreConfig) -> stream.StreamConfig {
  let ObjectStoreConfig(
    bucket:,
    description:,
    storage:,
    ttl:,
    max_bytes:,
    replicas:,
    compression: _,
  ) = config
  let max_bytes = case max_bytes {
    0 -> -1
    n -> n
  }
  let replicas = case replicas {
    0 -> 1
    n -> n
  }
  stream.StreamConfig(
    name: stream_name(bucket),
    subjects: [all_chunks_subject(bucket), all_meta_subject(bucket)],
    retention: stream.Limits,
    max_consumers: -1,
    max_msgs: -1,
    max_bytes:,
    max_age: ttl,
    max_msgs_per_subject: -1,
    max_msg_size: -1,
    discard: stream.New,
    storage:,
    num_replicas: replicas,
    duplicate_window: duration.milliseconds(120_000),
    description:,
    allow_rollup: True,
    allow_direct: True,
  )
}

// ----------------------------------------- Name encoding -----------------------------------------

pub fn encode_name(name: String) -> String {
  bit_array.base64_url_encode(bit_array.from_string(name), True)
}

pub fn decode_name(encoded: String) -> Result(String, Nil) {
  use decoded <- result.try(bit_array.base64_url_decode(encoded))
  bit_array.to_string(decoded)
}

// ----------------------------------------- NUID -----------------------------------------

pub fn generate_nuid() -> String {
  crypto.strong_random_bytes(22)
  |> bit_array.base64_url_encode(False)
}

// ----------------------------------------- Digest -----------------------------------------

pub fn compute_digest(data: BitArray) -> String {
  let hash = crypto.hash(crypto.Sha256, data)
  "SHA-256=" <> bit_array.base64_url_encode(hash, True)
}

pub fn verify_digest(data: BitArray, digest: String) -> Bool {
  compute_digest(data) == digest
}

pub fn split_into_chunks(data: BitArray) -> List(BitArray) {
  split_into_chunks_with_size(data, default_chunk_size)
}

fn split_into_chunks_with_size(
  data: BitArray,
  chunk_size: Int,
) -> List(BitArray) {
  let data_size = bit_array.byte_size(data)
  case data_size <= chunk_size {
    True -> [data]
    False ->
      case data {
        <<chunk:bytes-size(chunk_size), rest:bits>> -> [
          chunk,
          ..split_into_chunks_with_size(rest, chunk_size)
        ]
        _ -> [data]
      }
  }
}

// ----------------------------------------- JSON encoding -----------------------------------------

pub fn object_info_to_json(info: ObjectInfo) -> json.Json {
  let ObjectInfo(
    name:,
    bucket:,
    nuid:,
    size:,
    chunks:,
    digest:,
    deleted:,
    description:,
  ) = info
  json.object(
    [
      #("name", json.string(name)),
      #("bucket", json.string(bucket)),
      #("nuid", json.string(nuid)),
      #("size", json.int(size)),
      #("chunks", json.int(chunks)),
      #("digest", json.string(digest)),
      #("deleted", json.bool(deleted)),
    ]
    |> optional("description", description, json.string),
  )
}

// ----------------------------------------- Stream Msg Get Response -----------------------------------------

pub type MsgGetResponse {
  MsgGetResponse(
    data: String,
    subject: String,
    seq: Int,
    headers: Option(String),
  )
}

pub fn msg_get_response_decoder() -> decode.Decoder(MsgGetResponse) {
  use message <- decode.field("message", msg_get_message_decoder())
  decode.success(message)
}

fn msg_get_message_decoder() -> decode.Decoder(MsgGetResponse) {
  use subject <- decode.field("subject", decode.string)
  use seq <- decode.field("seq", decode.int)
  use data <- decode.field("data", decode.string)
  use headers <- decode.optional_field(
    "hdrs",
    None,
    decode.optional(decode.string),
  )
  decode.success(MsgGetResponse(data:, subject:, seq:, headers:))
}

pub fn decoded_msg_data(data: String) -> Result(BitArray, Nil) {
  bit_array.base64_decode(data)
}

// ----------------------------------------- JSON decoding -----------------------------------------

pub fn object_info_decoder() -> decode.Decoder(ObjectInfo) {
  use name <- decode.field("name", decode.string)
  use bucket <- decode.field("bucket", decode.string)
  use nuid <- decode.field("nuid", decode.string)
  use size <- decode.field("size", decode.int)
  use chunks <- decode.field("chunks", decode.int)
  use digest <- decode.field("digest", decode.string)
  use deleted <- decode.optional_field("deleted", False, decode.bool)
  use description <- decode.optional_field(
    "description",
    None,
    decode.optional(decode.string),
  )
  decode.success(ObjectInfo(
    name:,
    bucket:,
    nuid:,
    size:,
    chunks:,
    digest:,
    deleted:,
    description:,
  ))
}

// ----------------------------------------- Helpers -----------------------------------------

fn optional(
  list: List(#(String, json.Json)),
  key: String,
  maybe: Option(a),
  mapper: fn(a) -> json.Json,
) -> List(#(String, json.Json)) {
  case maybe {
    option.Some(value) -> [#(key, mapper(value)), ..list]
    option.None -> list
  }
}
