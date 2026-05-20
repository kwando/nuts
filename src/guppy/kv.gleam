//// NATS Key-Value Store client for the Guppy library.
////
//// This module provides a high-level API for interacting with NATS KV buckets,
//// including bucket management and key-value operations.
////
//// ## Quick start
////
//// ```gleam
//// import guppy
//// import guppy/kv
////
//// pub fn main() {
////   let assert Ok(gleam.otp.actor.Started(_, conn)) = guppy.start(guppy.new("127.0.0.1", 4222))
////   let ctx = kv.new_context(conn)
////
////   // Create a bucket
////   let assert Ok(_) = kv.create_bucket(ctx, kv.default_bucket_config("my-bucket"))
////
////   // Put a value
////   let assert Ok(rev) = kv.put(ctx, "my-bucket", "my-key", <<"hello">>)
////
////   // Get a value
////   let assert Ok(value) = kv.get(ctx, "my-bucket", "my-key")
////
////   // List all keys in a bucket
////   let assert Ok(keys) = kv.list_keys(ctx, "my-bucket")
//// }
//// ```

import gleam/bit_array
import gleam/dict
import gleam/dynamic/decode.{type Decoder}
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import gleam/time/timestamp.{type Timestamp}
import guppy.{type NatsMessage, NatsMessage}

// ── Context ─────────────────────────────────────────────────────────────────

/// Opaque context holding a NATS connection and configuration for KV operations.
///
/// Created with `new_context` and optionally extended with `with_logger`.
pub opaque type KvContext {
  KvContext(
    conn: Subject(guppy.Message),
    log: fn(String) -> Nil,
    request_timeout: Int,
  )
}

/// Create a new KV context bound to an active NATS connection.
///
/// The context holds the connection subject, a no-op logger, and a default
/// request timeout of 5 seconds. Use `with_logger` to customise logging.
///
/// Parameters:
/// - `conn`: The NATS connection actor subject returned by `guppy.start`.
pub fn new_context(conn: Subject(guppy.Message)) -> KvContext {
  KvContext(conn, fn(_) -> Nil { Nil }, request_timeout: 5000)
}

/// Attach a logger function to a KV context.
///
/// The logger receives raw request/response payloads during API calls, useful
/// for debugging and tracing JetStream API interactions.
///
/// Parameters:
/// - `ctx`: The KV context to extend.
/// - `log`: A function called with each logged message string.
pub fn with_logger(ctx: KvContext, log: fn(String) -> Nil) -> KvContext {
  KvContext(..ctx, log:)
}

// ── Errors ──────────────────────────────────────────────────────────────────

/// Errors that can occur during KV operations.
pub type KvError {
  ConnectionError(guppy.NatsError)
  ResponseDecodeError(json.DecodeError, BitArray)
  ApiError(code: Int, description: String, err_code: Int)
  BucketNotFound
  BucketAlreadyExists
  KeyNotFound
  BadRevision
  BadKey(String)
}

@internal
pub type KvApiError {
  KvApiError(code: Int, description: String, err_code: Int)
}

// ── Bucket Types ────────────────────────────────────────────────────────────

/// Storage backend for a KV bucket.
pub type Storage {
  /// Messages are persisted to disk.
  File
  /// Messages are stored in memory only.
  Memory
}

/// Compression algorithm for a KV bucket.
pub type Compression {
  /// No compression applied.
  NoCompression
  /// S2 compression (Snappy-based).
  S2
}

/// Configuration for a KV bucket, mapped to a backing JetStream stream.
///
/// Use `default_bucket_config` for sensible defaults, then override fields
/// as needed with record update syntax.
pub type BucketConfig {
  BucketConfig(
    bucket_name: String,
    description: Option(String),
    history: Int,
    ttl: Int,
    max_value_size: Int,
    max_bucket_size: Int,
    storage: Storage,
    replicas: Int,
    compression: Option(Compression),
  )
}

/// Create a `BucketConfig` with sensible defaults.
///
/// Defaults: history=1, ttl=0, max_value_size=-1 (unlimited),
/// max_bucket_size=-1 (unlimited), storage=Memory, replicas=1,
/// compression=None.
///
/// Parameters:
/// - `bucket_name`: The name of the KV bucket (used as the JetStream stream
///   name prefix `KV_<name>`).
pub fn default_bucket_config(bucket_name: String) -> BucketConfig {
  BucketConfig(
    bucket_name:,
    description: None,
    history: 1,
    ttl: 0,
    max_value_size: -1,
    max_bucket_size: -1,
    storage: Memory,
    replicas: 1,
    compression: None,
  )
}

/// Runtime status of a KV bucket, including message counts and sequence numbers.
pub type BucketStatus {
  BucketStatus(
    bucket: String,
    messages: Int,
    bytes: Int,
    first_seq: Int,
    last_seq: Int,
    consumer_count: Int,
  )
}

// ── Entry Types ──────────────────────────────────────────────────────────────

/// The operation that produced a KV entry.
pub type Operation {
  /// A normal put operation.
  Put
  /// A delete marker was written.
  Delete
  /// A purge marker was written (all revisions removed).
  Purge
}

/// A single entry in a KV bucket, representing one revision of a key.
pub type KeyValueEntry {
  KeyValueEntry(
    bucket: String,
    key: String,
    value: BitArray,
    revision: Int,
    created: Timestamp,
    delta: Int,
    operation: Operation,
  )
}

// ── Key Validation ──────────────────────────────────────────────────────────

/// Validate a KV key according to NATS KV naming rules.
///
/// Rejects empty keys, keys starting or ending with `'.'`, and keys
/// containing spaces, `'*'`, or `'>'`. Returns `Ok(Nil)` for valid keys.
///
/// Parameters:
/// - `key`: The key string to validate.
@internal
pub fn validate_key(key: String) -> Result(Nil, String) {
  case key {
    "" -> Error("key must not be empty")
    _ -> {
      let starts_with_dot = string.starts_with(key, ".")
      let ends_with_dot = string.ends_with(key, ".")
      let has_invalid =
        string.contains(key, " ")
        || string.contains(key, "*")
        || string.contains(key, ">")
      case starts_with_dot {
        True -> Error("key must not start with '.'")
        False ->
          case ends_with_dot {
            True -> Error("key must not end with '.'")
            False ->
              case has_invalid {
                True -> Error("key must not contain spaces, '*', or '>'")
                False -> Ok(Nil)
              }
          }
      }
    }
  }
}

// ── Bucket API ───────────────────────────────────────────────────────────────

/// Create a new KV bucket backed by a JetStream stream.
///
/// Sends `$JS.API.STREAM.CREATE.KV_<name>` with the bucket config as the
/// stream configuration. Returns the created bucket status on success.
///
/// Parameters:
/// - `ctx`: The KV context.
/// - `config`: Bucket configuration (name, storage, history, etc.).
///
/// Errors: `BucketAlreadyExists` (10058), `ConnectionError`.
pub fn create_bucket(
  ctx: KvContext,
  config: BucketConfig,
) -> Result(BucketStatus, KvError) {
  use msg <- make_request(ctx, bucket_create_request(config))
  use decoded <- result.try(decode_response(ctx, msg, bucket_status_decoder()))
  result.map_error(decoded, map_api_error)
}

/// Delete a KV bucket and all of its data.
///
/// Sends `$JS.API.STREAM.DELETE.KV_<name>`. This is irreversible.
///
/// Parameters:
/// - `ctx`: The KV context.
/// - `bucket`: The name of the bucket to delete.
///
/// Errors: `BucketNotFound` (10059), `ConnectionError`.
pub fn delete_bucket(ctx: KvContext, bucket: String) -> Result(Nil, KvError) {
  use msg <- make_request(ctx, bucket_delete_request(bucket))
  case decode_response(ctx, msg, delete_response_decoder()) {
    Ok(_) -> Ok(Nil)
    Error(err) -> Error(err)
  }
}

/// Get information about a KV bucket.
///
/// Sends `$JS.API.STREAM.INFO.KV_<name>` and returns the current bucket
/// status including message count, byte count, and sequence numbers.
///
/// Parameters:
/// - `ctx`: The KV context.
/// - `bucket`: The name of the bucket to query.
///
/// Errors: `BucketNotFound` (10059), `ConnectionError`.
pub fn get_bucket_info(
  ctx: KvContext,
  bucket: String,
) -> Result(BucketStatus, KvError) {
  use msg <- make_request(ctx, bucket_info_request(bucket))
  use decoded <- result.try(decode_response(ctx, msg, bucket_status_decoder()))
  result.map_error(decoded, map_api_error)
}

/// List all KV bucket names visible to this NATS connection.
///
/// Sends `$JS.API.STREAM.NAMES` filtered to streams with the `KV_` prefix.
/// Non-KV streams are excluded from the result.
///
/// Parameters:
/// - `ctx`: The KV context.
///
/// Errors: `ConnectionError`.
pub fn list_bucket_names(ctx: KvContext) -> Result(List(String), KvError) {
  case
    guppy.request(ctx.conn, list_bucket_names_request(), ctx.request_timeout)
  {
    Ok(msg) ->
      case decode_response(ctx, msg, bucket_names_decoder()) {
        Ok(names) -> Ok(names)
        Error(err) -> Error(err)
      }
    Error(err) -> Error(ConnectionError(err))
  }
}

/// List all active keys in a KV bucket.
///
/// Fetches the stream info and extracts all unique subject names from
/// `state.subjects`. Returns the keys with the `$KV.<bucket>.` prefix stripped.
///
/// Note: this returns all subjects known to the stream, including keys that
/// have been deleted (delete markers remain as stream messages). Use `get_entry` to
/// verify a key still exists.
///
/// Parameters:
/// - `ctx`: The KV context.
/// - `bucket`: The bucket name.
///
/// Errors: `BucketNotFound` (10059), `ConnectionError`.
pub fn list_keys(
  ctx: KvContext,
  bucket: String,
) -> Result(List(String), KvError) {
  use msg <- make_request(ctx, bucket_info_with_subjects_request(bucket))
  use decoded <- result.try(decode_response(
    ctx,
    msg,
    bucket_keys_decoder(bucket),
  ))
  result.map_error(decoded, map_api_error)
}

// ── Entry API ────────────────────────────────────────────────────────────────

/// Insert or overwrite a key-value pair in a bucket.
///
/// Publishes the value to `$KV.<bucket>.<key>`. Returns the revision number
/// assigned by JetStream for this write.
///
/// Parameters:
/// - `ctx`: The KV context.
/// - `bucket`: The bucket name.
/// - `key`: The key (validated against NATS KV naming rules).
/// - `value`: The value as a `BitArray` (any binary data).
///
/// Errors: `BadKey`, `ConnectionError`.
pub fn put(
  ctx: KvContext,
  bucket: String,
  key: String,
  value: BitArray,
) -> Result(Int, KvError) {
  validate_key(key)
  |> result.map_error(BadKey)
  |> result.try(fn(_) {
    let subject = key_to_subject(bucket, key)
    let msg = NatsMessage(subject:, reply_to: None, headers: [], payload: value)
    use response <- make_request(ctx, msg)
    decode_revision_response(response.payload)
  })
}

/// Retrieve the latest value for a key.
///
/// Uses `$JS.API.STREAM.MSG.GET` with `last_by_subj` to fetch the most recent
/// message for the key subject. Returns `KeyNotFound` if the latest message
/// is a delete or purge marker.
///
/// Parameters:
/// - `ctx`: The KV context.
/// - `bucket`: The bucket name.
/// - `key`: The key.
///
/// Errors: `BadKey`, `KeyNotFound` (10037), `BucketNotFound` (10059),
/// `ConnectionError`.
pub fn get(
  ctx: KvContext,
  bucket: String,
  key: String,
) -> Result(BitArray, KvError) {
  result.map(get_entry(ctx, bucket, key), fn(entry) { entry.value })
}

/// Retrieve the latest entry (value plus metadata) for a key.
///
/// Uses `$JS.API.STREAM.MSG.GET` with `last_by_subj` to fetch the most recent
/// message for the key subject. Returns `KeyNotFound` if the latest message
/// is a delete or purge marker.
///
/// Parameters:
/// - `ctx`: The KV context.
/// - `bucket`: The bucket name.
/// - `key`: The key.
///
/// Errors: `BadKey`, `KeyNotFound` (10037), `BucketNotFound` (10059),
/// `ConnectionError`.
pub fn get_entry(
  ctx: KvContext,
  bucket: String,
  key: String,
) -> Result(KeyValueEntry, KvError) {
  validate_key(key)
  |> result.map_error(BadKey)
  |> result.try(fn(_) {
    let stream_name = bucket_to_stream_name(bucket)
    let subj = key_to_subject(bucket, key)
    let request = msg_get_by_subject_request(stream_name, subj)
    use msg <- make_request(ctx, request)
    use entry <- result.try(decode_entry_response(ctx, msg, bucket, key))
    case entry.operation {
      Delete | Purge -> Error(KeyNotFound)
      Put -> Ok(entry)
    }
  })
}

/// Retrieve a specific revision of a key by its sequence number.
///
/// Uses `$JS.API.STREAM.MSG.GET` with the sequence number (revision). This
/// requires `history` > 1 on the bucket, otherwise old revisions are
/// discarded.
///
/// Parameters:
/// - `ctx`: The KV context.
/// - `bucket`: The bucket name.
/// - `key`: The key.
/// - `revision`: The revision (JetStream sequence number) to fetch.
///
/// Errors: `BadKey`, `KeyNotFound` (10037), `BadRevision` (10071/10072),
/// `BucketNotFound` (10059), `ConnectionError`.
pub fn get_revision(
  ctx: KvContext,
  bucket: String,
  key: String,
  revision: Int,
) -> Result(KeyValueEntry, KvError) {
  validate_key(key)
  |> result.map_error(BadKey)
  |> result.try(fn(_) {
    let stream_name = bucket_to_stream_name(bucket)
    let request = msg_get_by_seq_request(stream_name, revision)
    use msg <- make_request(ctx, request)
    use entry <- result.try(decode_entry_response(ctx, msg, bucket, key))
    case entry.operation {
      Delete | Purge -> Error(KeyNotFound)
      Put -> Ok(entry)
    }
  })
}

/// Create a key only if it does not already exist.
///
/// Sets the `Nats-Expected-Last-Subject-Sequence` header to `"0"`, which
/// causes the publish to fail if the key already has any value. Returns the
/// revision of the newly created entry.
///
/// Parameters:
/// - `ctx`: The KV context.
/// - `bucket`: The bucket name.
/// - `key`: The key.
/// - `value`: The initial value.
///
/// Errors: `BadKey`, `BadRevision` (key already exists), `ConnectionError`.
pub fn create(
  ctx: KvContext,
  bucket: String,
  key: String,
  value: BitArray,
) -> Result(Int, KvError) {
  validate_key(key)
  |> result.map_error(BadKey)
  |> result.try(fn(_) {
    let subject = key_to_subject(bucket, key)
    let msg =
      NatsMessage(
        subject:,
        reply_to: None,
        headers: [#("Nats-Expected-Last-Subject-Sequence", "0")],
        payload: value,
      )
    use response <- make_request(ctx, msg)
    decode_revision_response(response.payload)
  })
}

/// Update a key only if it matches the expected revision.
///
/// Sets the `Nats-Expected-Last-Subject-Sequence` header to the provided
/// revision, performing a CAS (compare-and-swap). Returns the new revision.
///
/// Parameters:
/// - `ctx`: The KV context.
/// - `bucket`: The bucket name.
/// - `key`: The key.
/// - `value`: The new value.
/// - `revision`: The expected current revision; the update fails if the
///   latest revision differs.
///
/// Errors: `BadKey`, `BadRevision` (10071/10072), `ConnectionError`.
pub fn update(
  ctx: KvContext,
  bucket: String,
  key: String,
  value: BitArray,
  revision: Int,
) -> Result(Int, KvError) {
  validate_key(key)
  |> result.map_error(BadKey)
  |> result.try(fn(_) {
    let subject = key_to_subject(bucket, key)
    let msg =
      NatsMessage(
        subject:,
        reply_to: None,
        headers: [
          #("Nats-Expected-Last-Subject-Sequence", int.to_string(revision)),
        ],
        payload: value,
      )
    use response <- make_request(ctx, msg)
    decode_revision_response(response.payload)
  })
}

/// Mark a key as deleted by publishing a delete marker.
///
/// Publishes a zero-length message with `KV-Operation: DEL` header to the key
/// subject. Subsequent `get` calls will return `KeyNotFound`.
///
/// Parameters:
/// - `ctx`: The KV context.
/// - `bucket`: The bucket name.
/// - `key`: The key to delete.
///
/// Errors: `BadKey`, `ConnectionError`.
pub fn delete_key(
  ctx: KvContext,
  bucket: String,
  key: String,
) -> Result(Nil, KvError) {
  validate_key(key)
  |> result.map_error(BadKey)
  |> result.try(fn(_) {
    let subject = key_to_subject(bucket, key)
    let msg =
      NatsMessage(
        subject:,
        reply_to: None,
        headers: [#("KV-Operation", "DEL")],
        payload: <<>>,
      )
    case guppy.request(ctx.conn, msg, ctx.request_timeout) {
      Ok(_) -> Ok(Nil)
      Error(err) -> Error(ConnectionError(err))
    }
  })
}

/// Permanently remove all revisions of a key.
///
/// Publishes a purge marker (`KV-Operation: PURGE`), then sends a JetStream
/// stream purge request filtered to the key's subject. This removes all
/// historical revisions, not just the latest.
///
/// Parameters:
/// - `ctx`: The KV context.
/// - `bucket`: The bucket name.
/// - `key`: The key to purge.
///
/// Errors: `BadKey`, `ConnectionError`.
pub fn purge_key(
  ctx: KvContext,
  bucket: String,
  key: String,
) -> Result(Nil, KvError) {
  validate_key(key)
  |> result.map_error(BadKey)
  |> result.try(fn(_) {
    let subject = key_to_subject(bucket, key)
    let msg =
      NatsMessage(
        subject:,
        reply_to: None,
        headers: [#("KV-Operation", "PURGE")],
        payload: <<>>,
      )
    case guppy.request(ctx.conn, msg, ctx.request_timeout) {
      Ok(_) -> {
        let purge_request = stream_purge_request(bucket, subject)
        use response <- make_request(ctx, purge_request)
        case decode_response(ctx, response, purge_response_decoder()) {
          Ok(_) -> Ok(Nil)
          Error(err) -> Error(err)
        }
      }
      Error(err) -> Error(ConnectionError(err))
    }
  })
}

// ── Internal: Request Builders ───────────────────────────────────────────────

/// Convert a bucket name to the backing JetStream stream name (`KV_<bucket>`).
fn bucket_to_stream_name(bucket: String) -> String {
  "KV_" <> bucket
}

/// Convert a bucket name and key to a NATS subject (`$KV.<bucket>.<key>`).
fn key_to_subject(bucket: String, key: String) -> String {
  "$KV." <> bucket <> "." <> key
}

/// Build a JetStream stream create request for a KV bucket.
///
/// Subject: `$JS.API.STREAM.CREATE.KV_<name>`.
/// The payload includes retention=limits, discard=new, allow_direct=true,
/// and allow_rollup_hdrs=true as required by NATS KV.
///
/// Parameters:
/// - `config`: The bucket configuration.
@internal
pub fn bucket_create_request(config: BucketConfig) -> NatsMessage {
  let stream_name = bucket_to_stream_name(config.bucket_name)
  let subjects = ["$KV." <> config.bucket_name <> ".>"]
  NatsMessage(
    subject: "$JS.API.STREAM.CREATE." <> stream_name,
    reply_to: None,
    headers: [],
    payload: json_payload(json.object(
      [
        #("name", json.string(stream_name)),
        #("subjects", json.array(subjects, json.string)),
        #("max_msgs_per_subject", json.int(config.history)),
        #("max_age", json.int(config.ttl)),
        #("max_msg_size", json.int(config.max_value_size)),
        #("max_bytes", json.int(config.max_bucket_size)),
        #("retention", json.string("limits")),
        #("storage", json.string(storage_to_string(config.storage))),
        #("num_replicas", json.int(config.replicas)),
        #("discard", json.string("new")),
        #("allow_direct", json.bool(True)),
        #("allow_rollup_hdrs", json.bool(True)),
      ]
      |> optional_field("description", config.description, json.string)
      |> optional_field("compression", config.compression, fn(c) {
        case c {
          NoCompression -> json.string("none")
          S2 -> json.string("s2")
        }
      }),
    )),
  )
}

/// Build a JetStream stream delete request for a KV bucket.
///
/// Subject: `$JS.API.STREAM.DELETE.KV_<name>`.
///
/// Parameters:
/// - `bucket`: The bucket name (without `KV_` prefix).
@internal
pub fn bucket_delete_request(bucket: String) -> NatsMessage {
  let stream_name = bucket_to_stream_name(bucket)
  NatsMessage(
    subject: "$JS.API.STREAM.DELETE." <> stream_name,
    reply_to: None,
    headers: [],
    payload: bit_array.from_string(""),
  )
}

/// Build a JetStream stream info request for a KV bucket.
///
/// Subject: `$JS.API.STREAM.INFO.KV_<name>`.
///
/// Parameters:
/// - `bucket`: The bucket name (without `KV_` prefix).
@internal
pub fn bucket_info_request(bucket: String) -> NatsMessage {
  let stream_name = bucket_to_stream_name(bucket)
  NatsMessage(
    subject: "$JS.API.STREAM.INFO." <> stream_name,
    reply_to: None,
    headers: [],
    payload: bit_array.from_string(""),
  )
}

/// Build a JetStream stream info request that includes all subjects.
///
/// Subject: `$JS.API.STREAM.INFO.KV_<name>`.
/// Payload includes `{"subjects_filter": ""}` to request subject details in
/// `state.subjects`.
///
/// Parameters:
/// - `bucket`: The bucket name (without `KV_` prefix).
fn bucket_info_with_subjects_request(bucket: String) -> NatsMessage {
  let stream_name = bucket_to_stream_name(bucket)
  NatsMessage(
    subject: "$JS.API.STREAM.INFO." <> stream_name,
    reply_to: None,
    headers: [],
    payload: json_payload(json.object([#("subjects_filter", json.string(">"))])),
  )
}

/// Build a JetStream stream names request filtered to KV subjects.
///
/// Subject: `$JS.API.STREAM.NAMES`. Payload includes `{"subject": "$KV.>"}`
/// to only return streams matching the KV subject pattern.
@internal
pub fn list_bucket_names_request() -> NatsMessage {
  NatsMessage(
    subject: "$JS.API.STREAM.NAMES",
    reply_to: None,
    headers: [],
    payload: json_payload(json.object(
      [] |> optional_field("subject", Some("$KV.>"), json.string),
    )),
  )
}

/// Build a JetStream direct-get-by-subject request.
///
/// Subject: `$JS.API.STREAM.MSG.GET.<stream_name>`.
/// Payload: `{"last_by_subj": "<subject>"}`.
///
/// Parameters:
/// - `stream_name`: The JetStream stream name (`KV_<bucket>`).
/// - `subject`: The NATS subject within the stream to fetch the last message for.
fn msg_get_by_subject_request(
  stream_name: String,
  subject: String,
) -> NatsMessage {
  NatsMessage(
    subject: "$JS.API.STREAM.MSG.GET." <> stream_name,
    reply_to: None,
    headers: [],
    payload: json_payload(
      json.object([
        #("last_by_subj", json.string(subject)),
      ]),
    ),
  )
}

/// Build a JetStream direct-get-by-sequence request.
///
/// Subject: `$JS.API.STREAM.MSG.GET.<stream_name>`.
/// Payload: `{"seq": <sequence>}`.
///
/// Parameters:
/// - `stream_name`: The JetStream stream name (`KV_<bucket>`).
/// - `seq`: The sequence number (revision) to fetch.
fn msg_get_by_seq_request(stream_name: String, seq: Int) -> NatsMessage {
  NatsMessage(
    subject: "$JS.API.STREAM.MSG.GET." <> stream_name,
    reply_to: None,
    headers: [],
    payload: json_payload(json.object([#("seq", json.int(seq))])),
  )
}

/// Build a JetStream stream purge request filtered to a specific subject.
///
/// Subject: `$JS.API.STREAM.PURGE.<stream_name>`.
/// Payload: `{"filter": "<subject>"}`.
///
/// Parameters:
/// - `bucket`: The bucket name (used to derive the stream name).
/// - `filter`: The subject filter — only messages matching this subject are
///   purged.
fn stream_purge_request(bucket: String, filter: String) -> NatsMessage {
  let stream_name = bucket_to_stream_name(bucket)
  NatsMessage(
    subject: "$JS.API.STREAM.PURGE." <> stream_name,
    reply_to: None,
    headers: [],
    payload: json_payload(json.object([#("filter", json.string(filter))])),
  )
}

// ── Internal: Decoders ────────────────────────────────────────────────────────

/// Decode a JetStream `stream_info_response` into `BucketStatus`.
///
/// Extracts `config.name` (strips `KV_` prefix), `state.messages`,
/// `state.bytes`, `state.first_seq`, `state.last_seq`, and
/// `state.consumer_count`. Returns `Error(KvApiError)` if the response
/// contains a top-level `error` field.
@internal
pub fn bucket_status_decoder() -> Decoder(Result(BucketStatus, KvApiError)) {
  use <- decode_api_error()
  use name <- decode.subfield(["config", "name"], decode.string)
  use messages <- decode.subfield(["state", "messages"], decode.int)
  use bytes <- decode.subfield(["state", "bytes"], decode.int)
  use first_seq <- decode.subfield(["state", "first_seq"], decode.int)
  use last_seq <- decode.subfield(["state", "last_seq"], decode.int)
  use consumer_count <- decode.subfield(["state", "consumer_count"], decode.int)
  let bucket = case string.starts_with(name, "KV_") {
    True -> string.slice(name, 3, string.length(name) - 3)
    False -> name
  }
  decode.success(
    Ok(BucketStatus(
      bucket:,
      messages:,
      bytes:,
      first_seq:,
      last_seq:,
      consumer_count:,
    )),
  )
}

/// Decode a JetStream `stream_names_response` into a list of bucket names.
///
/// Filters to streams with the `KV_` prefix and strips the prefix.
/// Returns an empty list if the `streams` field is absent or null.
@internal
pub fn bucket_names_decoder() -> Decoder(List(String)) {
  use names <- decode.optional_field(
    "streams",
    None,
    decode.optional(decode.list(decode.string)),
  )
  let names = option.unwrap(names, [])
  decode.success(
    names
    |> list.filter(fn(name) { string.starts_with(name, "KV_") })
    |> list.map(fn(name) { string.slice(name, 3, string.length(name) - 3) }),
  )
}

/// Decode `state.subjects` from a stream info response into a list of key
/// names by stripping the `$KV.<bucket>.` subject prefix.
///
/// Parameters:
/// - `bucket`: The bucket name used to build the subject prefix filter.
@internal
pub fn bucket_keys_decoder(
  bucket: String,
) -> Decoder(Result(List(String), KvApiError)) {
  use <- decode_api_error()
  let prefix = "$KV." <> bucket <> "."
  decode.then(
    decode.at(
      ["state"],
      decode.optionally_at(
        ["subjects"],
        None,
        decode.optional(decode.dict(decode.string, decode.dynamic)),
      ),
    ),
    fn(subjects) {
      let keys =
        subjects
        |> option.unwrap(dict.new())
        |> dict.keys()
        |> list.filter(fn(s) { string.starts_with(s, prefix) })
        |> list.map(fn(s) { string.drop_start(s, string.length(prefix)) })
      decode.success(Ok(keys))
    },
  )
}

/// Decode a JetStream delete/response into `Result(Nil, KvApiError)`.
///
/// Expects `{"success": true}` on success, or an `error` field on failure.
@internal
pub fn delete_response_decoder() -> Decoder(Result(Nil, KvApiError)) {
  use <- decode_api_error()
  use _success <- decode.optional_field("success", True, decode.bool)
  decode.success(Ok(Nil))
}

/// Decode a KV entry from its raw JetStream message representation.
///
/// Fields: `seq` (revision), `data` (base64-decoded value, defaults to empty),
/// `time` (RFC 3339 timestamp), `hdrs` (base64-encoded NATS headers for
///  operation detection: DEL/PURGE/Put).
///
/// Parameters:
/// - `bucket`: The bucket name to attach to the decoded entry.
/// - `key`: The key name to attach to the decoded entry.
@internal
pub fn entry_decoder(
  bucket: String,
  key: String,
) -> Decoder(Result(KeyValueEntry, KvApiError)) {
  use <- decode_api_error()
  use seq <- decode.field("seq", decode.int)
  use data <- decode.optional_field("data", <<>>, base64_data_decoder())
  use time <- decode.field("time", decode_timestamp())
  use operation <- decode.optional_field("hdrs", Put, decode_headers_decoder())
  decode.success(
    Ok(KeyValueEntry(
      bucket:,
      key:,
      value: data,
      revision: seq,
      created: time,
      delta: 0,
      operation:,
    )),
  )
}

/// Decode a KV entry from the `$JS.API.STREAM.MSG.GET` response format.
///
/// The MSG.GET response wraps the entry fields inside a `"message"` envelope.
/// This decoder unwraps it and delegates to `entry_decoder` for the inner
/// fields.
///
/// Parameters:
/// - `bucket`: The bucket name for the decoded entry.
/// - `key`: The key name for the decoded entry.
@internal
pub fn direct_get_entry_decoder(
  bucket: String,
  key: String,
) -> Decoder(Result(KeyValueEntry, KvApiError)) {
  use <- decode_api_error()
  decode.field("message", entry_decoder(bucket, key), fn(result) {
    decode.success(result)
  })
}

fn base64_data_decoder() -> Decoder(BitArray) {
  decode.then(decode.string, fn(encoded) {
    case bit_array.base64_decode(encoded) {
      Ok(data) -> decode.success(data)
      Error(Nil) -> decode.failure(<<>>, "base64 data")
    }
  })
}

fn decode_headers_decoder() -> Decoder(Operation) {
  decode.then(decode.string, fn(encoded) {
    case bit_array.base64_decode(encoded) {
      Ok(headers_bits) ->
        case bit_array.to_string(headers_bits) {
          Ok(headers_string) ->
            case
              string.contains(headers_string, "KV-Operation: DEL")
              || string.contains(headers_string, "Nats-KV-Operation: DEL")
            {
              True -> decode.success(Delete)
              False ->
                case
                  string.contains(headers_string, "KV-Operation: PURGE")
                  || string.contains(headers_string, "Nats-KV-Operation: PURGE")
                {
                  True -> decode.success(Purge)
                  False -> decode.success(Put)
                }
            }
          Error(Nil) -> decode.success(Put)
        }
      Error(Nil) -> decode.success(Put)
    }
  })
}

fn purge_response_decoder() -> Decoder(Result(Nil, KvApiError)) {
  use <- decode_api_error()
  use _success <- decode.optional_field("success", True, decode.bool)
  decode.success(Ok(Nil))
}

fn revision_decoder() -> Decoder(Result(Int, KvApiError)) {
  use <- decode_api_error()
  use seq <- decode.field("seq", decode.int)
  decode.success(Ok(seq))
}

// ── Internal: Helpers ─────────────────────────────────────────────────────────

fn make_request(
  ctx: KvContext,
  msg: NatsMessage,
  next: fn(NatsMessage) -> Result(a, KvError),
) -> Result(a, KvError) {
  case guppy.request(ctx.conn, msg, ctx.request_timeout) {
    Ok(response) -> next(response)
    Error(err) -> Error(ConnectionError(err))
  }
}

fn decode_response(
  ctx: KvContext,
  msg: NatsMessage,
  decoder: Decoder(a),
) -> Result(a, KvError) {
  ctx.log(msg |> string.inspect)
  ctx.log(msg.payload |> bit_array.to_string |> result.unwrap("<<binary>>"))
  json.parse_bits(msg.payload, decoder)
  |> result.map_error(ResponseDecodeError(_, msg.payload))
}

fn decode_revision_response(payload: BitArray) -> Result(Int, KvError) {
  case json.parse_bits(payload, revision_decoder()) {
    Ok(Ok(seq)) -> Ok(seq)
    Ok(Error(err)) -> Error(map_api_error(err))
    Error(decode_err) -> Error(ResponseDecodeError(decode_err, payload))
  }
}

fn decode_entry_response(
  ctx: KvContext,
  msg: NatsMessage,
  bucket: String,
  key: String,
) -> Result(KeyValueEntry, KvError) {
  use entry <- result.try(decode_response(
    ctx,
    msg,
    direct_get_entry_decoder(bucket, key),
  ))
  result.map_error(entry, map_api_error)
}

fn map_api_error(err: KvApiError) -> KvError {
  case err.err_code {
    10_059 -> BucketNotFound
    10_058 -> BucketAlreadyExists
    10_037 -> KeyNotFound
    10_071 -> BadRevision
    10_072 -> BadRevision
    _ ->
      ApiError(
        code: err.code,
        description: err.description,
        err_code: err.err_code,
      )
  }
}

fn decode_api_error(
  callback: fn() -> Decoder(Result(a, KvApiError)),
) -> Decoder(Result(a, KvApiError)) {
  use error <- decode.optional_field("error", None, {
    use code <- decode.field("code", decode.int)
    use description <- decode.optional_field("description", "", decode.string)
    use err_code <- decode.optional_field("err_code", -1, decode.int)
    decode.success(Some(KvApiError(code, description, err_code)))
  })
  case error {
    Some(error) -> decode.success(Error(error))
    None -> callback()
  }
}

fn decode_timestamp() -> Decoder(Timestamp) {
  decode.then(decode.string, fn(ts_string) {
    case timestamp.parse_rfc3339(ts_string) {
      Ok(ts) -> decode.success(ts)
      Error(_) -> decode.failure(timestamp.from_unix_seconds(0), "Timestamp")
    }
  })
}

fn storage_to_string(storage: Storage) -> String {
  case storage {
    File -> "file"
    Memory -> "memory"
  }
}

fn optional_field(
  options: List(#(b, json.Json)),
  key: b,
  value: Option(a),
  mapper: fn(a) -> json.Json,
) -> List(#(b, json.Json)) {
  case value {
    Some(value) -> [#(key, mapper(value)), ..options]
    None -> options
  }
}

fn json_payload(json: json.Json) -> BitArray {
  json.to_string(json)
  |> bit_array.from_string
}
