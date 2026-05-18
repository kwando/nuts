import gleam/bit_array
import gleam/dynamic/decode
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import guppy as nats
import guppy/kv
import guppy/test_utils
import simplifile

// ── Unit tests: request builders ──────────────────────────────────────────────

pub fn bucket_create_request_subject_test() {
  let config = kv.default_bucket_config("my-bucket")
  let msg = kv.bucket_create_request(config)
  assert msg.subject == "$JS.API.STREAM.CREATE.KV_my-bucket"
}

pub fn bucket_create_request_payload_test() {
  let config = kv.default_bucket_config("test-bucket")
  let msg = kv.bucket_create_request(config)
  let assert Ok(str) = bit_array.to_string(msg.payload)
  let assert Ok(_decoded) = json.parse(str, bucket_config_decoder())
}

pub fn bucket_create_request_with_options_test() {
  let config =
    kv.BucketConfig(
      ..kv.default_bucket_config("my-bucket"),
      description: Some("test description"),
      history: 10,
      ttl: 60_000_000_000,
      max_value_size: 1024,
      max_bucket_size: 1_048_576,
      storage: kv.File,
      replicas: 3,
    )
  let msg = kv.bucket_create_request(config)
  let assert Ok(str) = bit_array.to_string(msg.payload)
  let assert Ok(_decoded) = json.parse(str, bucket_config_decoder())
}

pub fn bucket_create_request_compression_s2_test() {
  let config =
    kv.BucketConfig(
      ..kv.default_bucket_config("test"),
      compression: Some(kv.S2),
    )
  let msg = kv.bucket_create_request(config)
  let assert Ok(str) = bit_array.to_string(msg.payload)
  let assert Ok(_decoded) = json.parse(str, bucket_config_decoder())
}

pub fn bucket_delete_request_subject_test() {
  let msg = kv.bucket_delete_request("my-bucket")
  assert msg.subject == "$JS.API.STREAM.DELETE.KV_my-bucket"
}

pub fn bucket_info_request_subject_test() {
  let msg = kv.bucket_info_request("my-bucket")
  assert msg.subject == "$JS.API.STREAM.INFO.KV_my-bucket"
}

pub fn list_bucket_names_request_subject_test() {
  let msg = kv.list_bucket_names_request()
  assert msg.subject == "$JS.API.STREAM.NAMES"
}

// ── Unit tests: decoders ──────────────────────────────────────────────────────

pub fn bucket_status_decoder_test() {
  let assert Ok(payload) =
    simplifile.read_bits("test/fixtures/stream_info_response.json")
  let assert Ok(result) = json.parse_bits(payload, kv.bucket_status_decoder())
  let assert Ok(status) = result
  assert status.bucket == "my_stream"
  assert status.messages == 0
  assert status.consumer_count == 0
}

pub fn bucket_status_decoder_error_test() {
  let payload =
    bit_array.from_string(
      "{\"error\":{\"code\":404,\"description\":\"stream not found\",\"err_code\":10059}}",
    )
  let assert Ok(result) = json.parse_bits(payload, kv.bucket_status_decoder())
  let assert Error(err) = result
  assert kv.KvApiError(
      code: 404,
      description: "stream not found",
      err_code: 10_059,
    )
    == err
}

pub fn bucket_names_decoder_test() {
  let payload =
    bit_array.from_string(
      "{\"total\":2,\"offset\":0,\"limit\":256,\"streams\":[\"KV_bucket1\",\"KV_bucket2\",\"OTHER_STREAM\"]}",
    )
  let assert Ok(names) = json.parse_bits(payload, kv.bucket_names_decoder())
  assert names == ["bucket1", "bucket2"]
}

pub fn bucket_names_decoder_empty_test() {
  let payload =
    bit_array.from_string("{\"total\":0,\"offset\":0,\"limit\":256}")
  let assert Ok(names) = json.parse_bits(payload, kv.bucket_names_decoder())
  assert names == []
}

pub fn delete_response_decoder_success_test() {
  let payload = bit_array.from_string("{\"success\":true}")
  let assert Ok(result) = json.parse_bits(payload, kv.delete_response_decoder())
  let assert Ok(_) = result
}

pub fn delete_response_decoder_error_test() {
  let payload =
    bit_array.from_string(
      "{\"error\":{\"code\":404,\"description\":\"stream not found\",\"err_code\":10059}}",
    )
  let assert Ok(result) = json.parse_bits(payload, kv.delete_response_decoder())
  let assert Error(err) = result
  assert kv.KvApiError(
      code: 404,
      description: "stream not found",
      err_code: 10_059,
    )
    == err
}

pub fn entry_decoder_put_test() {
  let payload =
    bit_array.from_string(
      "{\"seq\":5,\"data\":\"aGVsbG8=\",\"time\":\"2024-01-15T10:30:00.000000Z\"}",
    )
  let assert Ok(result) =
    json.parse_bits(payload, kv.entry_decoder("test-bucket", "my-key"))
  let assert Ok(entry) = result
  assert entry.bucket == "test-bucket"
  assert entry.key == "my-key"
  assert entry.value == <<"hello">>
  assert entry.revision == 5
  assert entry.operation == kv.Put
}

pub fn entry_decoder_no_data_test() {
  let payload =
    bit_array.from_string(
      "{\"seq\":3,\"time\":\"2024-01-15T10:30:00.000000Z\"}",
    )
  let assert Ok(result) =
    json.parse_bits(payload, kv.entry_decoder("test-bucket", "my-key"))
  let assert Ok(entry) = result
  assert entry.value == <<>>
  assert entry.operation == kv.Put
}

pub fn entry_decoder_with_delete_hdrs_test() {
  let hdrs_base64 =
    bit_array.base64_encode(<<"NATS/1.0\r\nKV-Operation: DEL\r\n\r\n">>, False)
  let payload_str =
    "{\"seq\":3,\"data\":\"\",\"time\":\"2024-01-15T10:30:00.000000Z\",\"hdrs\":\""
    <> hdrs_base64
    <> "\"}"
  let payload = bit_array.from_string(payload_str)
  let assert Ok(result) =
    json.parse_bits(payload, kv.entry_decoder("test-bucket", "my-key"))
  let assert Ok(entry) = result
  assert entry.operation == kv.Delete
}

pub fn entry_decoder_with_purge_hdrs_test() {
  let hdrs_base64 =
    bit_array.base64_encode(
      <<"NATS/1.0\r\nKV-Operation: PURGE\r\n\r\n">>,
      False,
    )
  let payload_str =
    "{\"seq\":3,\"data\":\"\",\"time\":\"2024-01-15T10:30:00.000000Z\",\"hdrs\":\""
    <> hdrs_base64
    <> "\"}"
  let payload = bit_array.from_string(payload_str)
  let assert Ok(result) =
    json.parse_bits(payload, kv.entry_decoder("test-bucket", "my-key"))
  let assert Ok(entry) = result
  assert entry.operation == kv.Purge
}

pub fn bucket_keys_decoder_test() {
  let payload =
    bit_array.from_string(
      "{\"state\":{\"subjects\":{\"$KV.my_bucket.key1\":{},\"$KV.my_bucket.key2\":{}}}}",
    )
  let assert Ok(result) =
    json.parse_bits(payload, kv.bucket_keys_decoder("my_bucket"))
  let assert Ok(keys) = result
  assert keys == ["key1", "key2"]
}

pub fn bucket_keys_decoder_empty_state_test() {
  let payload = bit_array.from_string("{\"state\":{}}")
  let assert Ok(result) =
    json.parse_bits(payload, kv.bucket_keys_decoder("my_bucket"))
  let assert Ok(keys) = result
  assert keys == []
}

pub fn bucket_keys_decoder_empty_subjects_test() {
  let payload = bit_array.from_string("{\"state\":{\"subjects\":{}}}")
  let assert Ok(result) =
    json.parse_bits(payload, kv.bucket_keys_decoder("my_bucket"))
  let assert Ok(keys) = result
  assert keys == []
}

pub fn entry_decoder_error_test() {
  let payload =
    bit_array.from_string(
      "{\"error\":{\"code\":404,\"description\":\"message not found\",\"err_code\":10037}}",
    )
  let assert Ok(result) =
    json.parse_bits(payload, kv.entry_decoder("test-bucket", "my-key"))
  let assert Error(err) = result
  assert kv.KvApiError(
      code: 404,
      description: "message not found",
      err_code: 10_037,
    )
    == err
}

// ── Unit tests: key validation ────────────────────────────────────────────────

pub fn validate_key_empty_test() {
  assert kv.validate_key("") == Error("key must not be empty")
}

pub fn validate_key_starts_with_dot_test() {
  assert kv.validate_key(".foo") == Error("key must not start with '.'")
}

pub fn validate_key_ends_with_dot_test() {
  assert kv.validate_key("foo.") == Error("key must not end with '.'")
}

pub fn validate_key_contains_space_test() {
  assert kv.validate_key("foo bar")
    == Error("key must not contain spaces, '*', or '>'")
}

pub fn validate_key_contains_wildcard_test() {
  assert kv.validate_key("foo.*")
    == Error("key must not contain spaces, '*', or '>'")
}

pub fn validate_key_contains_gt_test() {
  assert kv.validate_key("foo.>")
    == Error("key must not contain spaces, '*', or '>'")
}

pub fn validate_key_valid_test() {
  assert kv.validate_key("foo.bar") == Ok(Nil)
}

pub fn validate_key_valid_simple_test() {
  assert kv.validate_key("my-key") == Ok(Nil)
}

// ── Unit tests: defaults ──────────────────────────────────────────────────────

pub fn default_bucket_config_test() {
  let config = kv.default_bucket_config("test")
  assert config.bucket_name == "test"
  assert config.history == 1
  assert config.ttl == 0
  assert config.max_value_size == -1
  assert config.max_bucket_size == -1
  assert config.storage == kv.Memory
  assert config.replicas == 1
  assert config.compression == None
}

// ── Integration tests (require NATS on 127.0.0.1:6789) ───────────────────────

pub fn bucket_crud_test() {
  let assert Ok(actor.Started(_, conn)) =
    nats.new("127.0.0.1", 6789)
    |> nats.start()
  assert test_utils.await_connected(conn, 100)
  let ctx = kv.new_context(conn)

  // List should work (empty or not)
  let assert Ok(_) = kv.list_bucket_names(ctx)

  // Create a bucket
  let assert Ok(bucket) =
    kv.create_bucket(ctx, kv.default_bucket_config("test_bucket"))
  assert bucket.bucket == "test_bucket"

  // Get bucket info
  let assert Ok(info) = kv.get_bucket_info(ctx, "test_bucket")
  assert info.bucket == "test_bucket"

  // Delete the bucket
  let assert Ok(_) = kv.delete_bucket(ctx, "test_bucket")

  // Bucket should be gone
  let assert Error(kv.BucketNotFound) = kv.get_bucket_info(ctx, "test_bucket")

  nats.shutdown(conn)
}

pub fn list_keys_test() {
  let assert Ok(actor.Started(_, conn)) =
    nats.new("127.0.0.1", 6789)
    |> nats.start()
  assert test_utils.await_connected(conn, 100)
  let ctx = kv.new_context(conn)

  // Delete any leftover bucket so we start fresh
  let _ = kv.delete_bucket(ctx, "list_keys_bucket")

  let config =
    kv.BucketConfig(..kv.default_bucket_config("list_keys_bucket"), history: 5)
  let assert Ok(_) = kv.create_bucket(ctx, config)

  // No keys yet
  let assert Ok(keys) = kv.list_keys(ctx, "list_keys_bucket")
  assert keys == []

  // Put a few keys
  let assert Ok(_) = kv.put(ctx, "list_keys_bucket", "alpha", <<"1">>)
  let assert Ok(_) = kv.put(ctx, "list_keys_bucket", "beta", <<"2">>)
  let assert Ok(_) = kv.put(ctx, "list_keys_bucket", "gamma", <<"3">>)

  let assert Ok(keys) = kv.list_keys(ctx, "list_keys_bucket")
  assert list.contains(keys, "alpha")
  assert list.contains(keys, "beta")
  assert list.contains(keys, "gamma")

  // Delete a key — the subject still appears in stream subjects (the
  // delete marker is a message), so list_keys still includes it.
  let assert Ok(_) = kv.delete_key(ctx, "list_keys_bucket", "beta")

  let assert Ok(keys) = kv.list_keys(ctx, "list_keys_bucket")
  assert list.contains(keys, "alpha")
  assert list.contains(keys, "beta")
  assert list.contains(keys, "gamma")

  let assert Ok(_) = kv.delete_bucket(ctx, "list_keys_bucket")
  nats.shutdown(conn)
}

pub fn bucket_not_found_test() {
  let assert Ok(actor.Started(_, conn)) =
    nats.new("127.0.0.1", 6789)
    |> nats.start()
  assert test_utils.await_connected(conn, 100)
  let ctx = kv.new_context(conn)

  let assert Error(kv.BucketNotFound) =
    kv.get_bucket_info(ctx, "nonexistent_bucket")

  nats.shutdown(conn)
}

// ── Helper types and decoders ────────────────────────────────────────────────

pub type DecodedBucketConfig {
  DecodedBucketConfig(
    name: String,
    subjects: List(String),
    max_msgs_per_subject: Int,
    max_age: Int,
    max_msg_size: Int,
    max_bytes: Int,
    storage: String,
    num_replicas: Int,
    discard: String,
    allow_direct: Bool,
    allow_rollup_hdrs: Bool,
    description: Option(String),
    compression: Option(String),
  )
}

fn bucket_config_decoder() -> decode.Decoder(DecodedBucketConfig) {
  use name <- decode.field("name", decode.string)
  use subjects <- decode.field("subjects", decode.list(decode.string))
  use max_msgs_per_subject <- decode.field("max_msgs_per_subject", decode.int)
  use max_age <- decode.field("max_age", decode.int)
  use max_msg_size <- decode.field("max_msg_size", decode.int)
  use max_bytes <- decode.field("max_bytes", decode.int)
  use storage <- decode.field("storage", decode.string)
  use num_replicas <- decode.field("num_replicas", decode.int)
  use discard <- decode.field("discard", decode.string)
  use allow_direct <- decode.field("allow_direct", decode.bool)
  use allow_rollup_hdrs <- decode.field("allow_rollup_hdrs", decode.bool)
  use description <- decode.optional_field(
    "description",
    None,
    decode.optional(decode.string),
  )
  use compression <- decode.optional_field(
    "compression",
    None,
    decode.optional(decode.string),
  )
  decode.success(DecodedBucketConfig(
    name:,
    subjects:,
    max_msgs_per_subject:,
    max_age:,
    max_msg_size:,
    max_bytes:,
    storage:,
    num_replicas:,
    discard:,
    allow_direct:,
    allow_rollup_hdrs:,
    description:,
    compression:,
  ))
}
