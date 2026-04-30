import gleam/bit_array
import gleam/erlang/process
import gleam/list
import gleam/option.{None}
import gleam/string
import gleam/time/duration
import gleeunit/should
import nuts as nats
import nuts/internal/object_store as obj
import nuts/internal/stream
import nuts/object_store
import nuts/test_utils

pub fn create_bucket_test() {
  use port <- test_utils.with_nats_server()
  let options = nats.new("127.0.0.1", port)
  let name = process.new_name("nats-object-store-test")
  let conn = process.named_subject(name)

  let assert Ok(_) = nats.start(name, options)
  assert test_utils.await_connected(conn, 5)

  let config =
    obj.ObjectStoreConfig(
      bucket: "test_bucket",
      description: None,
      storage: stream.Memory,
      ttl: duration.milliseconds(0),
      max_bytes: -1,
      replicas: 1,
      compression: False,
    )

  let assert Ok(created) = object_store.create_bucket(conn, config)
  created.bucket |> should.equal("test_bucket")
}

pub fn put_and_get_object_test() {
  use port <- test_utils.with_nats_server()
  let options = nats.new("127.0.0.1", port)
  let name = process.new_name("nats-object-store-test")
  let conn = process.named_subject(name)

  let assert Ok(_) = nats.start(name, options)
  assert test_utils.await_connected(conn, 5)

  let config =
    obj.ObjectStoreConfig(
      bucket: "test_bucket",
      description: None,
      storage: stream.Memory,
      ttl: duration.milliseconds(0),
      max_bytes: -1,
      replicas: 1,
      compression: False,
    )

  let assert Ok(_) = object_store.create_bucket(conn, config)

  let data = bit_array.from_string("hello world from object store")
  let assert Ok(info) = object_store.put(conn, "test_bucket", "my_object", data)
  info.name |> should.equal("my_object")
  info.size |> should.equal(29)

  let assert Ok(result) = object_store.get(conn, "test_bucket", "my_object")
  let assert Ok(result_str) = bit_array.to_string(result)
  result_str |> should.equal("hello world from object store")
}

pub fn put_large_object_test() {
  use port <- test_utils.with_nats_server()
  let options = nats.new("127.0.0.1", port)
  let name = process.new_name("nats-object-store-test")
  let conn = process.named_subject(name)

  let assert Ok(_) = nats.start(name, options)
  assert test_utils.await_connected(conn, 5)

  let config =
    obj.ObjectStoreConfig(
      bucket: "test_bucket",
      description: None,
      storage: stream.Memory,
      ttl: duration.milliseconds(0),
      max_bytes: -1,
      replicas: 1,
      compression: False,
    )

  let assert Ok(_) = object_store.create_bucket(conn, config)

  let large_data =
    string.repeat("a", 200_000)
    |> bit_array.from_string

  let assert Ok(info) =
    object_store.put(conn, "test_bucket", "large_obj", large_data)
  should.be_true(info.chunks > 1)

  let assert Ok(result) = object_store.get(conn, "test_bucket", "large_obj")
  result |> should.equal(large_data)
}

pub fn put_memory_storage_test() {
  use port <- test_utils.with_nats_server()
  let options = nats.new("127.0.0.1", port)
  let name = process.new_name("nats-object-store-test")
  let conn = process.named_subject(name)

  let assert Ok(_) = nats.start(name, options)
  assert test_utils.await_connected(conn, 5)

  let config =
    obj.ObjectStoreConfig(
      bucket: "memory_bucket",
      description: None,
      storage: stream.Memory,
      ttl: duration.milliseconds(0),
      max_bytes: -1,
      replicas: 1,
      compression: False,
    )

  let assert Ok(_) = object_store.create_bucket(conn, config)

  let data = bit_array.from_string("in-memory object")
  let assert Ok(info) = object_store.put(conn, "memory_bucket", "mem_obj", data)
  info.name |> should.equal("mem_obj")

  let assert Ok(result) = object_store.get(conn, "memory_bucket", "mem_obj")
  let assert Ok(result_str) = bit_array.to_string(result)
  result_str |> should.equal("in-memory object")
}

pub fn list_objects_test() {
  use port <- test_utils.with_nats_server()
  let options = nats.new("127.0.0.1", port)
  let name = process.new_name("nats-list-test")
  let conn = process.named_subject(name)

  let assert Ok(_) = nats.start(name, options)
  assert test_utils.await_connected(conn, 5)

  let config =
    obj.ObjectStoreConfig(
      bucket: "list_bucket",
      description: None,
      storage: stream.Memory,
      ttl: duration.hours(1),
      max_bytes: -1,
      replicas: 1,
      compression: False,
    )

  let assert Ok(_) = object_store.create_bucket(conn, config)

  let assert Ok(_) =
    object_store.put(
      conn,
      "list_bucket",
      "file_a.txt",
      bit_array.from_string("content a"),
    )
  let assert Ok(_) =
    object_store.put(
      conn,
      "list_bucket",
      "file_b.txt",
      bit_array.from_string("content b"),
    )
  let assert Ok(_) =
    object_store.put(
      conn,
      "list_bucket",
      "file_c.txt",
      bit_array.from_string("content c"),
    )

  let assert Ok(objects) = object_store.list(conn, "list_bucket")
  list.length(objects) |> should.equal(3)

  let names =
    objects
    |> list.map(fn(info) { info.name })
    |> list.sort(by: string.compare)
  names |> should.equal(["file_a.txt", "file_b.txt", "file_c.txt"])
}

pub fn list_empty_bucket_test() {
  use port <- test_utils.with_nats_server()
  let options = nats.new("127.0.0.1", port)
  let name = process.new_name("nats-list-empty-test")
  let conn = process.named_subject(name)

  let assert Ok(_) = nats.start(name, options)
  assert test_utils.await_connected(conn, 5)

  let config =
    obj.ObjectStoreConfig(
      bucket: "empty_bucket",
      description: None,
      storage: stream.Memory,
      ttl: duration.hours(1),
      max_bytes: -1,
      replicas: 1,
      compression: False,
    )

  let assert Ok(_) = object_store.create_bucket(conn, config)

  let assert Ok(objects) = object_store.list(conn, "empty_bucket")
  list.length(objects) |> should.equal(0)
}
