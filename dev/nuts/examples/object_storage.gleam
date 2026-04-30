import gleam/bit_array
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{Some}
import gleam/string
import gleam/time/duration
import gleam_community/ansi
import nuts
import nuts/internal/object_store as object_store_internal
import nuts/internal/stream
import nuts/object_store
import simplifile

pub fn cyan_logger(msg) {
  io.println_error(msg |> ansi.cyan)
}

pub fn main() {
  let nuts_name = process.new_name("nuts")

  let assert Ok(_) =
    nuts.new("100.121.244.19", 4222)
    |> nuts.with_logger(nuts.Logger(
      info: cyan_logger,
      debug: cyan_logger,
      warning: cyan_logger,
    ))
    |> nuts.start(nuts_name, _)
  let nats_conn = process.named_subject(nuts_name)
  process.sleep(1000)

  let bucket_name = "filez"

  io.println(ansi.yellow("=== Creating object store bucket ==="))
  let bucket_config =
    object_store_internal.ObjectStoreConfig(
      bucket: bucket_name,
      description: Some("Example object store bucket"),
      storage: stream.Memory,
      ttl: duration.hours(24),
      max_bytes: 1024 * 1024 * 32,
      replicas: 1,
      compression: False,
    )

  case object_store.create_bucket(nats_conn, bucket_config) {
    Ok(config) -> io.println(ansi.green("Bucket created: " <> config.bucket))
    Error(err) ->
      io.println(ansi.red("Bucket creation failed: " <> error_to_string(err)))
  }

  io.println(ansi.yellow("\n=== Putting an object ==="))
  let object_name = "greeting.txt"
  let object_data =
    "Hello from the NUTS object store!"
    |> bit_array.from_string

  case object_store.put(nats_conn, bucket_name, object_name, object_data) {
    Ok(info) ->
      io.println(ansi.green(
        "Object stored: "
        <> info.name
        <> " ("
        <> int.to_string(info.size)
        <> " bytes, "
        <> int.to_string(info.chunks)
        <> " chunks, digest: "
        <> info.digest
        <> ")",
      ))
    Error(err) -> io.println(ansi.red("Put failed: " <> error_to_string(err)))
  }

  io.println(ansi.yellow("\n=== Putting a larger object ==="))
  let large_name = "data.bin"
  let large_data = list.repeat(<<0xAA>>, 200_000) |> bit_array.concat

  case object_store.put(nats_conn, bucket_name, large_name, large_data) {
    Ok(info) ->
      io.println(ansi.green(
        "Large object stored: "
        <> info.name
        <> " ("
        <> int.to_string(info.size)
        <> " bytes, "
        <> int.to_string(info.chunks)
        <> " chunks)",
      ))
    Error(err) -> io.println(ansi.red("Put failed: " <> error_to_string(err)))
  }

  io.println(ansi.yellow("\n=== Getting an object ==="))
  case object_store.get(nats_conn, bucket_name, object_name) {
    Ok(data) -> {
      let assert Ok(text) = bit_array.to_string(data)
      io.println(ansi.green("Retrieved object: " <> text))
    }
    Error(err) -> io.println(ansi.red("Get failed: " <> error_to_string(err)))
  }

  io.println(ansi.yellow("\n=== Getting a larger object ==="))
  case object_store.get(nats_conn, bucket_name, large_name) {
    Ok(data) ->
      io.println(ansi.green(
        "Retrieved large object: "
        <> int.to_string(bit_array.byte_size(data))
        <> " bytes",
      ))
    Error(err) -> io.println(ansi.red("Get failed: " <> error_to_string(err)))
  }

  io.println(ansi.yellow("\n=== Listing objects in bucket ==="))
  case object_store.list(nats_conn, bucket_name) {
    Ok(objects) -> {
      io.println(ansi.green("Objects in bucket '" <> bucket_name <> "':"))
      list.each(objects, fn(info) {
        io.println(
          "  - "
          <> info.name
          <> " ("
          <> int.to_string(info.size)
          <> " bytes, "
          <> int.to_string(info.chunks)
          <> " chunks)",
        )
      })
    }
    Error(err) -> io.println(ansi.red("List failed: " <> error_to_string(err)))
  }

  case
    object_store.get(nats_conn, "filez", "/mnt/ramdisk/metrics.20260429.log.gz")
  {
    Ok(data) -> {
      let assert Ok(_) = simplifile.write_bits("DOWNLOADED", data)
      Nil
    }
    Error(err) -> {
      io.println(ansi.red("Failed to get file: " <> error_to_string(err)))
    }
  }

  process.sleep(100)
  nuts.shutdown(nats_conn)
}

fn error_to_string(err: nuts.NatsError) -> String {
  case err {
    nuts.NotConnected -> "NotConnected"
    nuts.NetworkError(e) -> "NetworkError: " <> string.inspect(e)
    nuts.ProtocolError(msg) -> "ProtocolError: " <> msg
    nuts.AuthenticationFailed -> "AuthenticationFailed"
    nuts.GenericError(msg) -> "GenericError: " <> msg
    nuts.BadURL -> "BadURL"
  }
}
