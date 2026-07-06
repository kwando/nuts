import gleam/erlang/process
import gleam/otp/actor
import guppy
import guppy/kv

pub fn main() {
  let assert Ok(actor.Started(_, conn)) =
    guppy.start(guppy.new("10.0.0.7", 4222))

  process.sleep(1000)
  let ctx = kv.new_context(conn)

  // delete bucket if it already exists
  let _ = kv.delete_bucket(ctx, "guppy-example")
  let assert Ok(_) =
    kv.create_bucket(ctx, kv.default_bucket_config("guppy-example"))
  let bucket = kv.get_bucket(ctx, "guppy-example")

  // expect KeyNotFound if there is no data
  let assert Error(kv.KeyNotFound) = kv.get(bucket, "foo")

  // set the key "foo" to "bar"
  let _ = kv.put(bucket, "foo", <<"bar">>)

  // key should now have been set
  let assert Ok(<<"bar">>) = kv.get(bucket, "foo")

  // the foo key wil be returned here
  let assert Ok(["foo"]) = kv.list_keys(bucket)
}
