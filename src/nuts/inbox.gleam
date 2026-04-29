import gleam/int

@external(erlang, "nuts_ffi", "unique_integer")
fn unique_integer() -> Int

pub fn generate_prefix() -> String {
  "_INBOX." <> int.to_base36(unique_integer()) <> "."
}

pub fn new_inbox(prefix: String, token: String) -> String {
  prefix <> token
}
