import gleam/bit_array
import gleam/json
import nuts/nkey

pub type Auth {
  NoAuth
  NKeyAuth(public: String, signature: String)
}

pub type ConnectOptions {
  ConnectOptions(
    verbose: Bool,
    pedantic: Bool,
    tls_required: Bool,
    lang: String,
    version: String,
    headers: Bool,
    echo_: Bool,
    protocol: Int,
    auth: Auth,
    name: String,
    no_responders: Bool,
  )
}

pub fn to_json(connect_options: ConnectOptions) -> json.Json {
  let ConnectOptions(
    verbose:,
    pedantic:,
    tls_required:,
    lang:,
    version:,
    headers:,
    echo_:,
    protocol:,
    auth:,
    name:,
    no_responders:,
  ) = connect_options
  json.object(
    [
      #("verbose", json.bool(verbose)),
      #("pedantic", json.bool(pedantic)),
      #("tls_required", json.bool(tls_required)),
      #("lang", json.string(lang)),
      #("version", json.string(version)),
      #("headers", json.bool(headers)),
      #("echo_", json.bool(echo_)),
      #("protocol", json.int(protocol)),
      #("name", json.string(name)),
      #("no_responders", json.bool(no_responders)),
    ]
    |> append_auth(auth),
  )
}

fn append_auth(opts, auth: Auth) {
  case auth {
    NKeyAuth(public:, signature:) -> [
      #("nkey", json.string(public)),
      #("sig", json.string(signature)),
      ..opts
    ]
    NoAuth -> opts
  }
}

pub fn to_json_string(options: ConnectOptions) {
  json.to_string(to_json(options))
}

pub fn nkey_auth(
  nkey_seed: String,
  nonce: BitArray,
) -> Result(Auth, nkey.NkeyError) {
  case nkey.from_seed(nkey_seed) {
    Error(err) -> Error(err)
    Ok(keypair) -> {
      NKeyAuth(
        public: nkey.public(keypair),
        signature: nkey.sign(keypair, nonce)
          |> bit_array.base64_url_encode(False),
      )
      |> Ok
    }
  }
}
