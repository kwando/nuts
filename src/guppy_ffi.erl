%% Guppy NIF — Erlang functions that cannot be expressed in pure Gleam.
%%
%% Exported functions are called from Gleam via `@external(erlang, "guppy_ffi", ...)`.
%% Each function wraps an OTP / crypto / base32 primitive and converts exceptions
%% into Gleam-friendly `{ok, Value} | {error, Reason}` tuples.

-module(guppy_ffi).

-export([decode32/1, encode32/1, generate_key/1, sign/2, random_string/1, split_crlf/1]).

%% Decode a base32-encoded binary.
%% Returns `{ok, Decoded}` on success or `{error, Reason}` if the input is invalid.
decode32(Bin) ->
  try
    {ok, base32:decode(Bin)}
  catch
    _Class:ExceptionPattern:_Stacktrace ->
      {error, ExceptionPattern}
  end.

%% Encode a binary as base32 (no padding).
encode32(Bin) ->
  base32:encode(Bin).

%% Generate an Ed25519 key pair from a seed.
%% `PrivKeyIn` is the 32-byte seed. Returns `{ok, {PubKey, PrivKey}}` or `{error, Reason}`.
generate_key(PrivKeyIn) ->
  try
    case crypto:generate_key(eddsa, ed25519, PrivKeyIn) of
      {Pub, Priv} ->
        {ok, {Pub, Priv}}
    end
  catch
    _:ExceptionPattern:_Stacktrace ->
      {error, ExceptionPattern}
  end.

%% Sign `Data` with the given Ed25519 `PrivateKey`.
%% Returns a raw 64-byte signature binary.
sign(PrivateKey, Data) ->
  crypto:sign(eddsa, none, Data, [PrivateKey, ed25519]).

%% Generate a cryptographically-random string of length `Len`, base32-encoded.
random_string(Len) ->
  base32:encode(
    crypto:strong_rand_bytes(Len)).

%% Split a binary on the first `"\r\n"` sequence.
%% Returns `{ok, {Line, Rest}}` or `{error, nil}` when no CRLF is found.
%% Used by the NATS protocol parser to read line-delimited frames.
split_crlf(Binary) ->
  case binary:match(Binary, <<"\r\n">>) of
    {Pos, 2} ->
      <<Line:Pos/binary, "\r\n", Rest/binary>> = Binary,
      {ok, {Line, Rest}};
    nomatch ->
      {error, nil}
  end.
