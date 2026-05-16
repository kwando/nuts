-module(guppy_ffi).

-export([decode32/1, encode32/1, generate_key/1, sign/2, random_string/1, split_crlf/1]).

decode32(Bin) ->
  try
    {ok, base32:decode(Bin)}
  catch
    _Class:ExceptionPattern:_Stacktrace ->
      {error, ExceptionPattern}
  end.

encode32(Bin) ->
  base32:encode(Bin).

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

sign(PrivateKey, Data) ->
  crypto:sign(eddsa, none, Data, [PrivateKey, ed25519]).

random_string(Len) ->
  base32:encode(
    crypto:strong_rand_bytes(Len)).

split_crlf(Binary) ->
  case binary:match(Binary, <<"\r\n">>) of
    {Pos, 2} ->
      <<Line:Pos/binary, "\r\n", Rest/binary>> = Binary,
      {ok, {Line, Rest}};
    nomatch ->
      {error, nil}
  end.
