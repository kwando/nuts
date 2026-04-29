-module(nuts_ffi).

-export([decode32/1, encode32/1, generate_key/1, sign/2, match_crlf/1, unique_integer/0]).

decode32(Bin) ->
    try
        {ok, base32:decode(Bin)}
    catch
        _Class:ExceptionPattern:_Stacktrace ->
            {error, ExceptionPattern}
    end.

encode32(Bin) ->
    base32:encode(Bin).

%    type KeyType {
%      Eddsa
%    }
%
%    type KeyTypeHash {
%      Ed25519
%    }
%
%    @external(erlang, "crypto", "generate_key")
%    fn generate_key(_: KeyType, _: KeyTypeHash, input: BitArray) -> BitArray

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

match_crlf(Binary) ->
    case binary:match(Binary, <<"\r\n">>) of
        {Pos, 2} -> {ok, Pos};
        nomatch -> {error, nil}
    end.

unique_integer() ->
    erlang:unique_integer([monotonic, positive]).
