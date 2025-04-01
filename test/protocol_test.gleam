import gleam/option
import gleeunit/should
import nuts/internal/protocol.{Continue, NeedsMoreData}

pub fn parse_test() {
  protocol.parse(<<"PING\r\n">>)
  |> should.equal(Continue(protocol.Ping, <<>>))

  protocol.parse(<<"MSG hello 12 11">>)
  |> should.equal(NeedsMoreData)

  protocol.parse(<<"MSG hello 12">>)
  |> should.equal(NeedsMoreData)

  protocol.parse(<<"MSG hello 12 11\r\n">>)
  |> should.equal(NeedsMoreData)

  protocol.parse(<<"MSG hello 12 11\r\nhello world\r\n">>)
  |> should.equal(
    Continue(protocol.Msg("hello", "12", option.None, <<"hello world">>), <<>>),
  )

  protocol.parse(<<"MSG hello 12 11\r\nhello world\r\n">>)
  |> should.equal(
    Continue(protocol.Msg("hello", "12", option.None, <<"hello world">>), <<>>),
  )

  protocol.parse(<<"MSG hello 12 orban 11\r\nhello world\r\n">>)
  |> should.equal(
    Continue(
      protocol.Msg("hello", "12", option.Some("orban"), <<"hello world">>),
      <<>>,
    ),
  )
}

pub fn parse_message_with_headers_test() {
  protocol.parse(<<
    "HMSG FOO.BAR alice 34 45\r\nNATS/1.0\r\nFoodGroup: vegetable\r\n\r\nHello World\r\n",
  >>)
  |> should.equal(
    Continue(
      protocol.Hmsg(
        "FOO.BAR",
        [#("FoodGroup", "vegetable")],
        "alice",
        option.None,
        <<"Hello World">>,
      ),
      <<>>,
    ),
  )
}
