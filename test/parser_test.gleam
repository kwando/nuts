import gleam/option.{None, Some}
import gleeunit/should
import guppy/internal/parser.{Continue, Hmsg, NeedsMoreData}

pub fn parse_test() {
  parser.parse(<<"PING\r\n">>)
  |> should.equal(Continue(parser.Ping, <<>>))

  parser.parse(<<"MSG hello 12 11">>)
  |> should.equal(NeedsMoreData)

  parser.parse(<<"MSG hello 12">>)
  |> should.equal(NeedsMoreData)

  parser.parse(<<"MSG hello 12 11\r\n">>)
  |> should.equal(NeedsMoreData)

  parser.parse(<<"MSG hello 12 11\r\nhello world\r\n">>)
  |> should.equal(
    Continue(parser.Msg("hello", "12", None, <<"hello world">>), <<>>),
  )

  parser.parse(<<"MSG hello 12 11\r\nhello world\r\n">>)
  |> should.equal(
    Continue(parser.Msg("hello", "12", None, <<"hello world">>), <<>>),
  )

  parser.parse(<<"MSG hello 12 orban 11\r\nhello world\r\n">>)
  |> should.equal(
    Continue(parser.Msg("hello", "12", Some("orban"), <<"hello world">>), <<>>),
  )
}

pub fn parse_double_messages() {
  let assert Continue(parser.Msg("foo", "1", None, <<"bar">>), rest) =
    parser.parse(<<"MSG foo 1 3\r\nbar\r\nMSG foo 1 3\r\nbaz\r\n">>)

  let assert Continue(parser.Msg("foo", "1", None, <<"baz">>), <<>>) =
    parser.parse(rest)
}

pub fn parse_message_with_headers_test() {
  parser.parse(<<
    "HMSG FOO.BAR alice 34 45\r\nNATS/1.0\r\nFoodGroup: vegetable\r\n\r\nHello World\r\n",
  >>)
  |> should.equal(
    Continue(
      parser.Hmsg(
        topic: "FOO.BAR",
        headers: [#("FoodGroup", "vegetable")],
        sid: "alice",
        reply_to: option.None,
        payload: <<"Hello World">>,
      ),
      <<>>,
    ),
  )

  parser.parse(<<
    "HMSG FOO.BAR 9 BAZ.69 34 45\r\nNATS/1.0\r\nFoodGroup: vegetable\r\n\r\nHello World\r\n",
  >>)
  |> should.equal(
    Continue(
      parser.Hmsg(
        topic: "FOO.BAR",
        headers: [#("FoodGroup", "vegetable")],
        sid: "9",
        reply_to: Some("BAZ.69"),
        payload: <<"Hello World">>,
      ),
      <<>>,
    ),
  )
}

pub fn parse_hmsg_with_status_code_test() {
  let assert Continue(
    Hmsg(
      topic: "consumer6JE8",
      headers: _,
      sid: "1",
      reply_to: None,
      payload: _,
    ),
    <<>>,
  ) =
    parser.parse(<<
      "HMSG consumer6JE8 1 33 33\r\nNATS/1.0 409 Consumer Deleted\r\n\r\n\r\n",
    >>)

  let assert Continue(
    Hmsg(
      topic: "inbox",
      headers: headers,
      sid: "1",
      reply_to: Some("reply"),
      payload: <<>>,
    ),
    <<>>,
  ) =
    parser.parse(<<
      "HMSG inbox 1 reply 28 28\r\nNATS/1.0 408 No Messages\r\n\r\n\r\n",
    >>)

  should.equal(headers, [
    #("Nats-Status", "408 No Messages"),
  ])
}

pub fn parse_hmsg_status_with_headers_test() {
  let assert Continue(
    Hmsg(
      topic: "inbox",
      headers: headers,
      sid: "1",
      reply_to: Some("reply"),
      payload: <<"hello">>,
    ),
    <<>>,
  ) =
    parser.parse(<<
      "HMSG inbox 1 reply 43 48\r\nNATS/1.0 408 No Messages\r\nCustom: value\r\n\r\nhello\r\n",
    >>)

  should.equal(headers, [
    #("Nats-Status", "408 No Messages"),
    #("Custom", "value"),
  ])
}
