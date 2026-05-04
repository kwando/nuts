import gleam/option.{None, Some}
import gleeunit/should
import nuts/internal/protocol.{Continue, Hmsg, NeedsMoreData}

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
    Continue(protocol.Msg("hello", "12", None, <<"hello world">>), <<>>),
  )

  protocol.parse(<<"MSG hello 12 11\r\nhello world\r\n">>)
  |> should.equal(
    Continue(protocol.Msg("hello", "12", None, <<"hello world">>), <<>>),
  )

  protocol.parse(<<"MSG hello 12 orban 11\r\nhello world\r\n">>)
  |> should.equal(
    Continue(
      protocol.Msg("hello", "12", Some("orban"), <<"hello world">>),
      <<>>,
    ),
  )
}

pub fn parse_double_messages() {
  let assert Continue(protocol.Msg("foo", "1", None, <<"bar">>), rest) =
    protocol.parse(<<"MSG foo 1 3\r\nbar\r\nMSG foo 1 3\r\nbaz\r\n">>)

  let assert Continue(protocol.Msg("foo", "1", None, <<"baz">>), <<>>) =
    protocol.parse(rest)
}

pub fn parse_message_with_headers_test() {
  protocol.parse(<<
    "HMSG FOO.BAR alice 34 45\r\nNATS/1.0\r\nFoodGroup: vegetable\r\n\r\nHello World\r\n",
  >>)
  |> should.equal(
    Continue(
      protocol.Hmsg(
        topic: "FOO.BAR",
        headers: [#("FoodGroup", "vegetable")],
        sid: "alice",
        reply_to: option.None,
        payload: <<"Hello World">>,
      ),
      <<>>,
    ),
  )

  protocol.parse(<<
    "HMSG FOO.BAR 9 BAZ.69 34 45\r\nNATS/1.0\r\nFoodGroup: vegetable\r\n\r\nHello World\r\n",
  >>)
  |> should.equal(
    Continue(
      protocol.Hmsg(
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
    protocol.parse(<<
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
    protocol.parse(<<
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
    protocol.parse(<<
      "HMSG inbox 1 reply 43 48\r\nNATS/1.0 408 No Messages\r\nCustom: value\r\n\r\nhello\r\n",
    >>)

  should.equal(headers, [
    #("Nats-Status", "408 No Messages"),
    #("Custom", "value"),
  ])
}
