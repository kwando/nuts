import gleam/option.{None, Some}
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

pub fn parse_timeout_test() {
  let buffer = <<
    "HMSG my_inbox 2 81 81\r\nNATS/1.0 408 Request Timeout\r\nNats-Pending-Messages: 1\r\nNats-Pending-Bytes: 0\r\n\r\n\r\n",
  >>
  assert protocol.parse(buffer)
    == Continue(
      protocol.Hmsg(
        "my_inbox",
        [
          #("Nats-Status", "408 Request Timeout"),
          #("Nats-Pending-Messages", "1"),
          #("Nats-Pending-Bytes", "0"),
        ],
        "2",
        None,
        <<>>,
      ),
      <<>>,
    )
}

pub fn parse_partial_command_test() {
  let buffer = <<
    "M",
  >>
  assert protocol.parse(buffer) == NeedsMoreData
}

pub fn parse_invalid_command_test() {
  let buffer = <<
    "FOO 39\r\n",
  >>
  assert protocol.parse(buffer) == protocol.ProtocolError("invalid command")
}

pub fn parse_err_test() {
  assert protocol.parse(<<"-ERR\r\n">>)
    == protocol.ProtocolError("empty error message")
  assert protocol.parse(<<"-ERR this is an error\r\n">>)
    == Continue(protocol.ERR("this is an error"), <<>>)
}
