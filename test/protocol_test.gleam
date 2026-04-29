import gleam/option.{None, Some}
import nuts/internal/protocol.{
  Continue, NeedsMoreData, ParserConfig, ProtocolError,
}

const default_config = ParserConfig(max_payload: 1_048_576)

pub fn parse_test() {
  assert protocol.parse(<<"PING\r\n">>, config: default_config)
    == Continue(protocol.Ping, <<>>)

  assert protocol.parse(<<"MSG hello 12 11">>, config: default_config)
    == NeedsMoreData

  assert protocol.parse(<<"MSG hello 12">>, config: default_config)
    == NeedsMoreData

  assert protocol.parse(<<"MSG hello 12 11\r\n">>, config: default_config)
    == NeedsMoreData

  assert protocol.parse(
      <<"MSG hello 12 11\r\nhello world\r\n">>,
      config: default_config,
    )
    == Continue(protocol.Msg("hello", "12", None, <<"hello world">>), <<>>)

  assert protocol.parse(
      <<"MSG hello 12 11\r\nhello world\r\n">>,
      config: default_config,
    )
    == Continue(protocol.Msg("hello", "12", None, <<"hello world">>), <<>>)

  assert protocol.parse(
      <<"MSG hello 12 orban 11\r\nhello world\r\n">>,
      config: default_config,
    )
    == Continue(
      protocol.Msg("hello", "12", Some("orban"), <<"hello world">>),
      <<>>,
    )
}

pub fn parse_double_messages() {
  let assert Continue(protocol.Msg("foo", "1", None, <<"bar">>), rest) =
    protocol.parse(
      <<"MSG foo 1 3\r\nbar\r\nMSG foo 1 3\r\nbaz\r\n">>,
      config: default_config,
    )

  assert protocol.parse(rest, config: default_config)
    == Continue(protocol.Msg("foo", "1", None, <<"baz">>), <<>>)
}

pub fn parse_message_with_headers_test() {
  assert protocol.parse(
      <<
        "HMSG FOO.BAR alice 34 45\r\nNATS/1.0\r\nFoodGroup: vegetable\r\n\r\nHello World\r\n",
      >>,
      config: default_config,
    )
    == Continue(
      protocol.Hmsg(
        topic: "FOO.BAR",
        headers: [#("FoodGroup", "vegetable")],
        sid: "alice",
        reply_to: None,
        payload: <<"Hello World">>,
      ),
      <<>>,
    )

  assert protocol.parse(
      <<
        "HMSG FOO.BAR 9 BAZ.69 34 45\r\nNATS/1.0\r\nFoodGroup: vegetable\r\n\r\nHello World\r\n",
      >>,
      config: default_config,
    )
    == Continue(
      protocol.Hmsg(
        topic: "FOO.BAR",
        headers: [#("FoodGroup", "vegetable")],
        sid: "9",
        reply_to: Some("BAZ.69"),
        payload: <<"Hello World">>,
      ),
      <<>>,
    )
}

pub fn parse_timeout_test() {
  assert protocol.parse(
      <<
        "HMSG my_inbox 2 81 81\r\nNATS/1.0 408 Request Timeout\r\nNats-Pending-Messages: 1\r\nNats-Pending-Bytes: 0\r\n\r\n\r\n",
      >>,
      config: default_config,
    )
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
  assert protocol.parse(<<"M">>, config: default_config) == NeedsMoreData
}

pub fn parse_invalid_command_test() {
  assert protocol.parse(<<"FOO 39\r\n">>, config: default_config)
    == ProtocolError("invalid command")
}

pub fn parse_err_test() {
  assert protocol.parse(<<"-ERR\r\n">>, config: default_config)
    == ProtocolError("empty error message")

  assert protocol.parse(<<"-ERR this is an error\r\n">>, config: default_config)
    == Continue(protocol.ERR("this is an error"), <<>>)
}

pub fn parse_pong_test() {
  assert protocol.parse(<<"PONG\r\n">>, config: default_config)
    == Continue(protocol.Pong, <<>>)
}

pub fn parse_ok_test() {
  assert protocol.parse(<<"+OK\r\n">>, config: default_config)
    == Continue(protocol.OK, <<>>)
}

pub fn parse_info_test() {
  let info_json =
    "{\"server_id\":\"sid1\",\"server_name\":\"nats-1\",\"version\":\"2.10.0\",\"go\":\"1.21.0\",\"host\":\"127.0.0.1\",\"port\":4222,\"headers\":true,\"max_payload\":1048576,\"proto\":1}"
  let assert Continue(protocol.Info(info), <<>>) =
    protocol.parse(<<"INFO ", info_json:utf8, "\r\n">>, config: default_config)

  assert info.server_id == "sid1"
  assert info.host == "127.0.0.1"
  assert info.port == 4222
  assert info.headers == True
  assert info.max_payload == 1_048_576
  assert info.client_id == None
  assert info.auth_required == None
  assert info.nonce == None
  assert info.jetstream == None
}

pub fn parse_info_with_optional_fields_test() {
  let info_json =
    "{\"server_id\":\"sid2\",\"server_name\":\"nats-2\",\"version\":\"2.11.0\",\"go\":\"1.22.0\",\"host\":\"0.0.0.0\",\"port\":4222,\"headers\":false,\"max_payload\":2097152,\"proto\":1,\"client_id\":42,\"auth_required\":true,\"tls_required\":true,\"nonce\":\"abc123\",\"jetstream\":true,\"cluster\":\"test-cluster\",\"domain\":\"default\"}"
  let assert Continue(protocol.Info(info), <<>>) =
    protocol.parse(<<"INFO ", info_json:utf8, "\r\n">>, config: default_config)

  assert Some(42) == info.client_id
  assert Some(True) == info.auth_required
  assert Some(True) == info.tls_required
  assert Some(True) == info.jetstream
  assert Some("test-cluster") == info.cluster
  assert Some("default") == info.domain
}

pub fn parse_info_bad_json_test() {
  assert protocol.parse(<<"INFO {bad json}\r\n">>, config: default_config)
    == ProtocolError("failed to decode server info")
}

pub fn parse_info_partial_test() {
  assert protocol.parse(<<"INFO ">>, config: default_config) == NeedsMoreData
}

pub fn parse_info_with_remaining_data_test() {
  let info_json =
    "{\"server_id\":\"sid3\",\"server_name\":\"nats-3\",\"version\":\"2.10.0\",\"go\":\"1.21.0\",\"host\":\"localhost\",\"port\":4222,\"headers\":true,\"max_payload\":1048576,\"proto\":1}"
  let assert Continue(protocol.Info(info), rest) =
    protocol.parse(
      <<"INFO ", info_json:utf8, "\r\nPING\r\n">>,
      config: default_config,
    )

  assert info.server_id == "sid3"
  assert protocol.parse(rest, config: default_config)
    == Continue(protocol.Ping, <<>>)
}

pub fn parse_binary_payload_test() {
  assert protocol.parse(
      <<"MSG test 1 3\r\n", 0xFF, 0xFE, 0x01, "\r\n">>,
      config: default_config,
    )
    == Continue(protocol.Msg("test", "1", None, <<0xFF, 0xFE, 0x01>>), <<>>)
}

pub fn parse_over_max_payload_test() {
  assert protocol.parse(<<"MSG test 1 1048577\r\n">>, config: default_config)
    == ProtocolError("payload over max_payload")
}

pub fn parse_custom_max_payload_test() {
  let config = ParserConfig(max_payload: 10)
  assert protocol.parse(<<"MSG test 1 11\r\n">>, config:)
    == ProtocolError("payload over max_payload")

  assert protocol.parse(<<"MSG test 1 10\r\nhello worl\r\n">>, config:)
    == Continue(protocol.Msg("test", "1", None, <<"hello worl">>), <<>>)
}

pub fn parse_hmsg_status_only_test() {
  assert protocol.parse(
      <<
        "HMSG test 1 16 17\r\nNATS/1.0 408\r\n\r\nX\r\n",
      >>,
      config: default_config,
    )
    == Continue(
      protocol.Hmsg(
        topic: "test",
        headers: [#("Nats-Status", "408")],
        sid: "1",
        reply_to: None,
        payload: <<"X">>,
      ),
      <<>>,
    )
}

pub fn parse_hmsg_malformed_headers_test() {
  assert protocol.parse(
      <<
        "HMSG test 1 23 25\r\nNATS/1.0\r\nBadHeader\r\n\r\nhi\r\n",
      >>,
      config: default_config,
    )
    == ProtocolError("malformed headers")
}
