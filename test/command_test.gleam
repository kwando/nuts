import gleam/option.{None, Some}
import nuts/internal/command

pub fn hpub_test() {
  assert command.hpub(
      subject: "FOO",
      reply_to: None,
      headers: [#("Bar", "Baz")],
      payload: <<"Hello NATS!">>,
    )
    == <<"HPUB FOO 22 33\r\nNATS/1.0\r\nBar: Baz\r\n\r\nHello NATS!\r\n">>

  assert command.hpub(
      subject: "FRONT.DOOR",
      reply_to: Some("JOKE.22"),
      headers: [#("BREAKFAST", "donut"), #("LUNCH", "burger")],
      payload: <<"Knock Knock">>,
    )
    == <<
      "HPUB FRONT.DOOR JOKE.22 45 56\r\nNATS/1.0\r\nBREAKFAST: donut\r\nLUNCH: burger\r\n\r\nKnock Knock\r\n",
    >>

  assert command.hpub(
      subject: "NOTIFY",
      reply_to: None,
      headers: [#("Bar", "Baz")],
      payload: <<>>,
    )
    == <<
      "HPUB NOTIFY 22 22\r\nNATS/1.0\r\nBar: Baz\r\n\r\n\r\n",
    >>

  assert command.hpub(
      subject: "MORNING.MENU",
      reply_to: None,
      headers: [#("BREAKFAST", "donut"), #("BREAKFAST", "eggs")],
      payload: <<"Yum!">>,
    )
    == <<
      "HPUB MORNING.MENU 47 51\r\nNATS/1.0\r\nBREAKFAST: donut\r\nBREAKFAST: eggs\r\n\r\nYum!\r\n",
    >>
}

pub fn unsub_test() {
  assert command.unsub("1", None) == <<"UNSUB 1\r\n">>
  assert command.unsub("1", Some(5)) == <<"UNSUB 1 5\r\n">>
}

pub fn pub_test() {
  assert command.pub_("hello", reply_to: option.None, payload: <<"MUPP">>)
    == <<"PUB hello 4\r\nMUPP\r\n">>
  assert command.pub_("hello", reply_to: option.Some("my_inbox"), payload: <<
      "MUPP",
    >>)
    == <<"PUB hello my_inbox 4\r\nMUPP\r\n">>
}
