import gleam/time/timestamp
import nuts/internal/jetstream

pub fn parse_ack_test() {
  let ack = "$JS.ACK.nmea.nuts_example.1.1029024.940949.1777365454785427545.0"
  let expected =
    jetstream.DeliveryInfo(
      stream_seq: 1_029_024,
      consumer_seq: 940_949,
      timestamp: t("2026-04-28T08:37:34.785427545Z"),
      delivery_count: 1,
      pending: 0,
    )

  assert Ok(expected) == jetstream.parse_ack(ack)
}

fn t(value: String) {
  let assert Ok(ts) = timestamp.parse_rfc3339(value)
  ts
}
