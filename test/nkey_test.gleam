import gleam/bit_array
import nuts/nkey

const seed = "SUALHP366GCQN53R7X3MJF4BCNEK6WTKATRZ7QAMDC7UTVBMC2WYUDKK64"

pub fn nkey_test() {
  let assert Ok(key_pair) = nkey.from_seed(seed)
  let assert Error(nkey.KeyTypeError) =
    nkey.from_seed("UCGW4QHF3RJ5CNKXBA6U6JOSMJG5QLH3UQMBQS353F4PQNRANJQ25ARO")

  nkey.sign(key_pair, <<"ABC">>)
  == <<
    248,
    103,
    237,
    152,
    65,
    72,
    9,
    188,
    144,
    112,
    240,
    233,
    6,
    124,
    68,
    193,
    102,
    48,
    17,
    70,
    157,
    45,
    232,
    157,
    12,
    150,
    175,
    197,
    78,
    182,
    254,
    86,
    62,
    51,
    112,
    240,
    95,
    102,
    36,
    15,
    134,
    24,
    198,
    235,
    253,
    10,
    45,
    194,
    242,
    68,
    125,
    150,
    40,
    128,
    204,
    252,
    182,
    7,
    141,
    240,
    146,
    55,
    205,
    0,
  >>
}

pub fn decode32_test() {
  assert Ok(<<57, 206>>) == nkey.decode32(<<"HHHH">>)
  assert Error(nkey.IncorrectPadding) == nkey.decode32(<<"HHH">>)
  assert Error(nkey.NonAlphabeticCharacter) == nkey.decode32(<<"AÅÄÖ">>)
}

pub fn public_test() {
  let assert Ok(key_pair) = nkey.from_seed(seed)
  nkey.public(key_pair)
  == "UCGW4QHF3RJ5CNKXBA6U6JOSMJG5QLH3UQMBQS353F4PQNRANJQ25ARO"
}

pub fn crc_checksum_test() {
  assert nkey.crc(<<"123456789">>) == 0x31C3
  assert nkey.crc(<<"hello">>) == 0xC362
}

pub fn make_signature_test() {
  let _ukey = "UCGW4QHF3RJ5CNKXBA6U6JOSMJG5QLH3UQMBQS353F4PQNRANJQ25ARO"
  let nonce = "vGQdo8wNU4VUnCs"
  let assert Ok(key_pair) = nkey.from_seed(seed)

  let sig =
    nkey.sign(key_pair, <<nonce:utf8>>)
    |> bit_array.base64_url_encode(False)

  assert sig
    == "JeUQ0emFY__yBpq3PUJPfYPVuir3ivStRI8n_IYVL3Qx0upDzxcKOjpxZJegw39ra1driN8ME9uoYWwNuY7MAQ"

  let nonce = "YDHPCX9K2rMLfw8"

  let sig =
    nkey.sign(key_pair, <<nonce:utf8>>)
    |> bit_array.base64_url_encode(False)

  assert sig
    == "QRtb1pOyeDebYK6L2EC4iqwlWH9UEy4vxIU9k-buKt523jOhKNuP8IqJNl3HEDC1RPb6mCtUmu8UoItlzR6hDA"
}
