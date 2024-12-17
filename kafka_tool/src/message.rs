use core::str;

use kafka::consumer::Message;

pub fn decode(msg: &Message, topic: &str) {
    let key_str = str::from_utf8(msg.key);
    let value_str = str::from_utf8(msg.value);

    println!("{:?}", key_str);
    println!("{:?}", value_str);
}
