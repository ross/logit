//! One native control payload (`ControlMessage::decode`). Oracle: a decoded message re-encodes
//! to bytes that decode to the same message.
#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use logit_proto::native::control::ControlMessage;

fuzz_target!(|data: &[u8]| {
    let mut bytes = Bytes::copy_from_slice(data);
    if let Ok(message) = ControlMessage::decode(&mut bytes) {
        let again = ControlMessage::decode(&mut message.encode())
            .expect("a decoded control message re-encodes to a decodable one");
        assert_eq!(again, message);
    }
});
