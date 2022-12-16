use bytes::{BufMut, BytesMut};
use prost::Message;

use crate::{
    event::{
        proto::EventWrapper,
        Event,
    },
    sinks::util::http::HttpEventEncoder,
};

pub struct AppsignalEventEncoder {}

impl AppsignalEventEncoder {
    pub fn new() -> Self {
        Self {}
    }
}

impl HttpEventEncoder<BytesMut> for AppsignalEventEncoder {
    fn encode_event(&mut self, event: Event) -> Option<BytesMut> {
        let data = EventWrapper::from(event);
        let event_len = data.encoded_len();

        let mut out = BytesMut::with_capacity(event_len + 4);
        out.put_u32(event_len as u32);
        data.encode(&mut out).unwrap();

        Some(out)
    }
}
