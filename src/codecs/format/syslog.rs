use crate::{
    codecs::decoding::{BoxedDeserializer, Deserializer, DeserializerConfig},
    config::log_schema,
    event::{Event, Value},
    internal_events::SyslogConvertUtf8Error,
    schema::{self, field},
};
use bytes::Bytes;
use chrono::{DateTime, Datelike, Utc};
use serde::{Deserialize, Serialize};
use smallvec::{smallvec, SmallVec};
use syslog_loose::{IncompleteDate, Message, ProcId, Protocol};

/// Config used to build a `SyslogDeserializer`.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct SyslogDeserializerConfig;

#[typetag::serde(name = "syslog")]
impl DeserializerConfig for SyslogDeserializerConfig {
    fn build(&self) -> crate::Result<BoxedDeserializer> {
        Ok(Box::new(SyslogDeserializer))
    }

    fn output_schema(&self) -> schema::Output {
        use field::{Kind, Purpose};

        let mut schema = schema::Output::empty();

        // The `message` field is always defined. If parsing fails, the entire body becomes the
        // message.
        schema.define_field(
            log_schema().message_key(),
            Kind::bytes(),
            Some(Purpose::Message),
        );

        // All other fields are optional.
        schema.define_optional_field(
            log_schema().timestamp_key(),
            Kind::timestamp(),
            Some(Purpose::Timestamp),
        );
        schema.define_optional_field("hostname", Kind::bytes(), Some(Purpose::Host));
        schema.define_optional_field("severity", Kind::bytes(), Some(Purpose::Severity));
        schema.define_optional_field("facility", Kind::bytes(), None);
        schema.define_optional_field("version", Kind::integer(), None);
        schema.define_optional_field("appname", Kind::bytes(), None);
        schema.define_optional_field("msgid", Kind::bytes(), None);
        schema.define_optional_field("procid", Kind::integer().or_bytes(), None);

        // "structured data" in a syslog message can be stored in any field, but will always be
        // a string.
        schema.define_other_fields(Kind::bytes());

        schema
    }
}

/// Deserializer that builds an `Event` from a byte frame containing a syslog
/// message.
#[derive(Debug, Clone)]
pub struct SyslogDeserializer;

impl Deserializer for SyslogDeserializer {
    fn parse(&self, bytes: Bytes) -> crate::Result<SmallVec<[Event; 1]>> {
        let line = std::str::from_utf8(&bytes).map_err(|error| {
            emit!(&SyslogConvertUtf8Error { error });
            error
        })?;
        let line = line.trim();
        let parsed = syslog_loose::parse_message_with_year(line, resolve_year);
        let mut event = Event::from(parsed.msg);

        insert_fields_from_syslog(&mut event, parsed);

        Ok(smallvec![event])
    }
}

/// Function used to resolve the year for syslog messages that don't include the
/// year.
///
/// If the current month is January, and the syslog message is for December, it
/// will take the previous year.
///
/// Otherwise, take the current year.
fn resolve_year((month, _date, _hour, _min, _sec): IncompleteDate) -> i32 {
    let now = Utc::now();
    if now.month() == 1 && month == 12 {
        now.year() - 1
    } else {
        now.year()
    }
}

fn insert_fields_from_syslog(event: &mut Event, parsed: Message<&str>) {
    let log = event.as_mut_log();

    if let Some(timestamp) = parsed.timestamp {
        log.insert(
            log_schema().timestamp_key(),
            DateTime::<Utc>::from(timestamp),
        );
    }
    if let Some(host) = parsed.hostname {
        // FIXME: should this be `log_schema().host_key()`?
        log.insert("hostname", host.to_string());
    }
    if let Some(severity) = parsed.severity {
        log.insert("severity", severity.as_str().to_owned());
    }
    if let Some(facility) = parsed.facility {
        log.insert("facility", facility.as_str().to_owned());
    }
    if let Protocol::RFC5424(version) = parsed.protocol {
        log.insert("version", version as i64);
    }
    if let Some(app_name) = parsed.appname {
        log.insert("appname", app_name.to_owned());
    }
    if let Some(msg_id) = parsed.msgid {
        log.insert("msgid", msg_id.to_owned());
    }
    if let Some(procid) = parsed.procid {
        let value: Value = match procid {
            ProcId::PID(pid) => pid.into(),
            ProcId::Name(name) => name.to_string().into(),
        };
        log.insert("procid", value);
    }

    for element in parsed.structured_data.into_iter() {
        for (name, value) in element.params() {
            let key = format!("{}.{}", element.id, name);
            log.insert(key, value);
        }
    }
}
