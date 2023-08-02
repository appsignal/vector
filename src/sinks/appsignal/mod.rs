//! The AppSignal sink
//!
//! This sink provides downstream support for `AppSignal` to collect logs and a subset of Vector
//! metric types. These events are sent to the `appsignal-endpoint.net` domain, which is part of
//! the `appsignal.com` infrastructure.
//!
//! Logs and metrics are stored on an per app basis and require an app-level Push API key.

#[cfg(all(test, feature = "appsignal-integration-tests"))]
mod integration_tests;

use std::task::Poll;

use http::{header::AUTHORIZATION, Uri};
use serde_json::json;

use crate::{
    http::HttpClient,
    internal_events::SinkRequestBuildError,
    sinks::{
        prelude::*,
        util::{encoding::Encoder, Compression},
        BuildError,
    },
};
use bytes::Bytes;
use vector_common::sensitive_string::SensitiveString;
use vector_core::config::telemetry;

/// Configuration for the `appsignal` sink.
#[configurable_component(sink("appsignal", "AppSignal sink."))]
#[derive(Clone, Debug)]
pub struct AppsignalConfig {
    /// The URI for the AppSignal API to send data to.
    #[configurable(validation(format = "uri"))]
    #[configurable(metadata(docs::examples = "https://appsignal-endpoint.net"))]
    #[serde(default = "default_endpoint")]
    endpoint: String,

    /// A valid app-level AppSignal Push API key.
    #[configurable(metadata(docs::examples = "00000000-0000-0000-0000-000000000000"))]
    #[configurable(metadata(docs::examples = "${APPSIGNAL_PUSH_API_KEY}"))]
    push_api_key: SensitiveString,

    #[configurable(derived)]
    #[serde(default = "Compression::gzip_default")]
    compression: Compression,

    #[configurable(derived)]
    #[serde(
        default,
        skip_serializing_if = "crate::serde::skip_serializing_if_default"
    )]
    transformer: Transformer,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::skip_serializing_if_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,
}

fn default_endpoint() -> String {
    "https://appsignal-endpoint.net".to_string()
}

impl GenerateConfig for AppsignalConfig {
    fn generate_config() -> toml::Value {
        toml::from_str("").unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "appsignal")]
impl SinkConfig for AppsignalConfig {
    async fn build(&self, _cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let healthcheck = Box::pin(async move { Ok(()) });
        let sink = VectorSink::from_event_streamsink(AppsignalSink::new(self)?);

        Ok((sink, healthcheck))
    }

    fn input(&self) -> Input {
        Input::new(DataType::Metric | DataType::Log)
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

#[derive(Debug, Clone)]
struct AppsignalSink {
    endpoint: Uri,
    push_api_key: SensitiveString,
    client: HttpClient,
    compression: Compression,
    transformer: Transformer,
}

impl AppsignalSink {
    pub fn new(config: &AppsignalConfig) -> crate::Result<Self> {
        let tls = TlsSettings::from_options(&None).unwrap();
        let client = HttpClient::new(tls, &Default::default()).unwrap();
        let endpoint = endpoint_uri(&config.endpoint, "vector/events")?;
        let push_api_key = config.push_api_key.clone();
        let compression = config.compression.clone();
        let transformer = config.transformer.clone();

        Ok(Self {
            client,
            endpoint,
            push_api_key,
            compression,
            transformer,
        })
    }

    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let service = tower::ServiceBuilder::new().service(AppsignalService {
            client: self.client.clone(),
            endpoint: self.endpoint.clone(),
            push_api_key: self.push_api_key.clone(),
        });

        let sink = input
            .request_builder(
                None,
                AppsignalRequestBuilder {
                    compression: self.compression.clone(),
                    encoder: AppsignalEncoder {
                        transformer: self.transformer.clone(),
                    },
                },
            )
            .filter_map(|request| async move {
                match request {
                    Err(error) => {
                        emit!(SinkRequestBuildError { error });
                        None
                    }
                    Ok(req) => Some(req),
                }
            })
            .into_driver(service);

        sink.run().await
    }
}

#[async_trait::async_trait]
impl StreamSink<Event> for AppsignalSink {
    async fn run(
        self: Box<Self>,
        input: futures_util::stream::BoxStream<'_, Event>,
    ) -> Result<(), ()> {
        self.run_inner(input).await
    }
}

#[derive(Clone)]
struct AppsignalEncoder {
    pub transformer: crate::codecs::Transformer,
}

impl Encoder<Event> for AppsignalEncoder {
    fn encode_input(
        &self,
        mut event: Event,
        writer: &mut dyn std::io::Write,
    ) -> std::io::Result<(usize, GroupedCountByteSize)> {
        self.transformer.transform(&mut event);

        let mut byte_size = telemetry().create_request_count_byte_size();
        byte_size.add_event(&event, event.estimated_json_encoded_size_of());

        let json = match event {
            Event::Log(log) => json!({ "log": log }),
            Event::Metric(metric) => json!({ "metric": metric }),
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("The AppSignal sink does not support this type of event: {event:?}"),
                ))
            }
        };
        let body = json.to_string().bytes().collect::<Vec<u8>>();
        write_all(writer, 1, &body)?;

        Ok((body.len(), byte_size))
    }
}

#[derive(Clone)]
struct AppsignalRequest {
    payload: Bytes,
    finalizers: EventFinalizers,
    metadata: RequestMetadata,
}

impl MetaDescriptive for AppsignalRequest {
    fn get_metadata(&self) -> &RequestMetadata {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut RequestMetadata {
        &mut self.metadata
    }
}

impl Finalizable for AppsignalRequest {
    fn take_finalizers(&mut self) -> EventFinalizers {
        self.finalizers.take_finalizers()
    }
}

struct AppsignalRequestBuilder {
    encoder: AppsignalEncoder,
    compression: Compression,
}

impl RequestBuilder<Event> for AppsignalRequestBuilder {
    type Metadata = EventFinalizers;
    type Events = Event;
    type Encoder = AppsignalEncoder;
    type Payload = Bytes;
    type Request = AppsignalRequest;
    type Error = std::io::Error;

    fn compression(&self) -> Compression {
        self.compression
    }

    fn encoder(&self) -> &Self::Encoder {
        &self.encoder
    }

    fn split_input(
        &self,
        mut input: Event,
    ) -> (Self::Metadata, RequestMetadataBuilder, Self::Events) {
        let finalizers = input.take_finalizers();
        let metadata_builder = RequestMetadataBuilder::from_event(&input);

        (finalizers, metadata_builder, input)
    }

    fn build_request(
        &self,
        metadata: Self::Metadata,
        request_metadata: RequestMetadata,
        payload: EncodeResult<Self::Payload>,
    ) -> Self::Request {
        AppsignalRequest {
            finalizers: metadata,
            payload: payload.into_payload(),
            metadata: request_metadata,
        }
    }
}

struct AppsignalService {
    endpoint: Uri,
    push_api_key: SensitiveString,
    client: HttpClient,
}

impl tower::Service<AppsignalRequest> for AppsignalService {
    type Response = AppsignalResponse;
    type Error = &'static str;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut request: AppsignalRequest) -> Self::Future {
        let metadata = std::mem::take(request.metadata_mut());
        let json_byte_size = metadata.into_events_estimated_json_encoded_byte_size();
        let body = hyper::Body::from(request.payload);
        let req = http::Request::post(&self.endpoint)
            .header("Content-Type", "application/json")
            .header(
                AUTHORIZATION,
                format!("Bearer {}", self.push_api_key.inner()),
            )
            .body(body)
            .unwrap();

        let mut client = self.client.clone();

        Box::pin(async move {
            match client.call(req).await {
                Ok(response) => {
                    if response.status().is_success() {
                        Ok(AppsignalResponse { json_byte_size })
                    } else {
                        Err("received error response")
                    }
                }
                Err(_error) => Err("oops"),
            }
        })
    }
}

struct AppsignalResponse {
    json_byte_size: GroupedCountByteSize,
}

impl DriverResponse for AppsignalResponse {
    fn event_status(&self) -> EventStatus {
        EventStatus::Delivered
    }

    fn events_sent(&self) -> &GroupedCountByteSize {
        &self.json_byte_size
    }
}

fn endpoint_uri(endpoint: &str, path: &str) -> crate::Result<Uri> {
    let uri = if endpoint.ends_with('/') {
        format!("{endpoint}{path}")
    } else {
        format!("{endpoint}/{path}")
    };
    match uri.parse::<Uri>() {
        Ok(u) => Ok(u),
        Err(e) => Err(Box::new(BuildError::UriParseError { source: e })),
    }
}

#[cfg(test)]
mod test {
    use futures::{future::ready, stream};
    use serde::Deserialize;
    use vector_core::event::{Event, LogEvent};

    use crate::config::{GenerateConfig, SinkConfig, SinkContext};

    use super::{endpoint_uri, AppsignalConfig};

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<AppsignalConfig>();
    }

    #[test]
    fn endpoint_uri_with_path() {
        let uri = endpoint_uri("https://appsignal-endpoint.net", "vector/events");
        assert_eq!(
            uri.expect("Not a valid URI").to_string(),
            "https://appsignal-endpoint.net/vector/events"
        );
    }

    #[test]
    fn endpoint_uri_with_trailing_slash() {
        let uri = endpoint_uri("https://appsignal-endpoint.net/", "vector/events");
        assert_eq!(
            uri.expect("Not a valid URI").to_string(),
            "https://appsignal-endpoint.net/vector/events"
        );
    }
}
