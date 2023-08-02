//! `Basic` sink.
//! A sink that will send it's output to standard out for pedagogical purposes.

use crate::prelude::*;
use std::task::Poll;

use crate::{http::HttpClient, internal_events::SinkRequestBuildError, sinks::prelude::*};
use bytes::Bytes;

#[configurable_component(sink("basic", "Basic sink."))]
#[derive(Clone, Debug)]
/// A basic sink that dumps its output to stdout.
pub struct BasicConfig {
    /// The endpoint to send HTTP traffic to.
    ///
    /// This should include the protocol and host, but can also include the port, path, and any other valid part of a URI.
    #[configurable(metadata(
        docs::examples = "http://localhost:3000/",
        docs::examples = "http://example.com/endpoint/",
    ))]
    pub endpoint: String,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::skip_serializing_if_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,
}

impl GenerateConfig for BasicConfig {
    fn generate_config() -> toml::Value {
        toml::from_str("").unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "basic")]
impl SinkConfig for BasicConfig {
    async fn build(&self, _cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let healthcheck = Box::pin(async move { Ok(()) });
        let sink = VectorSink::from_event_streamsink(BasicSink);

        Ok((sink, healthcheck))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

#[derive(Debug, Clone)]
struct BasicSink {
    endpoint: String,
    client: HttpClient,
}

impl BasicSink {
    pub fn new(config: &BasicConfig) -> Self {
        let tls = TlsSettings::from_options(&None).unwrap();
        let client = HttpClient::new(tls, &Default::default()).unwrap();
        let endpoint = config.endpoint.clone();

        Self { client, endpoint }
    }

    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let service = tower::ServiceBuilder::new().service(BasicService {
            client: self.client.clone(),
            endpoint: self.endpoint.clone(),
        });

        let sink = input
            .request_builder(
                None,
                BasicRequestBuilder {
                    encoder: BasicEncoder,
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
impl StreamSink<Event> for BasicSink {
    async fn run(
        self: Box<Self>,
        input: futures_util::stream::BoxStream<'_, Event>,
    ) -> Result<(), ()> {
        self.run_inner(input).await
    }
}

#[derive(Clone)]
struct BasicEncoder;
impl Encoder<Event> for BasicEncoder {
    fn encode_input(
        &self,
        input: Event,
        writer: &mut dyn std::io::Write,
    ) -> std::io::Result<usize> {
    }
}

#[derive(Clone)]
struct BasicRequest {
    payload: Bytes,
    finalizers: EventFinalizers,
    metadata: RequestMetadata,
}

impl MetaDescriptive for BasicRequest {
    fn get_metadata(&self) -> RequestMetadata {
        self.metadata
    }
}

impl Finalizable for BasicRequest {
    fn take_finalizers(&mut self) -> EventFinalizers {
        self.finalizers.take_finalizers()
    }
}

impl RequestBuilder<Event> for BasicRequestBuilder {
    type Metadata = EventFinalizers;
    type Events = Event;
    type Encoder = BasicEncoder;
    type Payload = Bytes;
    type Request = BasicRequest;
    type Error = std::io::Error;

    fn compression(&self) -> Compression {
        Compression::None
    }

    fn encoder(&self) -> &Self::Encoder {
        &self.encoder
    }

    fn split_input(
        &self,
        mut input: Event,
    ) -> (Self::Metadata, RequestMetadataBuilder, Self::Events) {
        let finalizers = input.take_finalizers();
        let metadata_builder = RequestMetadataBuilder::from_events(&input);
        (finalizers, metadata_builder, input)
    }

    fn build_request(
        &self,
        metadata: Self::Metadata,
        request_metadata: RequestMetadata,
        payload: EncodeResult<Self::Payload>,
    ) -> Self::Request {
        BasicRequest {
            finalizers: metadata,
            payload: payload.into_payload(),
            metadata: request_metadata,
        }
    }
}

struct BasicService {
    endpoint: String,
    client: HttpClient,
}
impl tower::Service<BasicRequest> for BasicService {
    type Response = BasicResponse;
    type Error = &'static str;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: BasicRequest) -> Self::Future {
        let byte_size = request.payload.len();
        let body = hyper::Body::from(request.payload);
        let req = http::Request::post(&self.endpoint)
            .header("Content-Type", "application/json")
            .body(body)
            .unwrap();

        let mut client = self.client.clone();

        Box::pin(async move {
            match client.call(req).await {
                Ok(response) => {
                    if response.status().is_success() {
                        Ok(BasicResponse { byte_size })
                    } else {
                        Err("received error response")
                    }
                }
                Err(_error) => Err("oops"),
            }
        })
    }
}

struct BasicResponse {
    byte_size: usize,
}

impl DriverResponse for BasicResponse {
    fn event_status(&self) -> EventStatus {
        EventStatus::Delivered
    }

    fn events_sent(&self) -> RequestCountByteSize {
        // (events count, byte size)
        CountByteSize(1, self.byte_size).into()
    }
}
