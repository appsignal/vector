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

use http::{header::AUTHORIZATION, Request, StatusCode, Uri};
use hyper::Body;
use serde_json::{json, Value};

use crate::{
    http::HttpClient,
    internal_events::SinkRequestBuildError,
    sinks::{
        prelude::*,
        util::{
            encoding::{as_tracked_write, Encoder},
            http::HttpStatusRetryLogic,
            Compression,
        },
        BuildError,
    },
};
use bytes::Bytes;
use vector_common::sensitive_string::SensitiveString;
use vector_core::{
    config::{proxy::ProxyConfig, telemetry},
    tls::{MaybeTlsSettings, TlsEnableableConfig},
};

/// Configuration for the `appsignal` sink.
#[configurable_component(sink("appsignal", "AppSignal sink."))]
#[derive(Clone, Debug, Default)]
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
    #[serde(default)]
    batch: BatchConfig<AppsignalDefaultBatchSettings>,

    #[configurable(derived)]
    #[serde(
        default,
        skip_serializing_if = "crate::serde::skip_serializing_if_default"
    )]
    encoding: Transformer,

    #[configurable(derived)]
    #[serde(default)]
    request: TowerRequestConfig,

    #[configurable(derived)]
    tls: Option<TlsEnableableConfig>,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::skip_serializing_if_default"
    )]
    acknowledgements: AcknowledgementsConfig,
}

fn default_endpoint() -> String {
    "https://appsignal-endpoint.net".to_string()
}

#[derive(Clone, Copy, Debug, Default)]
struct AppsignalDefaultBatchSettings;

impl SinkBatchSettings for AppsignalDefaultBatchSettings {
    const MAX_EVENTS: Option<usize> = Some(100);
    const MAX_BYTES: Option<usize> = Some(450_000);
    const TIMEOUT_SECS: f64 = 1.0;
}

impl AppsignalConfig {
    fn build_client(&self, proxy: &ProxyConfig) -> crate::Result<HttpClient> {
        let tls = MaybeTlsSettings::from_config(&self.tls, false)?;
        let client = HttpClient::new(tls, proxy)?;
        Ok(client)
    }

    fn build_sink(&self, client: HttpClient) -> crate::Result<VectorSink> {
        let batch_settings = self.batch.into_batcher_settings()?;

        let endpoint = endpoint_uri(&self.endpoint, "vector/events")?;
        let protocol = get_protocol(&endpoint);
        let push_api_key = self.push_api_key.clone();
        let service = AppsignalService {
            endpoint,
            push_api_key,
            client,
        };

        let request_opts = self.request;
        let request_settings = request_opts.unwrap_with(&TowerRequestConfig::default());
        let retry_logic = HttpStatusRetryLogic::new(|req: &AppsignalResponse| req.http_status);

        let service = ServiceBuilder::new()
            .settings(request_settings, retry_logic)
            .service(service);

        let compression = self.compression.clone();
        let transformer = self.encoding.clone();
        let sink = AppsignalSink {
            service,
            compression,
            transformer,
            batch_settings,
            protocol,
        };

        Ok(VectorSink::from_event_streamsink(sink))
    }
}

impl_generate_config_from_default!(AppsignalConfig);

fn get_protocol(endpoint: &Uri) -> String {
    endpoint.scheme_str().unwrap_or("http").to_string()
}

#[async_trait::async_trait]
#[typetag::serde(name = "appsignal")]
impl SinkConfig for AppsignalConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let client = self.build_client(cx.proxy())?;
        let healthcheck = healthcheck(
            endpoint_uri(&self.endpoint, "vector/healthcheck")?,
            self.push_api_key.to_string(),
            client.clone(),
        )
        .boxed();
        let sink = self.build_sink(client)?;

        Ok((sink, healthcheck))
    }

    fn input(&self) -> Input {
        Input::new(DataType::Metric | DataType::Log)
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

async fn healthcheck(uri: Uri, push_api_key: String, client: HttpClient) -> crate::Result<()> {
    let request = Request::get(uri).header(AUTHORIZATION, format!("Bearer {}", push_api_key));
    let response = client.send(request.body(Body::empty()).unwrap()).await?;

    match response.status() {
        status if status.is_success() => Ok(()),
        other => Err(super::HealthcheckError::UnexpectedStatus { status: other }.into()),
    }
}

struct AppsignalSink<S> {
    service: S,
    compression: Compression,
    transformer: Transformer,
    batch_settings: BatcherSettings,
    protocol: String,
}

impl<S> AppsignalSink<S>
where
    S: Service<AppsignalRequest> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: std::fmt::Debug + Into<crate::Error> + Send,
{
    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let service = tower::ServiceBuilder::new().service(self.service);

        input
            .batched(self.batch_settings.into_byte_size_config())
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
            .into_driver(service)
            .protocol(self.protocol)
            .run()
            .await
    }
}

#[async_trait::async_trait]
impl<S> StreamSink<Event> for AppsignalSink<S>
where
    S: Service<AppsignalRequest> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: std::fmt::Debug + Into<crate::Error> + Send,
{
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

impl Encoder<Vec<Event>> for AppsignalEncoder {
    fn encode_input(
        &self,
        events: Vec<Event>,
        writer: &mut dyn std::io::Write,
    ) -> std::io::Result<(usize, GroupedCountByteSize)> {
        let mut result = Value::Array(Vec::new());
        let mut byte_size = telemetry().create_request_count_byte_size();
        for mut event in events {
            self.transformer.transform(&mut event);

            byte_size.add_event(&event, event.estimated_json_encoded_size_of());

            let json = match event {
                Event::Log(log) => json!({ "log": log }),
                Event::Metric(metric) => json!({ "metric": metric }),
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!(
                            "The AppSignal sink does not support this type of event: {event:?}"
                        ),
                    ))
                }
            };
            if let Value::Array(ref mut array) = result {
                array.push(json);
            }
        }
        let written_bytes =
            as_tracked_write::<_, _, std::io::Error>(writer, &result, |mut writer, value| {
                serde_json::to_writer(&mut writer, value)?;
                Ok(())
            })?;

        Ok((written_bytes, byte_size))
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

impl RequestBuilder<Vec<Event>> for AppsignalRequestBuilder {
    type Metadata = EventFinalizers;
    type Events = Vec<Event>;
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
        mut input: Vec<Event>,
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
        AppsignalRequest {
            finalizers: metadata,
            payload: payload.into_payload(),
            metadata: request_metadata,
        }
    }
}

#[derive(Clone)]
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
                    Ok(AppsignalResponse {
                        http_status: response.status(),
                        request_metadata: metadata,
                    })
                }
                Err(_error) => {
                    // TODO: better error message or snafu
                    Err("oops")
                }
            }
        })
    }
}

struct AppsignalResponse {
    http_status: StatusCode,
    request_metadata: RequestMetadata,
}

impl DriverResponse for AppsignalResponse {
    fn event_status(&self) -> EventStatus {
        if self.http_status.is_success() {
            EventStatus::Delivered
        } else if self.http_status.is_client_error() {
            EventStatus::Rejected
        } else {
            EventStatus::Errored
        }
    }

    fn events_sent(&self) -> &GroupedCountByteSize {
        self.request_metadata
            .events_estimated_json_encoded_byte_size()
    }

    fn bytes_sent(&self) -> Option<usize> {
        Some(self.request_metadata.request_wire_size())
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

    use crate::{
        config::{GenerateConfig, SinkConfig, SinkContext},
        test_util::{
            components::{run_and_assert_sink_compliance, HTTP_SINK_TAGS},
            http::{always_200_response, spawn_blackhole_http_server},
        },
    };

    use super::{endpoint_uri, AppsignalConfig};

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<AppsignalConfig>();
    }

    #[tokio::test]
    async fn component_spec_compliance() {
        let mock_endpoint = spawn_blackhole_http_server(always_200_response).await;

        let config = AppsignalConfig::generate_config().to_string();
        let mut config = AppsignalConfig::deserialize(toml::de::ValueDeserializer::new(&config))
            .expect("config should be valid");
        config.endpoint = mock_endpoint.to_string();

        let context = SinkContext::default();
        let (sink, _healthcheck) = config.build(context).await.unwrap();

        let event = Event::Log(LogEvent::from("simple message"));
        run_and_assert_sink_compliance(sink, stream::once(ready(event)), &HTTP_SINK_TAGS).await;
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
