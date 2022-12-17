use bytes::{Bytes, BytesMut};
use futures::{FutureExt, SinkExt};
use http::{Request, Uri};
use vector_config::configurable_component;

use crate::{
    config::{AcknowledgementsConfig, DataType, GenerateConfig, Input, SinkConfig, SinkContext},
    http::HttpClient,
    sinks::{
        appsignal::{healthcheck, encoder::AppsignalEventEncoder},
        util::{
            http::{BatchedHttpSink, HttpSink},
            BatchConfig, Buffer, Compression, RealtimeSizeBasedDefaultBatchSettings,
            TowerRequestConfig, UriSerde,
        },
        Healthcheck, VectorSink,
    },
    tls::{TlsConfig, TlsSettings},
};

/// Configuration for the `appsignal` sink.
#[configurable_component(sink("appsignal"))]
#[derive(Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct AppsignalSinkConfig {
    /// AppSignal endpoint to use
    pub endpoint: UriSerde,

    /// AppSignal api key to use
    pub api_key: Option<String>,

    #[configurable(derived)]
    #[serde(default = "Compression::gzip_default")]
    pub compression: Compression,

    #[configurable(derived)]
    #[serde(default)]
    pub batch: BatchConfig<RealtimeSizeBasedDefaultBatchSettings>,

    #[configurable(derived)]
    #[serde(default)]
    pub request: TowerRequestConfig,

    #[configurable(derived)]
    #[serde(default)]
    pub tls: Option<TlsConfig>,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::skip_serializing_if_default"
    )]
    acknowledgements: AcknowledgementsConfig,
}

impl AppsignalSinkConfig {
    fn build_http_client(&self, cx: &SinkContext) -> crate::Result<HttpClient> {
        let tls = TlsSettings::from_options(&self.tls)?;
        Ok(HttpClient::new(tls, cx.proxy())?)
    }

    fn build_healthcheck(&self, client: HttpClient) -> crate::Result<Healthcheck> {
        Ok(healthcheck(client, endpoint_uri(&self.endpoint.uri, "vector/healthcheck", None)).boxed())
    }

    fn build_sink(&self, _cx: SinkContext, client: HttpClient) -> crate::Result<VectorSink> {
        let batch = self.batch.into_batch_settings()?;
        let request = self.request.unwrap_with(&TowerRequestConfig::default());
        let sink = BatchedHttpSink::new(
            self.clone(),
            Buffer::new(batch.size, self.compression),
            request,
            batch.timeout,
            client,
        )
        .sink_map_err(|error| error!(message = "Fatal appsignal metrics sink error.", %error));

        Ok(VectorSink::from_event_sink(sink))
    }
}

impl GenerateConfig for AppsignalSinkConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(r#"endpoint = "https://appsignal-endpoint.net""#).unwrap()
    }
}

#[async_trait::async_trait]
impl SinkConfig for AppsignalSinkConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let client = self.build_http_client(&cx)?;
        let healthcheck = self.build_healthcheck(client.clone())?;
        let sink = self.build_sink(cx, client)?;

        Ok((sink, healthcheck))
    }

    fn input(&self) -> Input {
        Input::new(DataType::Metric | DataType::Log)
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

#[async_trait::async_trait]
impl HttpSink for AppsignalSinkConfig {
    type Input = BytesMut;
    type Output = BytesMut;
    type Encoder = AppsignalEventEncoder;

    fn build_encoder(&self) -> Self::Encoder {
        AppsignalEventEncoder::new()
    }

    async fn build_request(&self, events: Self::Output) -> crate::Result<Request<Bytes>> {
        let uri = endpoint_uri(&self.endpoint.uri, "vector/events", self.api_key.as_deref());

        let mut builder = Request::post(&uri).header("Content-Type", "application/json");

        if let Some(ce) = self.compression.content_encoding() {
            builder = builder.header("Content-Encoding", ce);
        }

        Ok(builder.body(events.freeze()).unwrap())
    }
}

fn endpoint_uri(uri: &Uri, path: &str, api_key: Option<&str>) -> Uri {
    let mut uri = uri.to_string();
    if !uri.ends_with('/') {
        uri.push('/');
    }
    uri.push_str(path);
    uri.push_str("?api_key=");
    if let Some(api_key) = api_key {
        uri.push_str(api_key);
    }
    uri.parse::<Uri>().expect("Could not parse uri")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<AppsignalSinkConfig>();
    }

    #[test]
    fn test_endpoint_uri() {
        let uri = endpoint_uri(
            &"https://appsignal-endpoint.net".parse().unwrap(),
            "vector/events",
            Some("api-key"),
        );
        assert_eq!(
            uri.to_string(),
            "https://appsignal-endpoint.net/vector/events?api_key=api-key"
        );
    }

    #[test]
    fn test_endpoint_uri_trailing_slash() {
        let uri = endpoint_uri(
            &"https://appsignal-endpoint.net/".parse().unwrap(),
            "vector/events",
            Some("api-key"),
        );
        assert_eq!(
            uri.to_string(),
            "https://appsignal-endpoint.net/vector/events?api_key=api-key"
        );
    }

    #[test]
    fn test_endpoint_uri_no_key() {
        let uri = endpoint_uri(
            &"https://appsignal-endpoint.net".parse().unwrap(),
            "vector/events",
            None
        );
        assert_eq!(
            uri.to_string(),
            "https://appsignal-endpoint.net/vector/events?api_key="
        );
    }
}
