use http::{header, Request, StatusCode, Uri};

use crate::config::SinkDescription;
use crate::{http::HttpClient, sinks::HealthcheckError};

pub mod config;
pub mod encoder;

inventory::submit! {
    SinkDescription::new::<config::AppsignalSinkConfig>("appsignal")
}

async fn healthcheck(
    client: HttpClient,
    healthcheck_endpoint: Uri,
    api_key: String,
) -> crate::Result<()> {
    let request = Request::get(healthcheck_endpoint)
        .header(header::AUTHORIZATION, format!("Bearer {}", api_key))
        .body(hyper::Body::empty())
        .unwrap();

    let response = client.send(request).await?;

    match response.status() {
        StatusCode::OK => Ok(()),
        other => Err(HealthcheckError::UnexpectedStatus { status: other }.into()),
    }
}
