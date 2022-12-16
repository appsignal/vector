use http::{Request, StatusCode, Uri};

use crate::config::SinkDescription;
use crate::{http::HttpClient, sinks::HealthcheckError};

pub mod config;
pub mod encoder;

inventory::submit! {
    SinkDescription::new::<config::AppsignalSinkConfig>("appsignal")
}

async fn healthcheck(client: HttpClient, healthcheck_endpoint: Uri) -> crate::Result<()> {
    let request = Request::get(healthcheck_endpoint)
        .body(hyper::Body::empty())
        .unwrap();

    let response = client.send(request).await?;

    match response.status() {
        StatusCode::OK => Ok(()),
        other => Err(HealthcheckError::UnexpectedStatus { status: other }.into()),
    }
}
