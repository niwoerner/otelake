use axum::{body::Bytes, http::StatusCode};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use prost::Message;

pub async fn metrics(body: Bytes) -> StatusCode {
    match ExportMetricsServiceRequest::decode(body) {
        Ok(req) => {
            println!("{req:#?}");
            StatusCode::OK
        }
        Err(e) => {
            eprintln!("metrics: decode failed: {e}");
            StatusCode::BAD_REQUEST
        }
    }
}
