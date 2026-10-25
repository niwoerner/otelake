use axum::{body::Bytes, http::StatusCode};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use prost::Message;

pub async fn logs(body: Bytes) -> StatusCode {
    match ExportLogsServiceRequest::decode(body) {
        Ok(req) => {
            println!("{req:#?}");
            StatusCode::OK
        }
        Err(e) => {
            eprintln!("logs: decode failed: {e}");
            StatusCode::BAD_REQUEST
        }
    }
}
