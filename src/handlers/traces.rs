use axum::{body::Bytes, http::StatusCode};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message;

// Empty 200 is a valid OTLP reply: an empty protobuf response encodes to 0 bytes.
pub async fn traces(body: Bytes) -> StatusCode {
    match ExportTraceServiceRequest::decode(body) {
        Ok(req) => {
            println!("{req:#?}");
            StatusCode::OK
        }
        Err(e) => {
            eprintln!("traces: decode failed: {e}");
            StatusCode::BAD_REQUEST
        }
    }
}
