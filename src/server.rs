use axum::{
    Router,
    extract::DefaultBodyLimit,
    routing::{get, post},
};

use crate::handlers::{health, logs, metrics, traces};

// OTLP batches can be large; axum's default body limit is only 2 MB.
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct AppState {
    pub version: &'static str,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health::health))
        .route("/v1/traces", post(traces::traces))
        .route("/v1/metrics", post(metrics::metrics))
        .route("/v1/logs", post(logs::logs))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state)
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let state = AppState {
        version: env!("CARGO_PKG_VERSION"),
    };

    let listener = tokio::net::TcpListener::bind("0.0.0.0:4318").await?;
    println!("listening on {}", listener.local_addr()?);

    axum::serve(listener, router(state)).await?;

    Ok(())
}
