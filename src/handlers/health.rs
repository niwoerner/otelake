use axum::extract::State;

use crate::server::AppState;

pub async fn health(State(s): State<AppState>) -> &'static str {
    s.version
}
