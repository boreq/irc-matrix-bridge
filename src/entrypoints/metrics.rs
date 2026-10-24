use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use tokio::net::TcpListener;

use crate::adapters::prometheus;
use crate::application::queries::bridge_metrics::BridgeMetricsHandler;

pub async fn run(listener: TcpListener, handler: Arc<BridgeMetricsHandler>) {
    let router = Router::new()
        .route("/metrics", get(scrape))
        .with_state(handler);
    if let Err(error) = axum::serve(listener, router).await {
        tracing::error!(%error, "metrics server stopped");
    }
}

async fn scrape(State(handler): State<Arc<BridgeMetricsHandler>>) -> Response {
    match handler.handle().await {
        Ok(metrics) => (
            [(header::CONTENT_TYPE, prometheus::CONTENT_TYPE)],
            prometheus::render(&metrics),
        )
            .into_response(),
        Err(error) => {
            tracing::error!(%error, "collecting metrics failed");
            // Never report an empty queue when the database could not be read.
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "collecting metrics failed\n",
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use crate::adapters::clock::SystemClock;
    use crate::adapters::connection_health::InMemoryConnectionHealth;
    use crate::adapters::sqlite::SqliteMessageRepository;
    use crate::application::ports::QueueStatsReader;
    use crate::domain::Bridges;
    use crate::domain::metrics::PendingDestination;
    use crate::errors::{Error, Result};

    struct BrokenQueue;

    #[async_trait]
    impl QueueStatsReader for BrokenQueue {
        async fn oldest_pending(&self) -> Result<Vec<PendingDestination>> {
            Err(Error::Unknown(anyhow::anyhow!("database unavailable")))
        }
    }

    async fn request(queue: Arc<dyn QueueStatsReader>, path: &str) -> String {
        let clock = Arc::new(SystemClock);
        let handler = Arc::new(BridgeMetricsHandler::new(
            Arc::new(Bridges::default()),
            queue,
            Arc::new(InMemoryConnectionHealth::new(clock.clone())),
            clock,
        ));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(run(listener, handler));
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let mut response = String::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            stream.read_to_string(&mut response),
        )
        .await
        .unwrap()
        .unwrap();
        server.abort();
        response
    }

    #[tokio::test]
    async fn serves_prometheus_only_at_metrics_path() {
        let queue = Arc::new(SqliteMessageRepository::in_memory().unwrap());
        let response = request(queue.clone(), "/metrics").await;
        assert!(response.starts_with("HTTP/1.1 200"));
        assert!(response.contains(prometheus::CONTENT_TYPE));
        assert!(response.contains("# TYPE bridge_pending_message_age_seconds gauge"));
        assert!(request(queue, "/other").await.starts_with("HTTP/1.1 404"));
    }

    #[tokio::test]
    async fn database_errors_fail_the_scrape() {
        let response = request(Arc::new(BrokenQueue), "/metrics").await;
        assert!(response.starts_with("HTTP/1.1 500"));
        assert!(!response.contains("# TYPE"));
    }
}
