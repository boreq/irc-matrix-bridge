use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;

use crate::application::commands::deliver_pending::{DeliverPending, DeliverPendingHandler};
use crate::application::ports::Clock;
use crate::domain::endpoint::NetworkId;

const MAX_IDLE: Duration = Duration::from_secs(60);

pub async fn run(
    network: NetworkId,
    handler: Arc<DeliverPendingHandler>,
    wakeup: Arc<Notify>,
    clock: Arc<dyn Clock>,
) {
    loop {
        let idle = match handler
            .handle(DeliverPending {
                network: network.clone(),
            })
            .await
        {
            Ok(result) => {
                if result.delivered > 0 {
                    tracing::debug!(%network, delivered = result.delivered, "delivered");
                }
                result.next_attempt_at.map_or(MAX_IDLE, |at| {
                    Duration::from_millis((at - clock.now_ms()).max(0) as u64).min(MAX_IDLE)
                })
            }
            Err(error) => {
                tracing::error!(%network, "delivery pass failed: {error:#}");
                Duration::from_secs(5)
            }
        };

        tokio::select! {
            _ = wakeup.notified() => {}
            _ = tokio::time::sleep(idle) => {}
        }
    }
}
