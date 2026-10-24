use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::application::ports::{Clock, ConnectionHealth};
use crate::domain::Network;
use crate::domain::time::DateTime;

/// Health observations are process-local; no observation means unknown/unhealthy.
pub struct InMemoryConnectionHealth {
    clock: Arc<dyn Clock>,
    observations: Mutex<HashMap<Network, DateTime>>,
}

impl InMemoryConnectionHealth {
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            clock,
            observations: Mutex::new(HashMap::new()),
        }
    }
}

impl ConnectionHealth for InMemoryConnectionHealth {
    fn record_healthy(&self, network: &Network) {
        let mut observations = self.observations.lock().unwrap();
        observations.insert(network.clone(), self.clock.now());
    }

    fn last_healthy(&self, network: &Network) -> Option<DateTime> {
        self.observations.lock().unwrap().get(network).copied()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI64, Ordering};

    use super::*;
    use crate::domain::IrcNetworkName;

    struct TestClock {
        millis: AtomicI64,
    }

    impl Clock for TestClock {
        fn now(&self) -> DateTime {
            DateTime::from_unix_millis(self.millis.load(Ordering::Relaxed)).unwrap()
        }
    }

    #[test]
    fn observations_are_updated_and_isolated_by_network() {
        let clock = Arc::new(TestClock {
            millis: AtomicI64::new(1_000),
        });
        let health = InMemoryConnectionHealth::new(clock.clone());
        let irc = Network::Irc(IrcNetworkName::new("test"));
        assert_eq!(health.last_healthy(&Network::Matrix), None);
        health.record_healthy(&Network::Matrix);
        assert_eq!(health.last_healthy(&Network::Matrix), Some(clock.now()));
        assert_eq!(health.last_healthy(&irc), None);
        clock.millis.store(2_000, Ordering::Relaxed);
        health.record_healthy(&irc);
        health.record_healthy(&Network::Matrix);
        assert_eq!(health.last_healthy(&Network::Matrix), Some(clock.now()));
        assert_eq!(health.last_healthy(&irc), Some(clock.now()));
    }
}
