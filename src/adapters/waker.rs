use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Notify;

use crate::application::ports::DeliveryWaker;
use crate::domain::channel::Network;

pub struct NotifyWaker {
    notifiers: HashMap<Network, Arc<Notify>>,
}

impl NotifyWaker {
    pub fn new<'a>(networks: impl IntoIterator<Item = &'a Network>) -> Self {
        Self {
            notifiers: networks
                .into_iter()
                .map(|network| (network.clone(), Arc::new(Notify::new())))
                .collect(),
        }
    }

    pub fn notifier(&self, network: &Network) -> Option<Arc<Notify>> {
        self.notifiers.get(network).cloned()
    }
}

impl DeliveryWaker for NotifyWaker {
    fn wake(&self, network: &Network) {
        if let Some(notify) = self.notifiers.get(network) {
            notify.notify_one();
        }
    }
}
