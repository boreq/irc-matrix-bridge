use crate::domain::Channel;
use crate::domain::time::DateTime;

/// The oldest persisted pending message for a destination, including delayed retries.
#[derive(Clone, Debug)]
pub struct PendingDestination {
    destination: Channel,
    received_at: DateTime,
}

impl PendingDestination {
    pub fn new(destination: Channel, received_at: DateTime) -> Self {
        Self {
            destination,
            received_at,
        }
    }

    pub fn destination(&self) -> &Channel {
        &self.destination
    }

    pub fn received_at(&self) -> DateTime {
        self.received_at
    }
}
