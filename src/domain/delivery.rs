use std::time::Duration;

use crate::domain::channel::Channel;
use crate::domain::message::{Message, MessageId};
use crate::domain::time::DateTime;

#[derive(Clone, Debug)]
pub struct Delivery {
    id: MessageId,
    message: Message,
    destination: Channel,
    attempts: u32,
    next_attempt_at: DateTime,
}

impl Delivery {
    pub fn new(
        id: MessageId,
        message: Message,
        destination: Channel,
        attempts: u32,
        next_attempt_at: DateTime,
    ) -> Self {
        Self {
            id,
            message,
            destination,
            attempts,
            next_attempt_at,
        }
    }

    pub fn id(&self) -> MessageId {
        self.id
    }

    pub fn message(&self) -> &Message {
        &self.message
    }

    pub fn destination(&self) -> &Channel {
        &self.destination
    }

    pub fn attempts(&self) -> u32 {
        self.attempts
    }

    pub fn next_attempt_at(&self) -> DateTime {
        self.next_attempt_at
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    max_attempts: u32,
    base_delay: Duration,
    max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 30,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(10 * 60),
        }
    }
}

impl RetryPolicy {
    pub fn new(max_attempts: u32, base_delay: Duration, max_delay: Duration) -> Self {
        Self {
            max_attempts,
            base_delay,
            max_delay,
        }
    }

    pub fn delay_after(&self, failed_attempts: u32) -> Option<Duration> {
        if failed_attempts >= self.max_attempts {
            return None;
        }
        let exp = failed_attempts.saturating_sub(1).min(30);
        Some(self.base_delay.saturating_mul(1 << exp).min(self.max_delay))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backs_off_exponentially_and_gives_up() {
        let policy = RetryPolicy::new(5, Duration::from_millis(100), Duration::from_millis(500));
        assert_eq!(policy.delay_after(1), Some(Duration::from_millis(100)));
        assert_eq!(policy.delay_after(2), Some(Duration::from_millis(200)));
        assert_eq!(policy.delay_after(3), Some(Duration::from_millis(400)));
        assert_eq!(policy.delay_after(4), Some(Duration::from_millis(500)));
        assert_eq!(policy.delay_after(5), None);
    }
}
