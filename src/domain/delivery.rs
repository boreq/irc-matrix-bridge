use std::time::Duration;

use crate::domain::channel::Channel;
use crate::domain::message::{Message, MessageId};
use crate::domain::time::DateTime;

#[derive(Clone, Debug)]
pub struct Delivery {
    pub id: MessageId,
    pub message: Message,
    pub destination: Channel,
    pub attempts: u32,
    pub next_attempt_at: DateTime,
}

#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
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
        let policy = RetryPolicy {
            max_attempts: 5,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(500),
        };
        assert_eq!(policy.delay_after(1), Some(Duration::from_millis(100)));
        assert_eq!(policy.delay_after(2), Some(Duration::from_millis(200)));
        assert_eq!(policy.delay_after(3), Some(Duration::from_millis(400)));
        assert_eq!(policy.delay_after(4), Some(Duration::from_millis(500)));
        assert_eq!(policy.delay_after(5), None);
    }
}
