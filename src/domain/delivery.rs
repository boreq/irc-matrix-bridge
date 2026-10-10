use crate::domain::endpoint::Endpoint;
use crate::domain::message::Message;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DeliveryId(pub i64);

#[derive(Clone, Debug)]
pub struct Delivery {
    pub id: DeliveryId,
    pub message: Message,
    pub destination: Endpoint,
    pub attempts: u32,
    pub next_attempt_at: i64,
}

#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay_ms: i64,
    pub max_delay_ms: i64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 30,
            base_delay_ms: 1_000,
            max_delay_ms: 10 * 60 * 1_000,
        }
    }
}

impl RetryPolicy {
    pub fn delay_after(&self, failed_attempts: u32) -> Option<i64> {
        if failed_attempts >= self.max_attempts {
            return None;
        }
        let exp = failed_attempts.saturating_sub(1).min(30);
        Some(
            self.base_delay_ms
                .saturating_mul(1 << exp)
                .min(self.max_delay_ms),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backs_off_exponentially_and_gives_up() {
        let policy = RetryPolicy {
            max_attempts: 5,
            base_delay_ms: 100,
            max_delay_ms: 500,
        };
        assert_eq!(policy.delay_after(1), Some(100));
        assert_eq!(policy.delay_after(2), Some(200));
        assert_eq!(policy.delay_after(3), Some(400));
        assert_eq!(policy.delay_after(4), Some(500));
        assert_eq!(policy.delay_after(5), None);
    }
}
