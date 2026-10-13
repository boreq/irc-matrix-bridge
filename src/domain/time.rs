use std::ops::Add;
use std::time::Duration;

use chrono::{TimeDelta, Utc};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DateTime {
    dt: chrono::DateTime<Utc>,
}

impl DateTime {
    pub fn now() -> Self {
        Self { dt: Utc::now() }
    }

    pub fn from_unix_millis(unix_millis: i64) -> Option<Self> {
        chrono::DateTime::from_timestamp_millis(unix_millis).map(|dt| Self { dt })
    }

    pub fn unix_millis(self) -> i64 {
        self.dt.timestamp_millis()
    }

    pub fn duration_until(self, later: DateTime) -> Duration {
        (later.dt - self.dt).to_std().unwrap_or(Duration::ZERO)
    }
}

impl Add<Duration> for DateTime {
    type Output = Self;

    fn add(self, duration: Duration) -> Self {
        let delta = TimeDelta::from_std(duration).unwrap_or(TimeDelta::MAX);
        Self {
            dt: self
                .dt
                .checked_add_signed(delta)
                .unwrap_or(chrono::DateTime::<Utc>::MAX_UTC),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adds_durations_and_measures_time_until_later_moments() {
        let start = DateTime::from_unix_millis(1_000).unwrap();
        let later = start + Duration::from_secs(2);

        assert_eq!(later.unix_millis(), 3_000);
        assert_eq!(start.duration_until(later), Duration::from_secs(2));
        assert_eq!(later.duration_until(start), Duration::ZERO);
    }
}
