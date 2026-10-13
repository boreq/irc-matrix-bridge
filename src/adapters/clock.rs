use crate::application::ports::Clock;
use crate::domain::time::DateTime;

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime {
        DateTime::now()
    }
}
