use std::time::Duration;

/// Exponential reconnect backoff with jitter.
///
/// The nominal delay doubles per failure up to `max`; the actual delay is drawn from
/// `[nominal/2, nominal]` so that both feeds (or many instances) don't reconnect in lockstep.
#[derive(Debug)]
pub struct Backoff {
    base: Duration,
    max: Duration,
    nominal: Duration,
}

impl Backoff {
    pub fn new(base: Duration, max: Duration) -> Self {
        Self {
            base,
            max,
            nominal: base,
        }
    }

    pub fn next_delay(&mut self) -> Duration {
        let nominal = self.nominal;
        self.nominal = (self.nominal * 2).min(self.max);
        let half = nominal / 2;
        half + half.mul_f64(fastrand::f64())
    }

    pub fn reset(&mut self) {
        self.nominal = self.base;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grows_exponentially_and_is_capped() {
        let mut b = Backoff::new(Duration::from_millis(100), Duration::from_millis(1000));
        let bounds = [100, 200, 400, 800, 1000, 1000];
        for nominal in bounds {
            let d = b.next_delay().as_millis() as u64;
            assert!(
                (nominal / 2..=nominal).contains(&d),
                "{d}ms not in [{}, {nominal}]",
                nominal / 2
            );
        }
    }

    #[test]
    fn reset_returns_to_base() {
        let mut b = Backoff::new(Duration::from_millis(100), Duration::from_secs(10));
        for _ in 0..5 {
            b.next_delay();
        }
        b.reset();
        assert!(b.next_delay() <= Duration::from_millis(100));
    }
}
