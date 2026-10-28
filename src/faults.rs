//! Deliberate fault injection for exercising recovery paths against live venues.

use std::time::Duration;

#[derive(Debug, Clone, Default)]
pub struct Faults {
    /// Silently discard every N-th frame (simulates a lost message → sequence gap).
    pub drop_every: Option<u64>,
    /// Replace every N-th frame with garbage (simulates a corrupted frame).
    pub garbage_every: Option<u64>,
    /// After this long into a session, ignore all frames (simulates a stalled connection).
    pub mute_after: Option<Duration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    Drop,
    Garbage,
    Mute,
}

impl Faults {
    pub fn is_empty(&self) -> bool {
        self.drop_every.is_none() && self.garbage_every.is_none() && self.mute_after.is_none()
    }

    /// `frame_no` is 1-based within the session.
    pub fn check(&self, frame_no: u64, session_age: Duration) -> Option<Fault> {
        if self.mute_after.is_some_and(|t| session_age >= t) {
            return Some(Fault::Mute);
        }
        if self
            .drop_every
            .is_some_and(|n| n > 0 && frame_no.is_multiple_of(n))
        {
            return Some(Fault::Drop);
        }
        if self
            .garbage_every
            .is_some_and(|n| n > 0 && frame_no.is_multiple_of(n))
        {
            return Some(Fault::Garbage);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_faults_by_default() {
        let f = Faults::default();
        assert!(f.is_empty());
        assert_eq!(f.check(100, Duration::from_secs(1000)), None);
    }

    #[test]
    fn periodic_faults() {
        let f = Faults {
            drop_every: Some(3),
            garbage_every: Some(5),
            ..Default::default()
        };
        let got: Vec<_> = (1..=6).map(|n| f.check(n, Duration::ZERO)).collect();
        use Fault::*;
        assert_eq!(
            got,
            [None, None, Some(Drop), None, Some(Garbage), Some(Drop)]
        );
    }

    #[test]
    fn mute_after_deadline() {
        let f = Faults {
            mute_after: Some(Duration::from_secs(5)),
            ..Default::default()
        };
        assert_eq!(f.check(1, Duration::from_secs(4)), None);
        assert_eq!(f.check(2, Duration::from_secs(5)), Some(Fault::Mute));
    }
}
