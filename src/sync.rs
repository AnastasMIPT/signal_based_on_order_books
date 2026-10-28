//! Snapshot + diff-stream synchronisation for Binance USD-M style depth feeds.
//!
//! Aster futures exposes the same protocol, so one implementation serves both venues.
//! Rules (Binance "How to manage a local order book correctly", USD-M futures):
//! 1. Buffer diff events from the stream before the snapshot arrives.
//! 2. Drop events with `u < lastUpdateId`.
//! 3. The first applied event must satisfy `U <= lastUpdateId <= u`.
//! 4. Every following event must have `pu == previous u`, otherwise the book is broken.
//!
//! This module performs no I/O, so all of the above is unit-tested directly.

use std::collections::VecDeque;

use serde::Deserialize;

use crate::book::{Level, OrderBook};
use crate::fixed::Fixed;

#[derive(Debug, Clone, Deserialize)]
pub struct DepthUpdate {
    /// Event time, exchange clock, ms.
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "U")]
    pub first_id: u64,
    #[serde(rename = "u")]
    pub final_id: u64,
    #[serde(rename = "pu")]
    pub prev_final_id: u64,
    #[serde(rename = "b")]
    pub bids: Vec<Level>,
    #[serde(rename = "a")]
    pub asks: Vec<Level>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DepthSnapshot {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,
    #[serde(rename = "E", default)]
    pub event_time: u64,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Stream is open, diffs are buffered, no usable book yet.
    AwaitingSnapshot,
    /// Snapshot applied; waiting for the event that bridges it (`U <= id <= u`).
    AwaitingBridge { snapshot_id: u64 },
    /// Book is consistent with the stream up to `last_id`.
    Live { last_id: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateOutcome {
    Buffered,
    /// Event is fully covered by the snapshot.
    Skipped,
    Applied,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SyncError {
    #[error("sequence gap: expected pu={expected}, got pu={got} (U={first_id}, u={final_id})")]
    Gap {
        expected: u64,
        got: u64,
        first_id: u64,
        final_id: u64,
    },
    #[error("first event after snapshot {snapshot_id} starts at U={first_id}, updates were missed")]
    MissedBridge { snapshot_id: u64, first_id: u64 },
    #[error("snapshot {snapshot_id} is older than buffered stream (first U={first_id}), refetch")]
    SnapshotTooOld { snapshot_id: u64, first_id: u64 },
    #[error("pre-snapshot buffer overflow ({0} events)")]
    BufferOverflow(usize),
    #[error("book crossed after u={final_id}: best bid {bid} >= best ask {ask}")]
    Crossed {
        final_id: u64,
        bid: Fixed,
        ask: Fixed,
    },
}

impl SyncError {
    /// Recoverable by fetching a newer snapshot while keeping the stream and buffer.
    pub fn is_retryable_snapshot(&self) -> bool {
        matches!(self, SyncError::SnapshotTooOld { .. })
    }
}

#[derive(Debug)]
pub struct BookSync {
    book: OrderBook,
    phase: Phase,
    buffer: VecDeque<DepthUpdate>,
    max_buffer: usize,
    exchange_time_ms: u64,
}

impl BookSync {
    pub fn new(max_buffer: usize) -> Self {
        Self {
            book: OrderBook::default(),
            phase: Phase::AwaitingSnapshot,
            buffer: VecDeque::new(),
            max_buffer,
            exchange_time_ms: 0,
        }
    }

    /// Exchange event time of the last applied snapshot or update, ms since epoch.
    pub fn exchange_time_ms(&self) -> u64 {
        self.exchange_time_ms
    }

    pub fn book(&self) -> &OrderBook {
        &self.book
    }

    pub fn phase(&self) -> Phase {
        self.phase
    }

    pub fn is_live(&self) -> bool {
        matches!(self.phase, Phase::Live { .. })
    }

    pub fn buffered(&self) -> usize {
        self.buffer.len()
    }

    pub fn last_update_id(&self) -> Option<u64> {
        match self.phase {
            Phase::Live { last_id } => Some(last_id),
            Phase::AwaitingBridge { snapshot_id } => Some(snapshot_id),
            Phase::AwaitingSnapshot => None,
        }
    }

    pub fn on_update(&mut self, ev: DepthUpdate) -> Result<UpdateOutcome, SyncError> {
        if self.phase == Phase::AwaitingSnapshot {
            if self.buffer.len() >= self.max_buffer {
                return Err(SyncError::BufferOverflow(self.buffer.len()));
            }
            self.buffer.push_back(ev);
            return Ok(UpdateOutcome::Buffered);
        }
        self.apply_event(&ev)
    }

    /// On error the book is left untouched (`SnapshotTooOld`) or must be `reset()`.
    pub fn on_snapshot(&mut self, snap: DepthSnapshot) -> Result<(), SyncError> {
        if self.phase != Phase::AwaitingSnapshot {
            return Ok(());
        }
        let id = snap.last_update_id;
        while self.buffer.front().is_some_and(|ev| ev.final_id < id) {
            self.buffer.pop_front();
        }
        if let Some(first) = self.buffer.front()
            && first.first_id > id
        {
            return Err(SyncError::SnapshotTooOld {
                snapshot_id: id,
                first_id: first.first_id,
            });
        }

        self.book.clear();
        self.book.apply(&snap.bids, &snap.asks);
        self.exchange_time_ms = snap.event_time;
        self.phase = Phase::AwaitingBridge { snapshot_id: id };

        while let Some(ev) = self.buffer.pop_front() {
            self.apply_event(&ev)?;
        }
        Ok(())
    }

    fn apply_event(&mut self, ev: &DepthUpdate) -> Result<UpdateOutcome, SyncError> {
        match self.phase {
            Phase::AwaitingSnapshot => unreachable!("events are buffered until the snapshot"),
            Phase::AwaitingBridge { snapshot_id } => {
                if ev.final_id < snapshot_id {
                    return Ok(UpdateOutcome::Skipped);
                }
                if ev.first_id > snapshot_id {
                    return Err(SyncError::MissedBridge {
                        snapshot_id,
                        first_id: ev.first_id,
                    });
                }
            }
            Phase::Live { last_id } => {
                if ev.prev_final_id != last_id {
                    return Err(SyncError::Gap {
                        expected: last_id,
                        got: ev.prev_final_id,
                        first_id: ev.first_id,
                        final_id: ev.final_id,
                    });
                }
            }
        }
        self.book.apply(&ev.bids, &ev.asks);
        if self.book.is_crossed() {
            let price = |l: Option<Level>| l.map(|l| l.price).unwrap_or_default();
            return Err(SyncError::Crossed {
                final_id: ev.final_id,
                bid: price(self.book.best_bid()),
                ask: price(self.book.best_ask()),
            });
        }
        self.exchange_time_ms = ev.event_time;
        self.phase = Phase::Live {
            last_id: ev.final_id,
        };
        Ok(UpdateOutcome::Applied)
    }

    /// Drops the book and buffer; the next event starts buffering for a new snapshot.
    pub fn reset(&mut self) {
        self.book.clear();
        self.buffer.clear();
        self.exchange_time_ms = 0;
        self.phase = Phase::AwaitingSnapshot;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixed::Fixed;

    fn lv(p: &str, q: &str) -> Level {
        Level::new(Fixed::parse(p).unwrap(), Fixed::parse(q).unwrap())
    }

    fn ev(first: u64, last: u64, prev: u64, bids: &[Level]) -> DepthUpdate {
        DepthUpdate {
            event_time: 0,
            first_id: first,
            final_id: last,
            prev_final_id: prev,
            bids: bids.to_vec(),
            asks: vec![],
        }
    }

    fn snap(id: u64) -> DepthSnapshot {
        DepthSnapshot {
            last_update_id: id,
            event_time: 0,
            bids: vec![lv("100", "1")],
            asks: vec![lv("101", "1")],
        }
    }

    #[test]
    fn buffers_until_snapshot_then_bridges() {
        let mut s = BookSync::new(100);
        // Stream before snapshot id 25: [10..15], [16..22], [23..30], [31..35]
        assert_eq!(
            s.on_update(ev(10, 15, 9, &[])).unwrap(),
            UpdateOutcome::Buffered
        );
        s.on_update(ev(16, 22, 15, &[lv("99", "7")])).unwrap();
        s.on_update(ev(23, 30, 22, &[lv("100", "2")])).unwrap();
        s.on_update(ev(31, 35, 30, &[lv("100", "3")])).unwrap();

        s.on_snapshot(snap(25)).unwrap();
        assert_eq!(s.phase(), Phase::Live { last_id: 35 });
        assert_eq!(s.buffered(), 0);
        // [16..22] predates the snapshot and must not have been applied.
        assert_eq!(s.book().top_bids(5), vec![lv("100", "3")]);

        assert_eq!(
            s.on_update(ev(36, 40, 35, &[])).unwrap(),
            UpdateOutcome::Applied
        );
        assert_eq!(s.last_update_id(), Some(40));
    }

    #[test]
    fn bridge_event_may_end_exactly_at_snapshot_id() {
        let mut s = BookSync::new(100);
        s.on_update(ev(20, 25, 19, &[lv("100", "9")])).unwrap();
        s.on_snapshot(snap(25)).unwrap();
        assert_eq!(s.phase(), Phase::Live { last_id: 25 });
    }

    #[test]
    fn snapshot_with_empty_buffer_waits_for_bridge() {
        let mut s = BookSync::new(100);
        s.on_snapshot(snap(25)).unwrap();
        assert_eq!(s.phase(), Phase::AwaitingBridge { snapshot_id: 25 });
        assert_eq!(
            s.on_update(ev(10, 20, 9, &[])).unwrap(),
            UpdateOutcome::Skipped
        );
        assert_eq!(
            s.on_update(ev(21, 30, 20, &[])).unwrap(),
            UpdateOutcome::Applied
        );
        assert!(s.is_live());
    }

    #[test]
    fn missing_bridge_is_an_error() {
        let mut s = BookSync::new(100);
        s.on_snapshot(snap(25)).unwrap();
        let err = s.on_update(ev(30, 40, 29, &[])).unwrap_err();
        assert!(matches!(
            err,
            SyncError::MissedBridge {
                snapshot_id: 25,
                first_id: 30
            }
        ));
    }

    #[test]
    fn snapshot_older_than_buffer_is_retryable() {
        let mut s = BookSync::new(100);
        s.on_update(ev(30, 40, 29, &[])).unwrap();
        let err = s.on_snapshot(snap(25)).unwrap_err();
        assert!(err.is_retryable_snapshot());
        assert_eq!(s.phase(), Phase::AwaitingSnapshot);
        assert_eq!(s.buffered(), 1);

        s.on_update(ev(41, 45, 40, &[])).unwrap();
        s.on_snapshot(snap(42)).unwrap();
        assert_eq!(s.phase(), Phase::Live { last_id: 45 });
    }

    #[test]
    fn gap_in_live_stream_is_detected() {
        let mut s = BookSync::new(100);
        s.on_update(ev(20, 30, 19, &[])).unwrap();
        s.on_snapshot(snap(25)).unwrap();
        let err = s.on_update(ev(36, 40, 35, &[])).unwrap_err();
        assert_eq!(
            err,
            SyncError::Gap {
                expected: 30,
                got: 35,
                first_id: 36,
                final_id: 40
            }
        );
    }

    #[test]
    fn gap_inside_buffer_is_detected_on_snapshot() {
        let mut s = BookSync::new(100);
        s.on_update(ev(20, 30, 19, &[])).unwrap();
        s.on_update(ev(36, 40, 35, &[])).unwrap();
        assert!(matches!(
            s.on_snapshot(snap(25)),
            Err(SyncError::Gap { .. })
        ));
    }

    #[test]
    fn buffer_is_bounded() {
        let mut s = BookSync::new(2);
        s.on_update(ev(1, 1, 0, &[])).unwrap();
        s.on_update(ev(2, 2, 1, &[])).unwrap();
        assert_eq!(
            s.on_update(ev(3, 3, 2, &[])),
            Err(SyncError::BufferOverflow(2))
        );
    }

    #[test]
    fn crossed_book_is_an_error() {
        let mut s = BookSync::new(100);
        s.on_update(ev(20, 30, 19, &[])).unwrap();
        s.on_snapshot(snap(25)).unwrap();
        let err = s.on_update(ev(31, 32, 30, &[lv("101", "1")])).unwrap_err();
        assert!(
            matches!(err, SyncError::Crossed { final_id: 32, .. }),
            "{err}"
        );
    }

    #[test]
    fn reset_allows_full_resync_on_same_stream() {
        let mut s = BookSync::new(100);
        s.on_update(ev(20, 30, 19, &[])).unwrap();
        s.on_snapshot(snap(25)).unwrap();
        assert!(s.on_update(ev(36, 40, 35, &[])).is_err());

        s.reset();
        assert_eq!(s.phase(), Phase::AwaitingSnapshot);
        assert_eq!(s.book().depth(), (0, 0));
        assert_eq!(
            s.on_update(ev(41, 45, 40, &[])).unwrap(),
            UpdateOutcome::Buffered
        );
        s.on_snapshot(snap(43)).unwrap();
        assert_eq!(s.phase(), Phase::Live { last_id: 45 });
    }

    #[test]
    fn parses_futures_depth_event() {
        let raw = r#"{"e":"depthUpdate","E":1793120883789,"T":1793120883750,"s":"ETHUSDT",
            "U":100,"u":105,"pu":99,"b":[["2352.05","2.760"]],"a":[["2352.06","0"]]}"#;
        let ev: DepthUpdate = serde_json::from_str(raw).unwrap();
        assert_eq!((ev.first_id, ev.final_id, ev.prev_final_id), (100, 105, 99));
        assert_eq!(ev.event_time, 1793120883789);
        assert_eq!(ev.bids, vec![lv("2352.05", "2.76")]);
        assert!(ev.asks[0].qty.is_zero());
    }
}
