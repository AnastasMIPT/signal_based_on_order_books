use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;

use crate::book::{Level, OrderBook};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedStatus {
    /// Opening the WebSocket.
    Connecting,
    /// Stream is open; waiting for snapshot / bridge event. Book data is not usable.
    Syncing,
    /// Book is consistent with the stream.
    Live,
    /// Book broke (gap, crossed); rebuilding from a new snapshot on the same connection.
    Resyncing,
    /// Connection lost; waiting before reconnecting. Book data is not usable.
    Disconnected,
}

impl fmt::Display for FeedStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            FeedStatus::Connecting => "connecting",
            FeedStatus::Syncing => "syncing",
            FeedStatus::Live => "live",
            FeedStatus::Resyncing => "resyncing",
            FeedStatus::Disconnected => "disconnected",
        };
        f.write_str(s)
    }
}

/// Lifetime counters of one feed, maintained by its task.
#[derive(Debug, Clone, Copy, Default)]
pub struct FeedStats {
    pub connects: u64,
    pub resyncs: u64,
    pub bad_frames: u64,
    pub events: u64,
}

/// Feed health published alongside every view.
#[derive(Debug, Clone, Copy)]
pub struct FeedHealth {
    pub status_since: Instant,
    pub stats: FeedStats,
}

/// Immutable, internally consistent picture of one venue's book that readers consume.
///
/// The feed task is the only writer: after each applied update it builds a new view and
/// swaps it in atomically. Readers get either the previous or the next view, never a mix.
#[derive(Debug, Clone)]
pub struct BookView {
    pub venue: &'static str,
    pub status: FeedStatus,
    pub health: FeedHealth,
    /// Best levels first. Empty unless `status == Live`.
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub last_update_id: Option<u64>,
    /// Exchange event time of the last applied update, ms since epoch.
    pub exchange_time_ms: u64,
    /// Local time at which this view was published.
    pub published_at: Instant,
}

impl BookView {
    pub fn empty(venue: &'static str, status: FeedStatus) -> Self {
        let now = Instant::now();
        Self::status_only(
            venue,
            status,
            FeedHealth {
                status_since: now,
                stats: FeedStats::default(),
            },
        )
    }

    pub fn status_only(venue: &'static str, status: FeedStatus, health: FeedHealth) -> Self {
        Self {
            venue,
            status,
            health,
            bids: Vec::new(),
            asks: Vec::new(),
            last_update_id: None,
            exchange_time_ms: 0,
            published_at: Instant::now(),
        }
    }

    pub fn from_book(
        venue: &'static str,
        book: &OrderBook,
        levels: usize,
        last_update_id: Option<u64>,
        exchange_time_ms: u64,
        health: FeedHealth,
    ) -> Self {
        Self {
            venue,
            status: FeedStatus::Live,
            health,
            bids: book.top_bids(levels),
            asks: book.top_asks(levels),
            last_update_id,
            exchange_time_ms,
            published_at: Instant::now(),
        }
    }

    pub fn best_bid(&self) -> Option<Level> {
        self.bids.first().copied()
    }

    pub fn best_ask(&self) -> Option<Level> {
        self.asks.first().copied()
    }
}

/// Single-writer, many-reader cell holding the latest view of one venue.
pub type ViewCell = Arc<ArcSwap<BookView>>;

pub fn new_cell(venue: &'static str) -> ViewCell {
    Arc::new(ArcSwap::from_pointee(BookView::empty(
        venue,
        FeedStatus::Connecting,
    )))
}
