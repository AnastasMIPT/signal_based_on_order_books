use std::time::Duration;

use crate::faults::Faults;

/// Everything a feed task needs to know about one venue.
#[derive(Debug, Clone)]
pub struct FeedConfig {
    pub venue: &'static str,
    pub ws_url: String,
    pub snapshot_url: String,
    /// Levels per side published to readers.
    pub view_levels: usize,
    /// No depth event within this window means the connection is considered dead.
    pub read_timeout: Duration,
    pub connect_timeout: Duration,
    pub reconnect_base: Duration,
    pub reconnect_max: Duration,
    pub max_buffer: usize,
    pub faults: Faults,
}

pub const SNAPSHOT_LIMIT: u32 = 1000;

/// Binance USD-M futures and Aster futures share the same market-data protocol.
pub fn binance_futures(symbol: &str) -> (String, String) {
    (
        format!(
            "wss://fstream.binance.com/ws/{}@depth@100ms",
            symbol.to_lowercase()
        ),
        format!("https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit={SNAPSHOT_LIMIT}"),
    )
}

pub fn aster_futures(symbol: &str) -> (String, String) {
    (
        format!(
            "wss://fstream.asterdex.com/ws/{}@depth@100ms",
            symbol.to_lowercase()
        ),
        format!("https://fapi.asterdex.com/fapi/v1/depth?symbol={symbol}&limit={SNAPSHOT_LIMIT}"),
    )
}
