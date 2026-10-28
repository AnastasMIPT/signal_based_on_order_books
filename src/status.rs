use std::time::{Duration, Instant};

use tracing::info;

use crate::view::{FeedStatus, ViewCell};

/// Periodic one-line health report per feed, read from the same views the signal uses.
///
/// `stale` flags a feed that still reports `live` but has not published for `stale_after`:
/// the period between the stream going quiet and the watchdog tearing the session down.
pub async fn run(cells: Vec<ViewCell>, every: Duration, stale_after: Duration) {
    let mut ticker = tokio::time::interval(every);
    ticker.tick().await;
    let mut prev_events = vec![0u64; cells.len()];
    let mut prev_at = Instant::now();
    loop {
        ticker.tick().await;
        let elapsed = prev_at.elapsed().as_secs_f64();
        prev_at = Instant::now();
        for (cell, prev) in cells.iter().zip(prev_events.iter_mut()) {
            let v = cell.load();
            let s = v.health.stats;
            let rate = s.events.saturating_sub(*prev) as f64 / elapsed;
            *prev = s.events;
            let view_age = v.published_at.elapsed();
            info!(
                venue = v.venue,
                status = %v.status,
                stale = v.status == FeedStatus::Live && view_age > stale_after,
                status_for_s = v.health.status_since.elapsed().as_secs(),
                events_per_s = format!("{rate:.1}"),
                view_age_ms = view_age.as_millis() as u64,
                last_update_id = v.last_update_id,
                connects = s.connects,
                resyncs = s.resyncs,
                bad_frames = s.bad_frames,
                "feed health"
            );
        }
    }
}
