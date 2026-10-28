//! Debug aid: periodically compares the published view with a fresh REST snapshot.
//!
//! The two are never taken at the same instant, so a few differing levels near the touch
//! are expected; a book that has drifted shows up as most levels mismatching.

use std::collections::HashSet;
use std::time::Duration;

use tracing::{info, warn};

use crate::book::Level;
use crate::feed::fetch_snapshot;
use crate::view::{FeedStatus, ViewCell};

const COMPARE_LEVELS: usize = 10;

pub async fn run(
    venue: &'static str,
    url: String,
    cell: ViewCell,
    http: reqwest::Client,
    every: Duration,
) {
    let mut ticker = tokio::time::interval(every);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let snap = match fetch_snapshot(&http, &url).await {
            Ok(s) => s,
            Err(e) => {
                warn!(venue, error = format!("{e:#}"), "verify: snapshot failed");
                continue;
            }
        };
        let view = cell.load();
        if view.status != FeedStatus::Live {
            info!(venue, status = %view.status, "verify: skipped, feed not live");
            continue;
        }
        let bids = matching(&view.bids, &snap.bids);
        let asks = matching(&view.asks, &snap.asks);
        info!(
            venue,
            matched_bids = format!("{bids}/{COMPARE_LEVELS}"),
            matched_asks = format!("{asks}/{COMPARE_LEVELS}"),
            view_id = view.last_update_id,
            rest_id = snap.last_update_id,
            "verify: view vs REST snapshot"
        );
    }
}

fn matching(view: &[Level], rest: &[Level]) -> usize {
    let rest: HashSet<_> = rest
        .iter()
        .take(COMPARE_LEVELS)
        .map(|l| (l.price, l.qty))
        .collect();
    view.iter()
        .take(COMPARE_LEVELS)
        .filter(|l| rest.contains(&(l.price, l.qty)))
        .count()
}
