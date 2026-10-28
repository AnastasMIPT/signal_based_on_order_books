use std::sync::Arc;

use anyhow::{Context, bail};
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

use crate::sync::{BookSync, DepthSnapshot, DepthUpdate, UpdateOutcome};
use crate::venue::FeedConfig;
use crate::view::{BookView, FeedStatus, ViewCell};

const MAX_SNAPSHOT_ATTEMPTS: u32 = 3;

/// Runs one venue forever: connect, sync, publish views; on any failure mark the
/// view as disconnected and start over.
pub async fn run(cfg: FeedConfig, cell: ViewCell, http: reqwest::Client) {
    loop {
        publish_status(&cfg, &cell, FeedStatus::Connecting);
        if let Err(e) = session(&cfg, &cell, &http).await {
            warn!(
                venue = cfg.venue,
                error = format!("{e:#}"),
                "feed session ended"
            );
        }
        publish_status(&cfg, &cell, FeedStatus::Disconnected);
        sleep(cfg.reconnect_delay).await;
    }
}

async fn session(cfg: &FeedConfig, cell: &ViewCell, http: &reqwest::Client) -> anyhow::Result<()> {
    let (mut ws, _) = timeout(cfg.read_timeout, connect_async(cfg.ws_url.as_str()))
        .await
        .context("connect timeout")?
        .context("connect")?;
    info!(venue = cfg.venue, url = cfg.ws_url, "websocket connected");
    publish_status(cfg, cell, FeedStatus::Syncing);

    let mut sync = BookSync::new(cfg.max_buffer);
    // The REST snapshot is fetched in a separate task so the stream keeps being read
    // (and buffered) while the request is in flight.
    let (snap_tx, mut snap_rx) = mpsc::channel::<anyhow::Result<DepthSnapshot>>(1);
    let mut snapshot_attempts = 0u32;
    let mut was_live = false;

    loop {
        let mut changed = false;
        tokio::select! {
            Some(res) = snap_rx.recv() => {
                let snap = res.context("snapshot request")?;
                let snapshot_id = snap.last_update_id;
                let buffered = sync.buffered();
                match sync.on_snapshot(snap) {
                    Ok(()) => {
                        debug!(venue = cfg.venue, snapshot_id, buffered, phase = ?sync.phase(), "snapshot applied");
                        changed = true;
                    }
                    Err(e) if e.is_retryable_snapshot() && snapshot_attempts < MAX_SNAPSHOT_ATTEMPTS => {
                        warn!(venue = cfg.venue, error = %e, "refetching snapshot");
                        snapshot_attempts += 1;
                        spawn_snapshot(http, cfg, &snap_tx);
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            frame = timeout(cfg.read_timeout, ws.next()) => {
                let msg = match frame {
                    Err(_) => bail!("no frames for {:?}", cfg.read_timeout),
                    Ok(None) => bail!("stream ended"),
                    Ok(Some(msg)) => msg.context("websocket read")?,
                };
                match msg {
                    Message::Text(text) => {
                        let ev: DepthUpdate = match serde_json::from_str(text.as_str()) {
                            Ok(ev) => ev,
                            Err(e) => {
                                warn!(venue = cfg.venue, error = %e, "unparseable frame skipped");
                                continue;
                            }
                        };
                        match sync.on_update(ev)? {
                            UpdateOutcome::Buffered if snapshot_attempts == 0 => {
                                snapshot_attempts = 1;
                                spawn_snapshot(http, cfg, &snap_tx);
                            }
                            UpdateOutcome::Applied => changed = true,
                            UpdateOutcome::Buffered | UpdateOutcome::Skipped => {}
                        }
                    }
                    Message::Close(frame) => bail!("closed by server: {frame:?}"),
                    // Pings are answered by tungstenite itself.
                    _ => continue,
                }
            }
        }

        if changed && sync.is_live() {
            if sync.book().is_crossed() {
                bail!(
                    "book crossed: bid {:?} >= ask {:?}",
                    sync.book().best_bid(),
                    sync.book().best_ask()
                );
            }
            if !was_live {
                was_live = true;
                let (bids, asks) = sync.book().depth();
                info!(
                    venue = cfg.venue,
                    last_update_id = sync.last_update_id(),
                    bids,
                    asks,
                    "book live"
                );
            }
            cell.store(Arc::new(BookView::from_book(
                cfg.venue,
                sync.book(),
                cfg.view_levels,
                sync.last_update_id(),
                sync.exchange_time_ms(),
            )));
        }
    }
}

fn spawn_snapshot(
    http: &reqwest::Client,
    cfg: &FeedConfig,
    tx: &mpsc::Sender<anyhow::Result<DepthSnapshot>>,
) {
    let http = http.clone();
    let url = cfg.snapshot_url.clone();
    let tx = tx.clone();
    tokio::spawn(async move {
        let _ = tx.send(fetch_snapshot(&http, &url).await).await;
    });
}

pub async fn fetch_snapshot(http: &reqwest::Client, url: &str) -> anyhow::Result<DepthSnapshot> {
    let snap = http
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json::<DepthSnapshot>()
        .await?;
    Ok(snap)
}

fn publish_status(cfg: &FeedConfig, cell: &ViewCell, status: FeedStatus) {
    cell.store(Arc::new(BookView::empty(cfg.venue, status)));
}
