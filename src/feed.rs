use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use crate::backoff::Backoff;
use crate::faults::Fault;
use crate::sync::{BookSync, DepthSnapshot, DepthUpdate, SyncError, UpdateOutcome};
use crate::venue::FeedConfig;
use crate::view::{BookView, FeedHealth, FeedStats, FeedStatus, ViewCell};

/// Snapshot requests (including retries) allowed per (re)sync before giving up on the session.
const MAX_SNAPSHOT_ATTEMPTS: u32 = 3;
/// Resyncs in a row without reaching `Live` before the connection itself is suspected.
const MAX_CONSECUTIVE_RESYNCS: u32 = 3;
const MAX_CONSECUTIVE_BAD_FRAMES: u32 = 20;
/// A session that stayed live this long resets the reconnect backoff.
const STABLE_SESSION: Duration = Duration::from_secs(30);

/// Keeps a feed task running: if it ever panics, the view is marked disconnected and the
/// task is started again, so a bug never leaves the venue silently dead.
pub async fn supervise(cfg: FeedConfig, cell: ViewCell, http: reqwest::Client) {
    loop {
        let task = tokio::spawn(run(cfg.clone(), cell.clone(), http.clone()));
        let outcome = task.await;
        error!(
            venue = cfg.venue,
            ?outcome,
            "feed task terminated, restarting"
        );
        cell.store(Arc::new(BookView::empty(
            cfg.venue,
            FeedStatus::Disconnected,
        )));
        sleep(Duration::from_secs(1)).await;
    }
}

/// Sole writer of one venue's view.
struct Publisher<'a> {
    cfg: &'a FeedConfig,
    cell: &'a ViewCell,
    status: FeedStatus,
    status_since: Instant,
    stats: FeedStats,
}

impl<'a> Publisher<'a> {
    fn new(cfg: &'a FeedConfig, cell: &'a ViewCell) -> Self {
        Self {
            cfg,
            cell,
            status: FeedStatus::Connecting,
            status_since: Instant::now(),
            stats: FeedStats::default(),
        }
    }

    fn health(&self) -> FeedHealth {
        FeedHealth {
            status_since: self.status_since.into_std(),
            stats: self.stats,
        }
    }

    fn live_for(&self) -> Option<Duration> {
        (self.status == FeedStatus::Live).then(|| self.status_since.elapsed())
    }

    /// Publishes a non-live status: readers immediately stop using this venue's book.
    fn set_status(&mut self, status: FeedStatus, reason: &str) {
        debug_assert_ne!(status, FeedStatus::Live);
        self.transition(status, reason);
        self.cell.store(Arc::new(BookView::status_only(
            self.cfg.venue,
            status,
            self.health(),
        )));
    }

    fn publish_book(&mut self, sync: &BookSync) {
        if self.status != FeedStatus::Live {
            let (bids, asks) = sync.book().depth();
            self.transition(FeedStatus::Live, "book synced");
            info!(
                venue = self.cfg.venue,
                last_update_id = sync.last_update_id(),
                bids,
                asks,
                "book live"
            );
        }
        self.cell.store(Arc::new(BookView::from_book(
            self.cfg.venue,
            sync.book(),
            self.cfg.view_levels,
            sync.last_update_id(),
            sync.exchange_time_ms(),
            self.health(),
        )));
    }

    fn transition(&mut self, status: FeedStatus, reason: &str) {
        if status == self.status {
            return;
        }
        info!(venue = self.cfg.venue, from = %self.status, to = %status, reason, "feed status");
        self.status = status;
        self.status_since = Instant::now();
    }
}

async fn run(cfg: FeedConfig, cell: ViewCell, http: reqwest::Client) {
    if !cfg.faults.is_empty() {
        warn!(venue = cfg.venue, faults = ?cfg.faults, "fault injection enabled");
    }
    let mut feed = Publisher::new(&cfg, &cell);
    let mut backoff = Backoff::new(cfg.reconnect_base, cfg.reconnect_max);
    loop {
        feed.stats.connects += 1;
        feed.set_status(FeedStatus::Connecting, "opening websocket");
        let Err(err) = session(&cfg, &http, &mut feed).await;
        if feed.live_for().is_some_and(|d| d >= STABLE_SESSION) {
            backoff.reset();
        }
        let delay = backoff.next_delay();
        warn!(
            venue = cfg.venue,
            error = format!("{err:#}"),
            retry_in_ms = delay.as_millis() as u64,
            "feed session ended"
        );
        feed.set_status(FeedStatus::Disconnected, "session ended");
        sleep(delay).await;
    }
}

/// One WebSocket connection. Book problems are repaired in place (resync over the same
/// stream); only connection-level problems end the session.
async fn session(
    cfg: &FeedConfig,
    http: &reqwest::Client,
    feed: &mut Publisher<'_>,
) -> anyhow::Result<Infallible> {
    let (mut ws, _) = timeout(cfg.connect_timeout, connect_async(cfg.ws_url.as_str()))
        .await
        .context("connect timeout")?
        .context("connect")?;
    feed.set_status(FeedStatus::Syncing, "websocket connected");

    let started = Instant::now();
    let mut sync = BookSync::new(cfg.max_buffer);
    // Snapshots are fetched in a separate task so the stream keeps being read (and
    // buffered) while the request is in flight. Results are tagged with the sync generation
    // so a response requested before a resync is ignored.
    let (snap_tx, mut snap_rx) = mpsc::channel::<(u64, anyhow::Result<DepthSnapshot>)>(4);
    let mut generation = 0u64;
    let mut snapshot_attempts = 0u32;
    let mut consecutive_resyncs = 0u32;
    let mut consecutive_bad = 0u32;
    let mut frame_no = 0u64;

    // Fed only by valid depth events: pings or garbage do not keep a dead stream "alive".
    let watchdog = sleep(cfg.read_timeout);
    tokio::pin!(watchdog);

    loop {
        let mut changed = false;
        let mut broken: Option<SyncError> = None;

        tokio::select! {
            () = &mut watchdog => bail!("watchdog: no depth events for {:?}", cfg.read_timeout),

            Some((snap_gen, res)) = snap_rx.recv() => {
                if snap_gen != generation {
                    continue;
                }
                match res {
                    Ok(snap) => {
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
                                spawn_snapshot(http, cfg, &snap_tx, generation, Duration::ZERO);
                            }
                            Err(e) => broken = Some(e),
                        }
                    }
                    Err(e) if snapshot_attempts < MAX_SNAPSHOT_ATTEMPTS => {
                        let delay = Duration::from_secs(snapshot_attempts.into());
                        warn!(venue = cfg.venue, error = format!("{e:#}"), retry_in_ms = delay.as_millis() as u64, "snapshot request failed");
                        snapshot_attempts += 1;
                        spawn_snapshot(http, cfg, &snap_tx, generation, delay);
                    }
                    Err(e) => return Err(e.context("snapshot request")),
                }
            }

            msg = ws.next() => {
                let msg = match msg {
                    None => bail!("stream ended"),
                    Some(msg) => msg.context("websocket read")?,
                };
                let text = match msg {
                    Message::Text(text) => text,
                    Message::Close(frame) => bail!("closed by server: {frame:?}"),
                    // Pings are answered by tungstenite itself.
                    _ => continue,
                };
                frame_no += 1;
                let payload = match cfg.faults.check(frame_no, started.elapsed()) {
                    None => text.as_str(),
                    Some(Fault::Garbage) => "{\"garbage\"",
                    Some(Fault::Drop | Fault::Mute) => continue,
                };
                let ev: DepthUpdate = match serde_json::from_str(payload) {
                    Ok(ev) => ev,
                    Err(e) => {
                        feed.stats.bad_frames += 1;
                        consecutive_bad += 1;
                        if consecutive_bad >= MAX_CONSECUTIVE_BAD_FRAMES {
                            bail!("{consecutive_bad} unparseable frames in a row, last error: {e}");
                        }
                        warn!(venue = cfg.venue, error = %e, bad_frames = feed.stats.bad_frames, "unparseable frame skipped");
                        continue;
                    }
                };
                consecutive_bad = 0;
                feed.stats.events += 1;
                watchdog.as_mut().reset(Instant::now() + cfg.read_timeout);

                match sync.on_update(ev) {
                    Ok(UpdateOutcome::Buffered) if snapshot_attempts == 0 => {
                        snapshot_attempts = 1;
                        spawn_snapshot(http, cfg, &snap_tx, generation, Duration::ZERO);
                    }
                    Ok(UpdateOutcome::Applied) => changed = true,
                    Ok(UpdateOutcome::Buffered | UpdateOutcome::Skipped) => {}
                    Err(e) => broken = Some(e),
                }
            }
        }

        if let Some(e) = broken {
            consecutive_resyncs += 1;
            if consecutive_resyncs > MAX_CONSECUTIVE_RESYNCS {
                bail!("{MAX_CONSECUTIVE_RESYNCS} resyncs in a row failed, last: {e}");
            }
            feed.stats.resyncs += 1;
            warn!(venue = cfg.venue, error = %e, resyncs = feed.stats.resyncs, "book out of sync, resyncing on the same connection");
            sync.reset();
            generation += 1;
            snapshot_attempts = 0;
            feed.set_status(FeedStatus::Resyncing, &e.to_string());
            continue;
        }

        if changed && sync.is_live() {
            consecutive_resyncs = 0;
            feed.publish_book(&sync);
        }
    }
}

fn spawn_snapshot(
    http: &reqwest::Client,
    cfg: &FeedConfig,
    tx: &mpsc::Sender<(u64, anyhow::Result<DepthSnapshot>)>,
    generation: u64,
    delay: Duration,
) {
    let http = http.clone();
    let url = cfg.snapshot_url.clone();
    let tx = tx.clone();
    tokio::spawn(async move {
        sleep(delay).await;
        let _ = tx
            .send((generation, fetch_snapshot(&http, &url).await))
            .await;
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
