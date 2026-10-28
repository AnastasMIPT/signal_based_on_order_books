mod backoff;
mod book;
mod faults;
mod feed;
mod fixed;
mod signal;
mod status;
mod sync;
mod venue;
mod verify;
mod view;

use std::time::{Duration, Instant};

use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::faults::Faults;
use crate::signal::SignalParams;
use crate::venue::FeedConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Venue {
    Binance,
    Aster,
}

#[derive(Debug, Parser)]
#[command(about = "Binance + Aster futures order books and a cross-venue signal")]
struct Args {
    #[arg(long, default_value = "ETHUSDT")]
    symbol: String,
    /// Levels per side published from each book to the signal
    #[arg(long, default_value_t = 20)]
    view_levels: usize,
    // signal::evaluate frequency
    #[arg(long, default_value_t = 1000)]
    signal_interval_ms: u64,
    // View older than stale_ms => one of venue stale => HOLD
    #[arg(long, default_value_t = 3000)]
    stale_ms: u64,
    // threshold for difference between mid prices from venues
    #[arg(long, default_value_t = 2.0)]
    threshold_bps: f64,

    // No depth events => session termination
    #[arg(long, default_value_t = 5)]
    read_timeout_secs: u64,
    // WebSocket handshake (DNS + TCP + TLS + upgrade) deadline
    #[arg(long, default_value_t = 5)]
    connect_timeout_secs: u64,

    // Reconnect backoff: doubles from base up to max, with jitter
    #[arg(long, default_value_t = 500)]
    reconnect_base_ms: u64,
    #[arg(long, default_value_t = 30_000)]
    reconnect_max_ms: u64,
    /// Periodically compare each published book with a fresh REST snapshot
    #[arg(long)]
    verify_every_secs: Option<u64>,
    /// Per-feed health line interval
    #[arg(long, default_value_t = 10)]
    status_every_secs: u64,

    /// Venue that the --fault-* options apply to
    #[arg(long, value_enum)]
    fault_venue: Option<Venue>,
    /// Drop every N-th frame (causes a sequence gap → resync)
    #[arg(long, requires = "fault_venue")]
    fault_drop_every: Option<u64>,
    /// Corrupt every N-th frame (bad-frame handling)
    #[arg(long, requires = "fault_venue")]
    fault_garbage_every: Option<u64>,
    /// Ignore all frames after N seconds into each session (watchdog → reconnect)
    #[arg(long, requires = "fault_venue")]
    fault_mute_after_secs: Option<u64>,
}

impl Args {
    fn faults_for(&self, venue: Venue) -> Faults {
        if self.fault_venue != Some(venue) {
            return Faults::default();
        }
        Faults {
            drop_every: self.fault_drop_every,
            garbage_every: self.fault_garbage_every,
            mute_after: self.fault_mute_after_secs.map(Duration::from_secs),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let args = Args::parse();

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    let feed_cfg = |venue: Venue, (ws_url, snapshot_url): (String, String)| FeedConfig {
        venue: match venue {
            Venue::Binance => "binance",
            Venue::Aster => "aster",
        },
        ws_url,
        snapshot_url,
        view_levels: args.view_levels,
        read_timeout: Duration::from_secs(args.read_timeout_secs),
        connect_timeout: Duration::from_secs(args.connect_timeout_secs),
        reconnect_base: Duration::from_millis(args.reconnect_base_ms),
        reconnect_max: Duration::from_millis(args.reconnect_max_ms),
        max_buffer: 10_000,
        faults: args.faults_for(venue),
    };
    let binance_cfg = feed_cfg(Venue::Binance, venue::binance_futures(&args.symbol));
    let aster_cfg = feed_cfg(Venue::Aster, venue::aster_futures(&args.symbol));

    let binance = view::new_cell("binance");
    let aster = view::new_cell("aster");

    if let Some(secs) = args.verify_every_secs {
        for (cfg, cell) in [(&binance_cfg, &binance), (&aster_cfg, &aster)] {
            let url = cfg.snapshot_url.replace("limit=1000", "limit=20");
            let every = Duration::from_secs(secs);
            tokio::spawn(verify::run(
                cfg.venue,
                url,
                cell.clone(),
                http.clone(),
                every,
            ));
        }
    }
    tokio::spawn(feed::supervise(binance_cfg, binance.clone(), http.clone()));
    tokio::spawn(feed::supervise(aster_cfg, aster.clone(), http.clone()));
    tokio::spawn(status::run(
        vec![binance.clone(), aster.clone()],
        Duration::from_secs(args.status_every_secs),
        Duration::from_millis(args.stale_ms),
    ));

    let params = SignalParams {
        stale_after: Duration::from_millis(args.stale_ms),
        threshold_bps: args.threshold_bps,
    };
    let mut ticker = tokio::time::interval(Duration::from_millis(args.signal_interval_ms));
    info!(symbol = args.symbol, "started");
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let decision = signal::evaluate(&binance.load(), &aster.load(), Instant::now(), &params);
                info!(bias = %decision.bias, "{}", decision.reason);
            }
            _ = tokio::signal::ctrl_c() => {
                info!("shutting down");
                return Ok(());
            }
        }
    }
}
