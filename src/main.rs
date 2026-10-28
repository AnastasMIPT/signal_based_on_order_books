mod book;
mod feed;
mod fixed;
mod signal;
mod sync;
mod venue;
mod verify;
mod view;

use std::time::{Duration, Instant};

use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::signal::SignalParams;
use crate::venue::FeedConfig;

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

    // No WS messages => session termination
    #[arg(long, default_value_t = 10)]
    read_timeout_secs: u64,

    #[arg(long, default_value_t = 2000)]
    reconnect_delay_ms: u64,
    /// Periodically compare each published book with a fresh REST snapshot
    #[arg(long)]
    verify_every_secs: Option<u64>,
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

    let feed_cfg = |venue: &'static str, (ws_url, snapshot_url): (String, String)| FeedConfig {
        venue,
        ws_url,
        snapshot_url,
        view_levels: args.view_levels,
        read_timeout: Duration::from_secs(args.read_timeout_secs),
        reconnect_delay: Duration::from_millis(args.reconnect_delay_ms),
        max_buffer: 10_000,
    };
    let binance_cfg = feed_cfg("binance", venue::binance_futures(&args.symbol));
    let aster_cfg = feed_cfg("aster", venue::aster_futures(&args.symbol));

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
    tokio::spawn(feed::run(binance_cfg, binance.clone(), http.clone()));
    tokio::spawn(feed::run(aster_cfg, aster.clone(), http.clone()));

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
