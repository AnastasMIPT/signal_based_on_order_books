//! Cross-venue mid deviation signal.
//!
//! Binance is treated as the reference (deeper, faster) market and the bias applies to the
//! Aster ETHUSDT perpetual: if Aster's mid sits notably below Binance's we expect it to
//! catch up (Long bias), if above — to come down (Short bias). Profitability is not a goal.

use std::fmt;
use std::time::{Duration, Instant};

use crate::view::{BookView, FeedStatus};

#[derive(Debug, Clone, Copy)]
pub struct SignalParams {
    /// A venue whose view was not updated for longer than this is considered stale.
    pub stale_after: Duration,
    /// Minimum |deviation| between Aster and Binance mids, basis points.
    pub threshold_bps: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bias {
    Hold,
    Long,
    Short,
}

impl fmt::Display for Bias {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Bias::Hold => "HOLD",
            Bias::Long => "LONG_BIAS",
            Bias::Short => "SHORT_BIAS",
        })
    }
}

#[derive(Debug, Clone)]
pub struct Decision {
    pub bias: Bias,
    pub reason: String,
}

struct Top {
    bid: f64,
    ask: f64,
    mid: f64,
    spread_bps: f64,
    age: Duration,
}

impl fmt::Display for Top {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "bid={:.2} ask={:.2} mid={:.3} spread={:.2}bps age={}ms",
            self.bid,
            self.ask,
            self.mid,
            self.spread_bps,
            self.age.as_millis()
        )
    }
}

/// Usable top of book, or the reason the venue cannot be used right now.
fn top(view: &BookView, now: Instant, p: &SignalParams) -> Result<Top, String> {
    if view.status != FeedStatus::Live {
        return Err(format!("{} is {}", view.venue, view.status));
    }
    let (Some(b), Some(a)) = (view.best_bid(), view.best_ask()) else {
        return Err(format!("{} has an empty side", view.venue));
    };
    let age = now.saturating_duration_since(view.published_at);
    if age > p.stale_after {
        return Err(format!("{} stale for {}ms", view.venue, age.as_millis()));
    }
    let (bid, ask) = (b.price.to_f64(), a.price.to_f64());
    let mid = (bid + ask) / 2.0;
    Ok(Top {
        bid,
        ask,
        mid,
        spread_bps: (ask - bid) / mid * 1e4,
        age,
    })
}

pub fn evaluate(binance: &BookView, aster: &BookView, now: Instant, p: &SignalParams) -> Decision {
    let (b, a) = match (top(binance, now, p), top(aster, now, p)) {
        (Ok(b), Ok(a)) => (b, a),
        (b, a) => {
            let why: Vec<String> = [b.err(), a.err()].into_iter().flatten().collect();
            return Decision {
                bias: Bias::Hold,
                reason: format!("no signal: {}", why.join("; ")),
            };
        }
    };

    let deviation_bps = (a.mid - b.mid) / b.mid * 1e4;
    let exchange_skew_ms = aster.exchange_time_ms as i64 - binance.exchange_time_ms as i64;
    let bias = if deviation_bps <= -p.threshold_bps {
        Bias::Long
    } else if deviation_bps >= p.threshold_bps {
        Bias::Short
    } else {
        Bias::Hold
    };
    Decision {
        bias,
        reason: format!(
            "aster-binance mid deviation={deviation_bps:+.2}bps (threshold ±{:.2}) exch_skew={exchange_skew_ms:+}ms | binance {b} | aster {a}",
            p.threshold_bps
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::Level;
    use crate::fixed::Fixed;

    const P: SignalParams = SignalParams {
        stale_after: Duration::from_secs(3),
        threshold_bps: 2.0,
    };

    fn view(venue: &'static str, bid: &str, ask: &str, published_at: Instant) -> BookView {
        let lv = |p: &str| Level::new(Fixed::parse(p).unwrap(), Fixed::parse("1").unwrap());
        BookView {
            bids: vec![lv(bid)],
            asks: vec![lv(ask)],
            last_update_id: Some(1),
            published_at,
            ..BookView::empty(venue, FeedStatus::Live)
        }
    }

    #[test]
    fn aster_below_binance_gives_long_bias() {
        let now = Instant::now();
        let b = view("binance", "2000.00", "2000.02", now);
        let a = view("aster", "1998.90", "1999.10", now); // -5bps
        assert_eq!(evaluate(&b, &a, now, &P).bias, Bias::Long);
    }

    #[test]
    fn aster_above_binance_gives_short_bias() {
        let now = Instant::now();
        let b = view("binance", "2000.00", "2000.02", now);
        let a = view("aster", "2001.00", "2001.20", now);
        assert_eq!(evaluate(&b, &a, now, &P).bias, Bias::Short);
    }

    #[test]
    fn small_deviation_holds() {
        let now = Instant::now();
        let b = view("binance", "2000.00", "2000.02", now);
        let a = view("aster", "2000.05", "2000.07", now);
        assert_eq!(evaluate(&b, &a, now, &P).bias, Bias::Hold);
    }

    #[test]
    fn stale_venue_forces_hold() {
        let now = Instant::now();
        let b = view("binance", "2000.00", "2000.02", now);
        let a = view("aster", "1990.00", "1990.10", now - Duration::from_secs(10));
        let d = evaluate(&b, &a, now, &P);
        assert_eq!(d.bias, Bias::Hold);
        assert!(d.reason.contains("aster stale"), "{}", d.reason);
    }

    #[test]
    fn non_live_venue_forces_hold() {
        let now = Instant::now();
        let b = BookView::empty("binance", FeedStatus::Syncing);
        let a = view("aster", "1990.00", "1990.10", now);
        let d = evaluate(&b, &a, now, &P);
        assert_eq!(d.bias, Bias::Hold);
        assert!(d.reason.contains("binance is syncing"), "{}", d.reason);
    }
}
