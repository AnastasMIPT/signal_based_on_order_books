//! Cross-venue mid deviation signal for the Aster ETHUSDT perpetual.
//!
//! Binance is the reference market (deeper, faster). If Aster's mid sits below Binance's mid
//! by more than a threshold, we expect Aster to catch up (Long bias); above it - to come
//! down (Short bias). Profitability is not a goal.
//!
//! Guards, each forcing Hold with the reason:
//! - a venue is not `Live`, its view is stale, or its data lags the exchange clock;
//! - the two books describe moments too far apart on the exchange clocks.
//!
//! Hysteresis: a bias is entered at `enter_bps` and kept while the deviation stays above
//! `exit_bps`, so the decision does not flicker around the threshold.

use std::fmt;
use std::time::{Duration, Instant};

use crate::view::{BookView, FeedStatus};

#[derive(Debug, Clone, Copy)]
pub struct SignalParams {
    /// A venue whose view was not updated for longer than this is unusable.
    pub stale_after: Duration,
    /// A venue whose last update lagged the exchange clock by more than this is unusable.
    pub max_lag_ms: i64,
    /// Maximum |exchange time(aster) - exchange time(binance)| between the two books.
    pub max_skew_ms: i64,
    /// |deviation| needed to enter a bias, basis points.
    pub enter_bps: f64,
    /// |deviation| below which an existing bias is dropped, basis points.
    pub exit_bps: f64,
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
    /// Short justification: the numbers the decision hinges on, or the guard that blocked it.
    pub reason: String,
    /// Every input per venue, for debugging. Empty if a venue was unusable.
    pub details: String,
}

/// Numbers derived from one venue's view.
struct Side {
    bid: f64,
    ask: f64,
    mid: f64,
    age: Duration,
    lag_ms: i64,
    exchange_time_ms: u64,
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "bid={:.2} ask={:.2} lag={}ms age={}ms",
            self.bid,
            self.ask,
            self.lag_ms,
            self.age.as_millis()
        )
    }
}

fn side(view: &BookView, now: Instant, p: &SignalParams) -> Result<Side, String> {
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
    if view.exchange_lag_ms > p.max_lag_ms {
        return Err(format!(
            "{} lags exchange by {}ms",
            view.venue, view.exchange_lag_ms
        ));
    }
    let (bid, ask) = (b.price.to_f64(), a.price.to_f64());
    Ok(Side {
        bid,
        ask,
        mid: (bid + ask) / 2.0,
        age,
        lag_ms: view.exchange_lag_ms,
        exchange_time_ms: view.exchange_time_ms,
    })
}

/// Stateful only for hysteresis; everything else is recomputed from the views each call.
pub struct SignalEngine {
    params: SignalParams,
    bias: Bias,
}

impl SignalEngine {
    pub fn new(params: SignalParams) -> Self {
        Self {
            params,
            bias: Bias::Hold,
        }
    }

    pub fn evaluate(&mut self, binance: &BookView, aster: &BookView, now: Instant) -> Decision {
        let decision = self.decide(binance, aster, now);
        self.bias = decision.bias;
        decision
    }

    fn decide(&self, binance: &BookView, aster: &BookView, now: Instant) -> Decision {
        let p = &self.params;
        let hold = |reason: String, details: String| Decision {
            bias: Bias::Hold,
            reason,
            details,
        };
        let (b, a) = match (side(binance, now, p), side(aster, now, p)) {
            (Ok(b), Ok(a)) => (b, a),
            (b, a) => {
                let why: Vec<String> = [b.err(), a.err()].into_iter().flatten().collect();
                return hold(format!("no signal: {}", why.join("; ")), String::new());
            }
        };
        let skew_ms = a.exchange_time_ms as i64 - b.exchange_time_ms as i64;
        let details = format!("skew={skew_ms:+}ms | binance {b} | aster {a}");
        if skew_ms.abs() > p.max_skew_ms {
            return hold(
                format!(
                    "no signal: books {skew_ms:+}ms apart on exchange clocks (max {})",
                    p.max_skew_ms
                ),
                details,
            );
        }

        // Negative: Aster is cheap relative to Binance.
        let deviation_bps = (a.mid - b.mid) / b.mid * 1e4;
        let direction = if deviation_bps < 0.0 {
            Bias::Long
        } else {
            Bias::Short
        };
        let reason = format!(
            "deviation={deviation_bps:+.2}bps | binance mid={:.3} | aster mid={:.3}",
            b.mid, a.mid
        );
        let threshold = if self.bias == direction {
            p.exit_bps
        } else {
            p.enter_bps
        };
        let bias = if deviation_bps.abs() >= threshold {
            direction
        } else {
            Bias::Hold
        };
        Decision {
            bias,
            reason,
            details,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::Level;
    use crate::fixed::Fixed;

    const P: SignalParams = SignalParams {
        stale_after: Duration::from_secs(3),
        max_lag_ms: 1000,
        max_skew_ms: 500,
        enter_bps: 2.0,
        exit_bps: 0.5,
    };

    fn view(venue: &'static str, bid: &str, ask: &str, now: Instant) -> BookView {
        let lv = |p: &str| Level::new(Fixed::parse(p).unwrap(), Fixed::parse("1").unwrap());
        BookView {
            bids: vec![lv(bid)],
            asks: vec![lv(ask)],
            last_update_id: Some(1),
            exchange_time_ms: 1_000_000,
            exchange_lag_ms: 50,
            published_at: now,
            ..BookView::empty(venue, FeedStatus::Live)
        }
    }

    fn binance(now: Instant) -> BookView {
        view("binance", "2000.00", "2000.02", now)
    }

    /// Aster book whose mid is `bps` basis points away from Binance's 2000.01.
    fn aster_at(bps: f64, now: Instant) -> BookView {
        let mid = 2000.01 * (1.0 + bps / 1e4);
        let bid = format!("{:.2}", mid - 0.05);
        let ask = format!("{:.2}", mid + 0.05);
        view("aster", &bid, &ask, now)
    }

    fn run(engine: &mut SignalEngine, aster_bps: f64) -> Decision {
        let now = Instant::now();
        engine.evaluate(&binance(now), &aster_at(aster_bps, now), now)
    }

    #[test]
    fn aster_cheap_gives_long_rich_gives_short() {
        let mut e = SignalEngine::new(P);
        assert_eq!(run(&mut e, -5.0).bias, Bias::Long);
        let mut e = SignalEngine::new(P);
        assert_eq!(run(&mut e, 5.0).bias, Bias::Short);
    }

    #[test]
    fn small_deviation_holds() {
        let mut e = SignalEngine::new(P);
        assert_eq!(run(&mut e, -1.0).bias, Bias::Hold);
    }

    #[test]
    fn hysteresis_keeps_bias_until_exit_threshold() {
        let mut e = SignalEngine::new(P);
        assert_eq!(run(&mut e, -5.0).bias, Bias::Long);
        assert_eq!(
            run(&mut e, -1.5).bias,
            Bias::Long,
            "below enter, above exit"
        );
        assert_eq!(run(&mut e, -0.3).bias, Bias::Hold, "below exit");
        assert_eq!(
            run(&mut e, -1.5).bias,
            Bias::Hold,
            "re-entry needs enter again"
        );
    }

    #[test]
    fn flip_to_opposite_side_needs_enter_threshold() {
        let mut e = SignalEngine::new(P);
        assert_eq!(run(&mut e, -5.0).bias, Bias::Long);
        assert_eq!(run(&mut e, 1.0).bias, Bias::Hold);
        assert_eq!(run(&mut e, 3.0).bias, Bias::Short);
    }

    #[test]
    fn stale_or_lagging_venue_forces_hold() {
        let now = Instant::now();
        let mut e = SignalEngine::new(P);
        let mut a = aster_at(-5.0, now);
        a.published_at = now - Duration::from_secs(10);
        let d = e.evaluate(&binance(now), &a, now);
        assert_eq!(d.bias, Bias::Hold);
        assert!(d.reason.contains("aster stale"), "{}", d.reason);

        let mut b = binance(now);
        b.exchange_lag_ms = 4000;
        let d = e.evaluate(&b, &aster_at(-5.0, now), now);
        assert_eq!(d.bias, Bias::Hold);
        assert!(d.reason.contains("binance lags exchange"), "{}", d.reason);
    }

    #[test]
    fn misaligned_books_force_hold() {
        let now = Instant::now();
        let mut e = SignalEngine::new(P);
        let mut a = aster_at(-5.0, now);
        a.exchange_time_ms += 2000;
        let d = e.evaluate(&binance(now), &a, now);
        assert_eq!(d.bias, Bias::Hold);
        assert!(
            d.reason.contains("apart on exchange clocks"),
            "{}",
            d.reason
        );
    }

    #[test]
    fn non_live_venue_forces_hold() {
        let now = Instant::now();
        let mut e = SignalEngine::new(P);
        let b = BookView::empty("binance", FeedStatus::Resyncing);
        let d = e.evaluate(&b, &aster_at(-5.0, now), now);
        assert_eq!(d.bias, Bias::Hold);
        assert!(d.reason.contains("binance is resyncing"), "{}", d.reason);
    }
}
