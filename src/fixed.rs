use std::fmt;

use serde::de::{self, Deserialize, Deserializer, Visitor};

/// Decimal number stored as an integer scaled by `10^SCALE_DIGITS`.
///
/// Prices and quantities arrive as decimal strings; parsing them straight into an
/// integer keeps book keys exact and totally ordered (no float equality issues).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Fixed(i64);

pub const SCALE_DIGITS: u32 = 8;
const SCALE: i64 = 10i64.pow(SCALE_DIGITS);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseFixedError {
    #[error("empty number")]
    Empty,
    #[error("invalid character in number")]
    InvalidChar,
    #[error("more than {SCALE_DIGITS} significant fractional digits")]
    TooPrecise,
    #[error("number out of range")]
    Overflow,
}

impl Fixed {
    #[cfg(test)]
    pub const fn raw(self) -> i64 {
        self.0
    }

    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    pub fn to_f64(self) -> f64 {
        self.0 as f64 / SCALE as f64
    }

    /// Parses non-negative decimal strings like `"2352.05"`, `"0.003"`, `"17"`.
    pub fn parse(s: &str) -> Result<Self, ParseFixedError> {
        let bytes = s.as_bytes();
        if bytes.is_empty() {
            return Err(ParseFixedError::Empty);
        }
        let (int_part, frac_part) = match s.find('.') {
            Some(dot) => (&bytes[..dot], &bytes[dot + 1..]),
            None => (bytes, &[][..]),
        };
        if int_part.is_empty() && frac_part.is_empty() {
            return Err(ParseFixedError::Empty);
        }

        let mut value: i64 = 0;
        for &b in int_part {
            let d = digit(b)?;
            value = value
                .checked_mul(10)
                .and_then(|v| v.checked_add(d))
                .ok_or(ParseFixedError::Overflow)?;
        }
        value = value.checked_mul(SCALE).ok_or(ParseFixedError::Overflow)?;

        let mut place = SCALE;
        for &b in frac_part {
            let d = digit(b)?;
            place /= 10;
            if place == 0 {
                // Binance pads with zeros ("2353.12000000"); only real precision loss is an error.
                if d != 0 {
                    return Err(ParseFixedError::TooPrecise);
                }
                continue;
            }
            value = value
                .checked_add(d * place)
                .ok_or(ParseFixedError::Overflow)?;
        }
        Ok(Fixed(value))
    }
}

fn digit(b: u8) -> Result<i64, ParseFixedError> {
    if b.is_ascii_digit() {
        Ok((b - b'0') as i64)
    } else {
        Err(ParseFixedError::InvalidChar)
    }
}

impl fmt::Display for Fixed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let int = self.0 / SCALE;
        let frac = (self.0 % SCALE).abs();
        if frac == 0 {
            return write!(f, "{int}");
        }
        let frac = format!("{frac:0width$}", width = SCALE_DIGITS as usize);
        write!(f, "{int}.{}", frac.trim_end_matches('0'))
    }
}

impl fmt::Debug for Fixed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl<'de> Deserialize<'de> for Fixed {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct FixedVisitor;

        impl Visitor<'_> for FixedVisitor {
            type Value = Fixed;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a decimal number encoded as a string")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Fixed, E> {
                Fixed::parse(v).map_err(E::custom)
            }
        }

        deserializer.deserialize_str(FixedVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> Fixed {
        Fixed::parse(s).unwrap()
    }

    #[test]
    fn parses_typical_values() {
        assert_eq!(p("2352.05").raw(), 235_205_000_000);
        assert_eq!(p("0.003").raw(), 300_000);
        assert_eq!(p("17").raw(), 1_700_000_000);
        assert_eq!(p("0").raw(), 0);
        assert_eq!(p("0.00000000").raw(), 0);
        assert_eq!(p(".5").raw(), 50_000_000);
        assert_eq!(p("5.").raw(), 500_000_000);
    }

    #[test]
    fn accepts_trailing_zero_padding() {
        assert_eq!(p("2353.1200000000000"), p("2353.12"));
    }

    #[test]
    fn rejects_garbage() {
        assert_eq!(Fixed::parse(""), Err(ParseFixedError::Empty));
        assert_eq!(Fixed::parse("."), Err(ParseFixedError::Empty));
        assert_eq!(Fixed::parse("-1"), Err(ParseFixedError::InvalidChar));
        assert_eq!(Fixed::parse("1e5"), Err(ParseFixedError::InvalidChar));
        assert_eq!(Fixed::parse("1.2.3"), Err(ParseFixedError::InvalidChar));
        assert_eq!(
            Fixed::parse("0.000000001"),
            Err(ParseFixedError::TooPrecise)
        );
        assert_eq!(
            Fixed::parse("99999999999999999999"),
            Err(ParseFixedError::Overflow)
        );
    }

    #[test]
    fn orders_numerically() {
        assert!(p("2352.1") > p("2352.05"));
        assert!(p("10") > p("9.99999999"));
    }

    #[test]
    fn displays_compactly() {
        assert_eq!(p("2352.050").to_string(), "2352.05");
        assert_eq!(p("17").to_string(), "17");
        assert_eq!(p("0.003").to_string(), "0.003");
    }

    #[test]
    fn deserializes_from_json_string() {
        let v: [Fixed; 2] = serde_json::from_str(r#"["2352.05","2.760"]"#).unwrap();
        assert_eq!(v, [p("2352.05"), p("2.76")]);
    }
}
