use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Price {
    pub value: i64,
    pub scale: i32,
    pub currency: String,
}

impl Price {
    pub fn new(value: i64, scale: i32, currency: impl Into<String>) -> Self {
        Self {
            value,
            scale,
            currency: currency.into(),
        }
    }

    pub fn from_f64(value: f64, currency: impl Into<String>) -> Self {
        Self::from_f64_with_scale(value, 4, currency)
    }

    pub fn from_f64_with_scale(value: f64, scale: i32, currency: impl Into<String>) -> Self {
        let factor = 10_f64.powi(scale.max(0));
        Self::new((value * factor).round() as i64, scale, currency)
    }

    pub fn as_f64(&self) -> f64 {
        let factor = 10_f64.powi(self.scale.max(0));
        self.value as f64 / factor
    }

    pub fn display_value(&self) -> String {
        format_scaled(self.value, self.scale)
    }
}

impl Default for Price {
    fn default() -> Self {
        Self::new(0, 4, "USD")
    }
}

impl fmt::Display for Price {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} {}", self.display_value(), self.currency)
    }
}

impl<'de> Deserialize<'de> for Price {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum PriceRepr {
            Structured {
                value: i64,
                scale: i32,
                #[serde(default = "default_currency")]
                currency: String,
            },
            Number(f64),
        }

        match PriceRepr::deserialize(deserializer)? {
            PriceRepr::Structured {
                value,
                scale,
                currency,
            } => Ok(Price::new(value, scale, currency)),
            PriceRepr::Number(value) => Ok(Price::from_f64(value, default_currency())),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Money {
    pub value: i64,
    pub scale: i32,
    pub currency: String,
}

impl Money {
    pub fn new(value: i64, scale: i32, currency: impl Into<String>) -> Self {
        Self {
            value,
            scale,
            currency: currency.into(),
        }
    }

    pub fn from_f64(value: f64, currency: impl Into<String>) -> Self {
        Self::from_f64_with_scale(value, 2, currency)
    }

    pub fn from_f64_with_scale(value: f64, scale: i32, currency: impl Into<String>) -> Self {
        let factor = 10_f64.powi(scale.max(0));
        Self::new((value * factor).round() as i64, scale, currency)
    }

    pub fn as_f64(&self) -> f64 {
        let factor = 10_f64.powi(self.scale.max(0));
        self.value as f64 / factor
    }

    pub fn display_value(&self) -> String {
        format_scaled(self.value, self.scale)
    }
}

impl Default for Money {
    fn default() -> Self {
        Self::new(0, 2, "USD")
    }
}

impl fmt::Display for Money {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} {}", self.display_value(), self.currency)
    }
}

impl<'de> Deserialize<'de> for Money {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum MoneyRepr {
            Structured {
                value: i64,
                scale: i32,
                #[serde(default = "default_currency")]
                currency: String,
            },
            Number(f64),
        }

        match MoneyRepr::deserialize(deserializer)? {
            MoneyRepr::Structured {
                value,
                scale,
                currency,
            } => Ok(Money::new(value, scale, currency)),
            MoneyRepr::Number(value) => Ok(Money::from_f64(value, default_currency())),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LatencyBreakdown {
    pub signal_to_intent_ms: Option<u64>,
    pub intent_to_risk_ms: Option<u64>,
    pub risk_to_submit_ms: Option<u64>,
    pub submit_to_ack_ms: Option<u64>,
    pub ack_to_first_fill_ms: Option<u64>,
    pub submit_to_terminal_ms: Option<u64>,
}

fn default_currency() -> String {
    "USD".to_string()
}

fn format_scaled(value: i64, scale: i32) -> String {
    if scale <= 0 {
        return value.to_string();
    }

    let negative = value < 0;
    let absolute = value.abs();
    let divisor = 10_i64.pow(scale as u32);
    let whole = absolute / divisor;
    let fractional = absolute % divisor;
    let sign = if negative { "-" } else { "" };
    format!("{sign}{whole}.{fractional:0width$}", width = scale as usize)
}
