use chrono::{DateTime, FixedOffset, Local, NaiveDate, TimeZone};
use regex::Regex;
use serde_json::Value;
use std::sync::LazyLock;
use std::time::Duration;

const MAX_RESET_DELAY: Duration = Duration::from_secs(6 * 60 * 60);

/// Collected only from provider error channels, never assistant or tool output.
#[derive(Debug, Default)]
pub(crate) struct CapacitySignal {
    pub retry_after: Option<Option<Duration>>,
}

impl CapacitySignal {
    pub fn observe(&mut self, error: &str) {
        self.record(classify(error, Local::now().fixed_offset()));
    }

    pub fn observe_error(&mut self, error: &Value) {
        self.record(classify_error(error, Local::now().fixed_offset()));
    }

    fn record(&mut self, signal: Option<Option<Duration>>) {
        if let Some(retry_after) = signal {
            match &mut self.retry_after {
                Some(existing) if existing.is_none() => *existing = retry_after,
                None => self.retry_after = Some(retry_after),
                _ => {}
            }
        }
    }
}

fn classify(error: &str, now: DateTime<FixedOffset>) -> Option<Option<Duration>> {
    if let Ok(value) = serde_json::from_str::<Value>(error) {
        return classify_error(&value, now);
    }
    let text = error.to_ascii_lowercase().replace(['_', '-'], " ");
    static LIMIT: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"usage ?limit|rate ?limit|too many requests|resource ?exhausted|insufficient quota|quota ?(?:exceeded|exhausted)|exceeded your current quota|you['’]ve hit your limit").unwrap()
    });
    static HTTP_STATUS: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"(?i)\b(?:http(?:/\d(?:\.\d)?)?(?:\s+status(?:\s+code)?)?|status(?:\s+code)?)\s*[:=]?\s*429\b").unwrap()
    });
    if !LIMIT.is_match(&text) && !HTTP_STATUS.is_match(error) {
        return None;
    }
    let hint = text_retry_hint(error, now);
    Some(hint.map(|delay| delay.min(MAX_RESET_DELAY)))
}

/// Inspect error messages and explicit provider status fields, excluding usage
/// counters, costs, and other event metadata. Read reset metadata only after
/// the error has been identified as a capacity failure.
fn classify_error(error: &Value, now: DateTime<FixedOffset>) -> Option<Option<Duration>> {
    let signal = match error {
        Value::String(text) => classify(text, now),
        Value::Object(fields) => ["message", "errorMessage", "reason", "type", "status"]
            .iter()
            .filter_map(|key| fields.get(*key).and_then(Value::as_str))
            .find_map(|text| classify(text, now))
            .or_else(|| {
                ["status", "code"]
                    .iter()
                    .filter_map(|key| fields.get(*key))
                    .any(|value| value.as_i64() == Some(429) || value.as_str() == Some("429"))
                    .then_some(None)
            })
            .or_else(|| {
                ["error", "data", "cause", "details"]
                    .iter()
                    .filter_map(|key| fields.get(*key))
                    .find_map(|value| classify_error(value, now))
            }),
        Value::Array(values) => values.iter().find_map(|value| classify_error(value, now)),
        _ => None,
    }?;
    Some(
        json_retry_hint(error, now)
            .or(signal)
            .map(|delay| delay.min(MAX_RESET_DELAY)),
    )
}

fn until(at: DateTime<FixedOffset>, now: DateTime<FixedOffset>) -> Duration {
    (at - now).to_std().unwrap_or(Duration::ZERO)
}

fn json_retry_hint(value: &Value, now: DateTime<FixedOffset>) -> Option<Duration> {
    if let Some(delay) = value.get("retryDelay").and_then(Value::as_str) {
        return text_retry_hint(&format!("retry in {delay}"), now);
    }
    for key in ["retry_after", "retry_after_seconds", "resets_in_seconds"] {
        if let Some(seconds) = value.get(key).and_then(Value::as_u64) {
            return Some(Duration::from_secs(seconds));
        }
    }
    if let Some(ms) = value.get("retry_after_ms").and_then(Value::as_u64) {
        return Some(Duration::from_millis(ms));
    }
    for key in ["resets_at", "reset_at", "limit_resets_at_ms"] {
        if let Some(reset) = value.get(key) {
            let at = if let Some(epoch) = reset.as_i64() {
                if key.ends_with("_ms") {
                    DateTime::from_timestamp_millis(epoch)
                } else {
                    DateTime::from_timestamp(epoch, 0)
                }
                .map(|at| at.fixed_offset())
            } else {
                reset
                    .as_str()
                    .and_then(|at| DateTime::parse_from_rfc3339(at).ok())
            };
            if let Some(at) = at {
                return Some(until(at, now));
            }
        }
    }
    match value {
        Value::Object(fields) => fields
            .values()
            .find_map(|value| json_retry_hint(value, now)),
        Value::Array(values) => values.iter().find_map(|value| json_retry_hint(value, now)),
        Value::String(text) => serde_json::from_str::<Value>(text)
            .ok()
            .and_then(|value| json_retry_hint(&value, now))
            .or_else(|| text_retry_hint(text, now)),
        _ => None,
    }
}

fn text_retry_hint(error: &str, now: DateTime<FixedOffset>) -> Option<Duration> {
    if let Some((_, suffix)) = error.rsplit_once('|')
        && let Ok(epoch) = suffix.trim().parse::<i64>()
        && let Some(at) = DateTime::from_timestamp(epoch, 0)
    {
        return Some(until(at.fixed_offset(), now));
    }
    static RETRY_AFTER: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"(?i)retry[- ]after\s*:\s*(\d+)\b").unwrap());
    if let Some(hint) = RETRY_AFTER.captures(error) {
        return hint[1].parse().ok().map(Duration::from_secs);
    }
    static ISO: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[Zz]|[+-]\d{2}:\d{2})")
            .unwrap()
    });
    if let Some(at) = ISO
        .find(error)
        .and_then(|at| DateTime::parse_from_rfc3339(at.as_str()).ok())
    {
        return Some(until(at, now));
    }
    static RELATIVE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"(?i)(?:(?:try again|retry|resets?|reset)\s+(?:in|after)|retry[- ]after\s*:)\s*((?:\d+(?:\.\d+)?\s*(?:days?|hours?|minutes?|seconds?|ms|[dhms])\b\s*)+)").unwrap()
    });
    static UNIT: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"(?i)(\d+(?:\.\d+)?)\s*(days?|hours?|minutes?|seconds?|ms|[dhms])\b").unwrap()
    });
    if let Some(hint) = RELATIVE.captures(error) {
        let seconds = UNIT.captures_iter(&hint[1]).try_fold(0.0, |total, part| {
            let scale = match part[2].to_ascii_lowercase().as_str() {
                "ms" => 0.001,
                unit if unit.starts_with('d') => 86400.0,
                unit if unit.starts_with('h') => 3600.0,
                unit if unit.starts_with('m') => 60.0,
                _ => 1.0,
            };
            part[1]
                .parse::<f64>()
                .ok()
                .map(|value| total + value * scale)
        })?;
        return Duration::try_from_secs_f64(seconds.min(MAX_RESET_DELAY.as_secs_f64())).ok();
    }
    static CLOCK: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"(?i)(?:try again at|resets?(?: at)?)\s+(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<meridiem>am|pm)?\b(?:\s+on\s+(?P<month>[a-z]+)\s+(?P<day>\d{1,2})(?:st|nd|rd|th)?,?\s+(?P<year>\d{4}))?").unwrap()
    });
    static DATE_FIRST_CLOCK: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"(?i)(?:try again at|resets?(?: at)?)\s+(?P<month>[a-z]+)\s+(?P<day>\d{1,2})(?:st|nd|rd|th)?,?\s+(?P<year>\d{4})\s+(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<meridiem>am|pm)?\b").unwrap()
    });
    let clock = CLOCK
        .captures(error)
        .or_else(|| DATE_FIRST_CLOCK.captures(error))?;
    let mut hour = clock["hour"].parse::<u32>().ok()?;
    let minute = clock
        .name("minute")
        .map_or(Some(0), |m| m.as_str().parse::<u32>().ok())?;
    if let Some(meridiem) = clock.name("meridiem") {
        if !(1..=12).contains(&hour) {
            return None;
        }
        hour = hour % 12
            + if meridiem.as_str().eq_ignore_ascii_case("pm") {
                12
            } else {
                0
            };
    }
    let timezone = if error.to_ascii_lowercase().contains("utc") {
        FixedOffset::east_opt(0)?
    } else {
        *now.offset()
    };
    let local_now = now.with_timezone(&timezone);
    let date = if let Some(month) = clock.name("month") {
        let date = format!("{} {} {}", month.as_str(), &clock["day"], &clock["year"]);
        NaiveDate::parse_from_str(&date, "%B %d %Y")
            .or_else(|_| NaiveDate::parse_from_str(&date, "%b %d %Y"))
            .ok()?
    } else {
        local_now.date_naive()
    };
    let mut at = timezone
        .from_local_datetime(&date.and_hms_opt(hour, minute, 0)?)
        .single()?;
    if clock.name("month").is_none() && at <= local_now {
        at += chrono::Duration::days(1);
    }
    Some(until(at, now))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_429_requires_http_status_context() {
        let now = Local::now().fixed_offset();
        for error in [
            "request failed after 429 tokens",
            "request cost was 0.429 dollars",
            "429",
            r#"{"error":{"message":"model rejected","input_tokens":429}}"#,
            r#"{"error":{"message":"model rejected","total_cost_usd":0.429}}"#,
            r#"{"error":{"code":4290,"message":"model rejected"}}"#,
            r#"{"error":{"status":429.5,"message":"model rejected"}}"#,
            r#"{"error":{"message":"model rejected","diagnostic":"usage limit"}}"#,
        ] {
            assert_eq!(classify(error, now), None, "{error}");
        }
        for error in [
            "status 429",
            "HTTP 429",
            "HTTP/1.1 429",
            "429 Too Many Requests",
            r#"{"error":{"status":429,"message":"request rejected"}}"#,
            r#"{"error":{"code":429,"message":"request rejected"}}"#,
        ] {
            assert_eq!(classify(error, now), Some(None), "{error}");
        }
    }

    #[test]
    fn capacity_codex_reset_on_another_day_accepts_both_orders() {
        let now = DateTime::parse_from_rfc3339("2026-09-30T23:00:00-04:00").unwrap();
        for error in [
            "You've hit your usage limit. Try again at Oct 1st, 2026 1:05 AM.",
            "You've hit your usage limit. Try again at 1:05 AM on October 1st, 2026.",
        ] {
            assert_eq!(
                classify(error, now),
                Some(Some(Duration::from_secs(7500))),
                "{error}"
            );
        }
    }

    #[test]
    fn capacity_reset_times() {
        let now = DateTime::parse_from_rfc3339("2026-09-30T13:00:00-04:00").unwrap();
        for (error, seconds) in [
            (
                "You've hit your usage limit. Try again at 2:05 PM on September 30th, 2026.",
                3900,
            ),
            (
                "You've hit your usage limit. Try again at 14:05 on Sep 30, 2026.",
                3900,
            ),
            ("You've hit your limit · resets 2:05pm", 3900),
            (
                "HTTP 429 Too Many Requests; retry after 1 minute 30 seconds",
                90,
            ),
            ("HTTP 429; Retry-After: 90", 90),
            ("RESOURCE_EXHAUSTED: Please retry in 32.5s.", 32),
            ("provider usage limit reached; resets in 2h 5m", 7500),
            (
                r#"{"error":{"type":"rate_limit_error","retry_after_ms":90000}}"#,
                90,
            ),
            (
                r#"{"error":{"code":429,"details":[{"retryDelay":"90s"}]}}"#,
                90,
            ),
            (
                r#"{"error":{"type":"usage_limit_reached","resets_at":"2026-09-30T18:05:00Z"}}"#,
                3900,
            ),
            ("rate limit; resets at 18:05 UTC", 3900),
            ("usage limit; resets at 2026-09-30T18:05:00Z", 3900),
            ("usage limit; resets in 7d", 21600),
            ("usage limit; resets at 2026-09-29T18:05:00Z", 0),
        ] {
            assert_eq!(
                classify(error, now).flatten().unwrap().as_secs(),
                seconds,
                "{error}"
            );
        }
        let epoch = now.timestamp() + 3900;
        assert_eq!(
            classify(&format!("Claude AI usage limit reached|{epoch}"), now),
            Some(Some(Duration::from_secs(3900)))
        );
    }

    #[test]
    fn capacity_errors_without_reset_use_backoff() {
        let now = Local::now().fixed_offset();
        for error in [
            "You've hit your usage limit",
            "rate_limit_exceeded",
            "usageLimitExceeded",
            "insufficient_quota",
            "HTTP 429",
            "RESOURCE_EXHAUSTED",
            "usage limit; resets at nonsense",
        ] {
            assert_eq!(classify(error, now), Some(None), "{error}");
        }
        for error in ["model rejected", "invalid API key", "compilation failed"] {
            assert_eq!(classify(error, now), None, "{error}");
        }
    }
}
