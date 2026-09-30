use chrono::{DateTime, Utc};
use rand::Rng;
use serde::{Deserialize, Serialize};

/// Kafka topic used by the pipeline.
pub const TOPIC: &str = "sensor-events";
/// Temperature above this (°C) is treated as an alert.
pub const ALERT_THRESHOLD_C: f64 = 35.0;

/// Mock sensor event produced to Kafka.
/// This is the message-queue payload: producer -> Kafka topic -> consumer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SensorEvent {
    pub sensor_id: String,
    pub temperature_c: f64,
    pub humidity_pct: f64,
    pub timestamp_ms: i64,
}

impl SensorEvent {
    pub fn new(
        sensor_id: String,
        temperature_c: f64,
        humidity_pct: f64,
        timestamp_ms: i64,
    ) -> Self {
        Self {
            sensor_id,
            temperature_c,
            humidity_pct,
            timestamp_ms,
        }
    }

    /// Generate a random mock event (used by producer).
    pub fn random(sensor_id: &str) -> Self {
        let mut rng = rand::thread_rng();
        Self {
            sensor_id: sensor_id.to_owned(),
            temperature_c: rng.gen_range(15.0..45.0),
            humidity_pct: rng.gen_range(20.0..95.0),
            timestamp_ms: Utc::now().timestamp_millis(),
        }
    }

    /// Validate event ranges. Used by consumer to drop corrupt messages.
    pub fn validate(&self) -> Result<(), String> {
        if self.sensor_id.trim().is_empty() {
            return Err("sensor_id is empty".to_owned());
        }
        if !(-50.0..=80.0).contains(&self.temperature_c) {
            return Err(format!("temperature out of range: {}", self.temperature_c));
        }
        if !(0.0..=100.0).contains(&self.humidity_pct) {
            return Err(format!("humidity out of range: {}", self.humidity_pct));
        }
        if self.timestamp_ms <= 0 {
            return Err("timestamp must be positive".to_owned());
        }
        Ok(())
    }

    /// Event-driven rule: hot sensor => alert.
    pub fn is_alert(&self) -> bool {
        self.temperature_c > ALERT_THRESHOLD_C
    }

    pub fn to_json(&self) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn from_json(bytes: &[u8]) -> anyhow::Result<Self> {
        let ev: Self = serde_json::from_slice(bytes)?;
        ev.validate().map_err(|e| anyhow::anyhow!(e))?;
        Ok(ev)
    }

    pub fn datetime(&self) -> Option<DateTime<Utc>> {
        DateTime::from_timestamp_millis(self.timestamp_ms)
    }
}

/// Result after consumer processing.
#[derive(Debug, Clone, PartialEq)]
pub struct ProcessedEvent {
    pub event: SensorEvent,
    pub alert: bool,
    pub processed_at_ms: i64,
}

/// Core consumer logic: validate + apply alert rule.
/// Pure function — easy to unit test without a live Kafka broker.
pub fn process_event(event: SensorEvent) -> Result<ProcessedEvent, String> {
    event.validate()?;
    let alert = event.is_alert();
    Ok(ProcessedEvent {
        event,
        alert,
        processed_at_ms: Utc::now().timestamp_millis(),
    })
}

/// Format for file/DB sink. Prefix makes ordering + delivery visible in logs.
pub fn format_for_storage(p: &ProcessedEvent) -> String {
    let tag = if p.alert { "ALERT" } else { "OK" };
    let payload = serde_json::to_string(&p.event).unwrap_or_else(|_| "{}".to_owned());
    format!("[{}] {}", tag, payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(temp: f64) -> SensorEvent {
        SensorEvent::new("sensor-1".to_owned(), temp, 55.0, 1_700_000_000_000)
    }

    #[test]
    fn test_serialization_roundtrip() {
        let ev = sample(22.5);
        let bytes = ev.to_json().unwrap();
        let back = SensorEvent::from_json(&bytes).unwrap();
        assert_eq!(ev, back);
    }

    #[test]
    fn test_validation_rejects_bad_temp() {
        let ev = sample(200.0);
        assert!(ev.validate().is_err());
    }

    #[test]
    fn test_validation_rejects_bad_humidity() {
        let ev = SensorEvent::new("s-1".to_owned(), 20.0, 150.0, 1_700_000_000_000);
        assert!(ev.validate().is_err());
    }

    #[test]
    fn test_validation_rejects_empty_sensor_id() {
        let ev = SensorEvent::new("".to_owned(), 20.0, 50.0, 1_700_000_000_000);
        assert!(ev.validate().is_err());
    }

    #[test]
    fn test_alert_detection_high_temp() {
        assert!(sample(38.2).is_alert());
    }

    #[test]
    fn test_no_alert_normal_temp() {
        assert!(!sample(24.0).is_alert());
    }

    #[test]
    fn test_process_event_marks_alert_and_formats() {
        let p = process_event(sample(40.0)).unwrap();
        assert!(p.alert);
        let line = format_for_storage(&p);
        assert!(line.starts_with("[ALERT]"));
        assert!(line.contains("sensor-1"));
    }

    #[test]
    fn test_from_json_rejects_malformed() {
        assert!(SensorEvent::from_json(b"not json").is_err());
        // valid JSON but invalid range -> rejected by validate()
        let bad = r#"{"sensor_id":"s-1","temperature_c":999.0,"humidity_pct":50.0,"timestamp_ms":1700000000000}"#;
        assert!(SensorEvent::from_json(bad.as_bytes()).is_err());
    }

    #[test]
    fn test_validation_rejects_extreme_cold_temp() {
        let ev = sample(-60.0);
        assert!(ev.validate().is_err());
    }

    #[test]
    fn test_validation_rejects_negative_humidity() {
        let ev = SensorEvent::new("s-1".to_owned(), 20.0, -1.0, 1_700_000_000_000);
        assert!(ev.validate().is_err());
    }
}
