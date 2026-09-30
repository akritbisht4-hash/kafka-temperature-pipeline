use kafka_rust_pipeline::{format_for_storage, process_event, SensorEvent};

#[test]
fn test_pipeline_normal_event_flow() {
    let raw = r#"{"sensor_id":"sensor-10","temperature_c":21.5,"humidity_pct":45.0,"timestamp_ms":1710000000000}"#;
    let event = SensorEvent::from_json(raw.as_bytes()).expect("should deserialize and validate");
    let processed = process_event(event).expect("should process event");

    assert!(!processed.alert);
    let stored = format_for_storage(&processed);
    assert!(stored.starts_with("[OK]"));
    assert!(stored.contains("sensor-10"));
}

#[test]
fn test_pipeline_alert_event_flow() {
    let raw = r#"{"sensor_id":"sensor-99","temperature_c":42.8,"humidity_pct":60.0,"timestamp_ms":1710000000000}"#;
    let event = SensorEvent::from_json(raw.as_bytes()).expect("should deserialize and validate");
    let processed = process_event(event).expect("should process event");

    assert!(processed.alert);
    let stored = format_for_storage(&processed);
    assert!(stored.starts_with("[ALERT]"));
    assert!(stored.contains("sensor-99"));
}
