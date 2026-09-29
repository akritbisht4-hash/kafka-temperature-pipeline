use chrono::Utc;
use kafka_rust_pipeline::{SensorEvent, TOPIC};
use rskafka::{
    client::{partition::UnknownTopicHandling, ClientBuilder},
    record::Record,
};
use std::{collections::BTreeMap, env, time::Duration};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_owned());
    let topic = env::var("KAFKA_TOPIC").unwrap_or_else(|_| TOPIC.to_owned());
    let count: usize = env::var("EVENT_COUNT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    let interval_ms: u64 = env::var("INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200);

    tracing::info!(brokers = %brokers, topic = %topic, "connecting to Kafka");
    // Retry: Kafka is slow to start on low-RAM machines.
    let client = {
        let mut last_err = None;
        let mut client_opt = None;
        for attempt in 1..=30 {
            match ClientBuilder::new(vec![brokers.clone()]).build().await {
                Ok(c) => {
                    client_opt = Some(c);
                    break;
                }
                Err(e) => {
                    tracing::warn!(attempt, err = %e, "kafka not ready, retrying");
                    last_err = Some(e);
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
        match client_opt {
            Some(c) => c,
            None => return Err(anyhow::anyhow!("cannot connect to Kafka: {:?}", last_err)),
        }
    };

    // Ensure topic exists (idempotent — ignore AlreadyExists).
    if let Ok(controller) = client.controller_client() {
        let _ = controller.create_topic(&topic, 1, 1, 5_000).await;
    }

    let partition_client = client
        .partition_client(topic.clone(), 0, UnknownTopicHandling::Retry)
        .await?;

    for i in 0..count {
        let sensor_id = format!("sensor-{}", (i % 3) + 1);
        let event = SensorEvent::random(&sensor_id);

        let payload = event.to_json()?;
        let record = Record {
            key: Some(sensor_id.as_bytes().to_vec()),
            value: Some(payload),
            headers: BTreeMap::new(),
            timestamp: Utc::now(),
        };

        let offsets = partition_client
            .produce(vec![record], Default::default())
            .await?;
        tracing::info!(
            n = i,
            offset = offsets[0],
            sensor = %sensor_id,
            temp = event.temperature_c,
            "produced event"
        );

        tokio::time::sleep(Duration::from_millis(interval_ms)).await;
    }

    tracing::info!("producer done");
    Ok(())
}
