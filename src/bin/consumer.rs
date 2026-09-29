use kafka_rust_pipeline::{format_for_storage, process_event, SensorEvent, TOPIC};
use rskafka::client::{
    partition::{OffsetAt, UnknownTopicHandling},
    ClientBuilder,
};
use std::{env, fs::OpenOptions, io::Write};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_owned());
    let topic = env::var("KAFKA_TOPIC").unwrap_or_else(|_| TOPIC.to_owned());
    let out_path = env::var("OUTPUT_PATH").unwrap_or_else(|_| "output/events.jsonl".to_owned());
    let max_events: Option<usize> = env::var("MAX_EVENTS").ok().and_then(|v| v.parse().ok());

    tracing::info!(brokers = %brokers, topic = %topic, "connecting to Kafka");
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
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
            }
        }
        match client_opt {
            Some(c) => c,
            None => return Err(anyhow::anyhow!("cannot connect to Kafka: {:?}", last_err)),
        }
    };

    if let Ok(controller) = client.controller_client() {
        let _ = controller.create_topic(&topic, 1, 1, 5_000).await;
    }

    let partition_client = client
        .partition_client(topic.clone(), 0, UnknownTopicHandling::Retry)
        .await?;

    // Start from earliest so a fresh run replays all events (shows ordering).
    let mut offset = partition_client
        .get_offset(OffsetAt::Earliest)
        .await
        .unwrap_or(0);
    tracing::info!(offset, "starting consume");

    if let Some(parent) = std::path::Path::new(&out_path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut consumed = 0usize;

    loop {
        if let Some(max) = max_events {
            if consumed >= max {
                break;
            }
        }

        let (records, _watermark) = partition_client
            .fetch_records(offset, 1..1_000_000, 1_000)
            .await?;

        if records.is_empty() {
            // No new data — poll again (message delivery: wait, don't spin hard).
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            continue;
        }

        for r in records {
            let kafka_offset = r.offset;
            match r.record.value.as_deref() {
                Some(bytes) => match SensorEvent::from_json(bytes) {
                    Ok(ev) => match process_event(ev) {
                        Ok(p) => {
                            let line = format_for_storage(&p);
                            tracing::info!(offset = kafka_offset, %line, "consumed");
                            let mut f = OpenOptions::new()
                                .create(true)
                                .append(true)
                                .open(&out_path)?;
                            writeln!(f, "{}", line)?;
                        }
                        Err(e) => {
                            tracing::warn!(offset = kafka_offset, err = %e, "validation failed")
                        }
                    },
                    Err(e) => tracing::warn!(offset = kafka_offset, err = %e, "bad payload"),
                },
                None => tracing::warn!(offset = kafka_offset, "empty value"),
            }
            offset = kafka_offset + 1;
            consumed += 1;
        }
    }

    tracing::info!(consumed, "consumer done");
    Ok(())
}
