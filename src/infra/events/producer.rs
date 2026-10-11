//! Event Producer for ConFuse Platform


// EventProducer requires the kafka feature (rdkafka → librdkafka → OpenSSL).
// Gate the entire implementation so services that don't need Kafka can compile
// without a system OpenSSL / CMake installation.

mod kafka_impl {
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use rdkafka::ClientConfig;
    use serde::Serialize;
    use anyhow::Result;

    /// Kafka event producer
    pub struct EventProducer {
        producer: FutureProducer,
    }

    impl EventProducer {
        pub fn new(bootstrap_servers: &str) -> Result<Self> {
            let enable_idempotence = std::env::var("KAFKA_ENABLE_IDEMPOTENCE")
                .expect("Missing required environment variable: KAFKA_ENABLE_IDEMPOTENCE")
                .to_lowercase() == "true";

            let mut config = ClientConfig::new();
            config
                .set("bootstrap.servers", bootstrap_servers)
                .set("message.max.bytes", "10485760") // 10MB
                .set("delivery.timeout.ms", "60000") // 1 minute
                .set("request.timeout.ms", "20000")
                .set("batch.size", "131072") // 128KB batches
                .set("linger.ms", "20")
                .set("queue.buffering.max.messages", "10000")
                .set("queue.buffering.max.kbytes", "65536") // 64MB buffer limit
                .set("enable.idempotence", enable_idempotence.to_string());

            if let Ok(protocol) = std::env::var("KAFKA_SECURITY_PROTOCOL") {
                config.set("security.protocol", protocol);
            }
            if let Ok(mechanism) = std::env::var("KAFKA_SASL_MECHANISM") {
                config.set("sasl.mechanism", mechanism);
            }
            if let Ok(username) = std::env::var("KAFKA_SASL_USERNAME").or_else(|_| std::env::var("CONFLUENT_API_KEY")) {
                config.set("sasl.username", username);
            }
            if let Ok(password) = std::env::var("KAFKA_SASL_PASSWORD").or_else(|_| std::env::var("CONFLUENT_API_SECRET")) {
                config.set("sasl.password", password);
            }
            if let Ok(ca_location) = std::env::var("KAFKA_SSL_CA_LOCATION") {
                config.set("ssl.ca.location", ca_location);
            }
            if let Ok(ca_pem) = std::env::var("KAFKA_SSL_CA_PEM") {
                config.set("ssl.ca.pem", ca_pem.replace("\\n", "\n"));
            }

            let producer: FutureProducer = config.create()?;

            // Verify connection by fetching metadata
            use rdkafka::producer::Producer;
            producer.client().fetch_metadata(None, std::time::Duration::from_secs(10))?;
            tracing::info!("Successfully connected and verified Aiven Kafka at {}", bootstrap_servers);

            Ok(Self { producer })
        }
        pub async fn publish<T: Serialize>(&self, topic: &str, event: &T) -> Result<()> {
            let payload = serde_json::to_string(event)?;
            let record = FutureRecord::to(topic)
                .payload(&payload)
                .key("event");

            match self.producer.send(record, std::time::Duration::from_secs(0)).await {
                Ok(_delivery) => {
                    tracing::info!("Event sent successfully");
                    Ok(())
                }
                Err((e, _)) => {
                    tracing::error!("Failed to send event: {}", e);
                    Err(anyhow::anyhow!("Failed to send event: {}", e))
                }
            }
        }

        /// Publish with retries and optional DLQ fallback.
        pub async fn publish_with_retry<T: Serialize + std::fmt::Debug>(
            &self,
            topic: &str,
            event: &T,
            retries: usize,
            dlq_topic: Option<&str>,
        ) -> Result<()> {
            use tokio::time::{sleep, Duration};

            let mut last_err: Option<anyhow::Error> = None;

            for attempt in 0..retries {
                match self.publish(topic, event).await {
                    Ok(_) => return Ok(()),
                    Err(e) => {
                        tracing::warn!("Publish attempt {} failed for topic {}: {}", attempt + 1, topic, e);
                        last_err = Some(e);
                        let delay = Duration::from_millis((2u64.pow(attempt as u32)) * 500);
                        sleep(delay).await;
                    }
                }
            }

            tracing::error!("Failed to publish after {} attempts", retries);

            if let Some(dlq) = dlq_topic {
                // Build failure envelope
                let envelope = serde_json::json!({
                    "failedTopic": topic,
                    "failedAt": chrono::Utc::now().timestamp_millis(),
                    "error": format!("{:?}", last_err),
                    "event": format!("{:?}", event),
                });
                if let Err(e) = self.publish(dlq, &envelope).await {
                    tracing::error!("Failed to publish failure envelope to DLQ {}: {}", dlq, e);
                } else {
                    tracing::info!("Published failure envelope to DLQ {}", dlq);
                }
            }

            Err(last_err.unwrap_or_else(|| anyhow::anyhow!("publish failed without error")))
        }
    }
}


pub use kafka_impl::EventProducer;
// Kafka producer for unified-processor
//
// Publishes chunk messages to the "chunks.raw" topic with retry logic.
// Only compiled when the "kafka" feature is enabled.

use crate::graph::{
    EventHeaders, EventMetadata, SimplifiedChunk, SimplifiedChunkRawEvent,
};
use crate::infra::events::topics::Topics;
use anyhow::Result;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info};


/// Event publisher for unified-processor
pub struct ChunkEventPublisher {
    producer: Option<EventProducer>,
}

impl ChunkEventPublisher {
    /// Create a new event publisher
    pub fn new() -> Self {
        Self {
            producer: Self::create_producer(),
        }
    }

    /// Access the internal producer
    pub fn get_producer(&self) -> Option<&EventProducer> {
        self.producer.as_ref()
    }

    /// Check if Kafka is available
    pub fn is_kafka_available() -> bool {
        match std::env::var("KAFKA_BOOTSTRAP_SERVERS") {
            Ok(bootstrap_servers) => {
                match EventProducer::new(&bootstrap_servers) {
                    Ok(_) => {
                        info!("Kafka health check passed");
                        true
                    }
                    Err(e) => {
                        error!("Kafka health check failed: {}", e);
                        false
                    }
                }
            }
            Err(_) => {
                error!("KAFKA_BOOTSTRAP_SERVERS not configured");
                false
            }
        }
    }

    /// Create Kafka producer if configured
    fn create_producer() -> Option<EventProducer> {
        match std::env::var("KAFKA_BOOTSTRAP_SERVERS") {
            Ok(bootstrap_servers) => {
                let mut attempts = 0;
                loop {
                    match EventProducer::new(&bootstrap_servers) {
                        Ok(producer) => {
                            info!("Kafka event producer initialized for unified-processor");
                            return Some(producer);
                        }
                        Err(e) => {
                            attempts += 1;
                            if attempts >= 30 {
                                error!("Failed to initialize Kafka producer after 30 attempts: {}. Event publishing disabled.", e);
                                return None;
                            }
                            tracing::warn!("Kafka producer failed to connect, retrying in 5s... ({}/30)", attempts);
                            std::thread::sleep(std::time::Duration::from_secs(5));
                        }
                    }
                }
            }
            Err(_) => {
                info!("KAFKA_BOOTSTRAP_SERVERS not configured. Event publishing disabled.");
                None
            }
        }
    }

    /// Publish chunk raw event with retry logic (retry every 5 seconds)
    ///
    /// # Arguments
    /// * `source_id` - Source identifier (document or repository ID)
    /// * `chunks` - Vector of simplified chunks to publish
    /// * `correlation_id` - Optional correlation ID for tracking
    /// * `user_id` - User context identifier
    ///
    /// # Returns
    /// * `Ok(())` if published successfully or if producer is not available
    /// * `Err` if all retries fail
    pub async fn publish_chunks(
        &self,
        source_id: &str,
        repo_name: Option<String>,
        chunks: Vec<SimplifiedChunk>,
        correlation_id: Option<String>,
        user_id: &str,
    ) -> Result<()> {
        if let Some(ref producer) = self.producer {
            let event = SimplifiedChunkRawEvent {
                headers: EventHeaders::new("unified-processor", "CHUNK_RAW")
                    .with_correlation_id(correlation_id.unwrap_or_else(|| source_id.to_string())),
                metadata: EventMetadata {
                    user_id: Some(user_id.to_string()),
                    ..Default::default()
                },
                source_id: source_id.to_string(),
                repo_name,
                chunks,
                timestamp: chrono::Utc::now().to_rfc3339(),
            };

            // Bounded retry logic: try up to 3 times before returning error
            let max_publish_attempts = 3;
            let mut last_err = None;
            for attempt in 1..=max_publish_attempts {
                let dlq = std::env::var("KAFKA_DLQ_TOPIC").ok();
                match producer.publish_with_retry(&Topics::chunks_raw(), &event, 2, dlq.as_deref()).await {
                    Ok(()) => {
                        info!("Published chunk raw event for source: {}", source_id);
                        return Ok(());
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to publish chunk raw event for source {} (attempt {}/{}): {}",
                            source_id,
                            attempt,
                            max_publish_attempts,
                            e
                        );
                        last_err = Some(e);
                        if attempt < max_publish_attempts {
                            sleep(Duration::from_millis(500 * attempt as u64)).await;
                        }
                    }
                }
            }

            if let Some(e) = last_err {
                error!(
                    "Exhausted Kafka publish retries for source {}: {}. Proceeding without Kafka embeddings.",
                    source_id, e
                );
                return Err(e);
            }
            Ok(())
        } else {
            info!("Event publisher not available, skipping chunk publication");
            Ok(())
        }
    }


}

impl Default for ChunkEventPublisher {
    fn default() -> Self {
        Self::new()
    }
}
