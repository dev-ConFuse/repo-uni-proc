/// Kafka topic constants for the platform.
/// Kafka topic names for the platform.
///
/// Topic names are configuration (env-only — see `.cline/rules/env-policy.md`);
/// values come from `.map.env`.
pub struct Topics;
impl Topics {
    pub fn embedding_generated() -> String {
        std::env::var("KAFKA_EMBEDDING_GENERATED_TOPIC")
            .expect("Missing required environment variable: KAFKA_EMBEDDING_GENERATED_TOPIC")
    }

    pub fn chunks_raw() -> String {
        std::env::var("KAFKA_CHUNKS_RAW_TOPIC")
            .expect("Missing required environment variable: KAFKA_CHUNKS_RAW_TOPIC")
    }

    pub fn repo_events() -> String {
        std::env::var("KAFKA_REPO_EVENTS_TOPIC")
            .expect("Missing required environment variable: KAFKA_REPO_EVENTS_TOPIC")
    }
}