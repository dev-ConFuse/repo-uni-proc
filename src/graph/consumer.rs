//! Event Consumer for ConFuse Platform

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{StreamConsumer, Consumer};
use rdkafka::message::{Message, BorrowedMessage};
use serde::de::DeserializeOwned;
use futures::StreamExt;
use anyhow::Result;
use std::sync::Arc;
use tracing::{info, error, warn, Instrument};

/// Kafka event consumer for background message processing
pub struct EventConsumer {
    consumer: StreamConsumer,
    group_id: String,
}

impl EventConsumer {
    /// Create a new event consumer with specified consumer group
    pub fn new(bootstrap_servers: &str, group_id: &str) -> Result<Self> {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", bootstrap_servers)
            .set("group.id", group_id)
            .set("client.id", format!("{}-consumer", group_id))
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.offset.store", "true")
            // Session / heartbeat — keep well inside max.poll.interval
            .set("session.timeout.ms", "30000")
            .set("heartbeat.interval.ms", "10000")
            // 30-minute poll interval: process_file() on large repos can take several minutes;
            // the old 300 s was too tight and caused PollExceeded errors.
            .set("max.poll.interval.ms", "1800000")
            .set("auto.commit.interval.ms", "5000")
            .set("allow.auto.create.topics", "true")
            // Keep the TCP connection alive so Aiven doesn't drop idle connections
            // (fixes BrokerTransportFailure after long idle periods)
            .set("socket.keepalive.enable", "true")
            .set("socket.timeout.ms", "60000")
            // Lower the broker-side wait time for fetch responses to improve responsiveness
            // when topics are idle (at the cost of more frequent fetch requests).
            .set("fetch.wait.max.ms", "500");

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

        let consumer: StreamConsumer = config.create()?;

        info!("Kafka event consumer initialized with group: {}", group_id);

        Ok(Self {
            consumer,
            group_id: group_id.to_string(),
        })
    }

    /// Subscribe to topics and start consuming messages
    pub async fn subscribe(&self, topics: &[&str]) -> Result<()> {
        self.consumer.subscribe(topics)?;
        info!("Subscribed to topics: {:?}", topics);
        Ok(())
    }

    /// Start consuming messages with async handler
    pub async fn consume<F, T>(&self, handler: Arc<F>) -> Result<()>
    where
        F: Fn(T) -> futures::future::BoxFuture<'static, Result<()>> + Send + Sync + 'static,
        T: DeserializeOwned + Send + 'static,
    {
        info!("Starting message consumption for group: {}", self.group_id);

        let mut message_stream = self.consumer.stream();
        
        while let Some(message_result) = message_stream.next().await {
            match message_result {
                Ok(message) => {
                    let payload = match message.payload() {
                        Some(p) => p,
                        None => {
                            warn!("Received message with empty payload");
                            continue;
                        }
                    };

                    // Deserialize message
                    match serde_json::from_slice::<T>(payload) {
                        Ok(event) => {
                            let handler = handler.clone();
                            tokio::spawn(async move {
                                if let Err(e) = handler(event).await {
                                    error!("Error processing message: {}", e);
                                }
                            });
                        }
                        Err(e) => {
                            error!("Failed to deserialize message: {}", e);
                            // Continue processing other messages
                        }
                    }
                }
                Err(e) => {
                    error!("Kafka consumer error: {}", e);
                    // Continue processing
                }
            }
        }

        Ok(())
    }

    /// Get consumer group ID
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Commit offsets manually (if auto-commit is disabled)
    pub async fn commit_message(&self, message: &BorrowedMessage<'_>) -> Result<()> {
        self.consumer.commit_message(message, rdkafka::consumer::CommitMode::Async)?;
        Ok(())
    }
}

/// Trait for event handlers
pub trait EventHandler<T>: Send + Sync {
    fn handle(&self, event: T) -> futures::future::BoxFuture<'static, Result<()>>;
}

/// Convenience function to create a boxed future handler
pub fn boxed_handler<F, Fut, T>(f: F) -> impl Fn(T) -> futures::future::BoxFuture<'static, Result<()>>
where
    F: Fn(T) -> Fut + Send + Sync + 'static,
    Fut: futures::Future<Output = Result<()>> + Send + 'static,
    T: Send + 'static,
{
    move |event| Box::pin(f(event))
}
// Kafka event consumer for unified-processor
//
// Listens for:
// - SimplifiedEmbeddingGeneratedEvent: triggered by embeddings-service with embeddings
// - StreamedFileEvent: streamed repository files on repo.events topic

use tracing::debug;
use crate::graph::SimplifiedEmbeddingGeneratedEvent;
use crate::infra::events::topics::Topics;
use crate::infra::events::types::{RepoUpdated, RepoIngestRequested};
use crate::core::orchestrator::UnifiedProcessor;

#[derive(Debug, serde::Deserialize)]
pub struct StreamedFileEvent {
    pub repo_id: String,
    #[serde(default)]
    pub repo_name: String,
    pub file_path: String,
    pub content: String,
    pub url: String,
    pub user_id: Option<String>,
    #[serde(default)]
    pub is_deleted: bool,
}

pub struct UnifiedEventConsumer {
    processor: Arc<UnifiedProcessor>,
}

impl UnifiedEventConsumer {
    pub fn new(processor: Arc<UnifiedProcessor>) -> Self {
        Self { processor }
    }

    /// Start the background event consumer loop with retry on Kafka disconnection
    pub async fn start(&self) -> anyhow::Result<()> {
        let bootstrap_servers = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
            .expect("Missing required environment variable: KAFKA_BOOTSTRAP_SERVERS");

        let processor = self.processor.clone();

        loop {
            match self.start_consumer(&bootstrap_servers, processor.clone()).await {
                Ok(_) => {
                    info!("Kafka consumer stopped gracefully");
                    break;
                }
                Err(e) => {
                    error!("Kafka consumer error: {}. Retrying in 5 seconds...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }

        Ok(())
    }

    async fn start_consumer(&self, bootstrap_servers: &str, processor: Arc<UnifiedProcessor>) -> anyhow::Result<()> {
        let group_id = std::env::var("REPO_UNI_PROC_KAFKA_GROUP_ID")
            .expect("Missing required environment variable: REPO_UNI_PROC_KAFKA_GROUP_ID");
        let consumer = EventConsumer::new(bootstrap_servers, &group_id)
            .map_err(|e| anyhow::anyhow!("Failed to create Kafka consumer: {}", e))?;



        let embedding_generated_topic = Topics::embedding_generated();
        let repo_events_topic = Topics::repo_events();
        consumer.subscribe(&[
            embedding_generated_topic.as_str(),
            repo_events_topic.as_str(),
        ]).await?;

        info!("Unified event consumer started on topics: {}, {}",
            embedding_generated_topic, repo_events_topic);

        // Use the consume method from EventConsumer which handles the loop and spawning
        // We use boxed_handler helper to ensure the closure returns a BoxFuture as required

        consumer.consume::<_, serde_json::Value>(Arc::new(boxed_handler(move |event_json: serde_json::Value| {
            let processor = processor.clone();
            
            // Extract correlation ID generically from json
            let correlation_id = event_json.get("headers")
                .and_then(|h| h.get("correlation_id"))
                .and_then(|c| c.as_str())
                .map(|s| s.to_string())
                .or_else(|| {
                    event_json.get("headers")
                        .and_then(|h| h.get("event_id"))
                        .and_then(|e| e.as_str())
                        .map(|s| s.to_string())
                })
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                
            let span = tracing::info_span!("process_event", correlation_id = %correlation_id);
            
            async move {
                if let Err(e) = Self::handle_event(processor, event_json).await {
                    error!("Error handling event: {}", e);
                    // Return Ok to keep the consumer running even if one event fails
                    Ok(())
                } else {
                    Ok(())
                }
            }.instrument(span)
        }))).await?;

        Ok(())
    }

    async fn handle_event(processor: Arc<UnifiedProcessor>, event_json: serde_json::Value) -> anyhow::Result<()> {
        // Removed noisy Processing raw event log


        // 1.5. Try RepoUpdated (from webhooks)
        if let Ok(_repo_updated) = serde_json::from_value::<RepoUpdated>(event_json.clone()) {
            // Silently ignore RepoUpdated event (handled by data-connector HTTP streaming)
            return Ok(());
        }

        // 1.6 Try RepoIngestRequested (handled by data-connector now)
        if let Ok(_repo_req) = serde_json::from_value::<RepoIngestRequested>(event_json.clone()) {
            // Silently ignore RepoIngestRequested event (handled by data-connector streaming API)
            return Ok(());
        }

        // 1.8. Try StreamedFileEvent (file streaming from data-connector)
        if let Ok(file_event) = serde_json::from_value::<StreamedFileEvent>(event_json.clone()) {
            info!("Handling StreamedFileEvent via Kafka: repo_id={}, file_path={}, is_deleted={}", file_event.repo_id, file_event.file_path, file_event.is_deleted);
            let user_id = file_event.user_id.unwrap_or_else(|| "system".to_string());
            
            if file_event.is_deleted {
                if let Err(e) = processor.delete_file(&file_event.file_path, &file_event.repo_id, &user_id).await {
                    error!("Failed to delete file {} via Kafka: {}", file_event.file_path, e);
                    return Err(e.into());
                }
                info!("Successfully deleted file {} via Kafka", file_event.file_path);
            } else {
                if let Err(e) = processor.process_file(&file_event.content, false, &file_event.file_path, &file_event.repo_id, &file_event.repo_name, &user_id).await {
                    error!("Failed to process streamed file {} via Kafka: {}", file_event.file_path, e);
                    return Err(e.into());
                }
                info!("Successfully processed streamed file {} via Kafka", file_event.file_path);
            }
            return Ok(());
        }

        // 2. Try SimplifiedEmbeddingGeneratedEvent
        if let Ok(emb_event) = serde_json::from_value::<SimplifiedEmbeddingGeneratedEvent>(event_json.clone()) {
            if emb_event.repo_name.is_none() {
                // Silently skip document embedding event in repo-uni-proc
                return Ok(());
            }

            info!("Handling SimplifiedEmbeddingGeneratedEvent: source_id={}, chunks={}", emb_event.source_id, emb_event.chunks.len());

            let user_id = emb_event.metadata.user_id.as_deref().unwrap_or("system");
            let user_graph = processor.falkordb_storage.with_user_graph(user_id);

            let chunks_len = emb_event.chunks.len();
            for (i, chunk) in emb_event.chunks.iter().enumerate() {
                let content = match processor.get_chunk_content(&chunk.id, user_id).await {
                    Ok(c) => c,
                    Err(e) => {
                        error!(
                            "Chunk content not found for chunk_id={} source_id={}: {}",
                            chunk.id, emb_event.source_id, e
                        );
                        // Skip this chunk, as it was likely deleted or we cannot process it
                        continue;
                    }
                };


                // Diagnostic log: show what we are about to store in FalkorDB
                info!(
                    "[FalkorDB] Storing chunk → source_id={} | chunk_id={} | type={} | embedding_dim={} | graph={}",
                    emb_event.source_id,
                    chunk.id,
                    chunk.chunk_type,
                    chunk.embedding.len(),
                    user_graph.graph_name
                );

                let repo_name_str = emb_event.repo_name.as_deref().unwrap_or(&emb_event.source_id);
                let _repo_file_path = format!("{}/{}", repo_name_str, chunk.file_id);
                let _language_str = chunk.language.as_deref().unwrap_or("unknown");

                if let Err(e) = user_graph.update_chunk_embedding(
                    &chunk.id,
                    &chunk.embedding,
                ).await {
                    error!("Failed to store chunk {} in FalkorDB: {}", chunk.id, e);
                } else {
                    // Evict from cache once successfully stored to free memory
                    {
                        let mut cache = processor.chunk_content_cache.lock().unwrap_or_else(|p| p.into_inner());
                        cache.remove(&chunk.id);
                    }
                    info!("[FalkorDB] Successfully stored chunk_id={} for source_id={}", chunk.id, emb_event.source_id);


                    // --- NEXT_CHUNK LINKING ---
                    // Connect this chunk to the next chunk in the sequence
                    if i < chunks_len - 1 {
                        let next_chunk = &emb_event.chunks[i + 1];
                        let next_chunk_query = format!(
                            r#"MATCH (a:Vector_Chunk {{id: "{}"}}) MERGE (b:Vector_Chunk {{id: "{}"}}) MERGE (a)-[r:NEXT_CHUNK {{confidence: 1.0}}]->(b)"#,
                            chunk.id, next_chunk.id
                        );
                        if let Err(e) = user_graph.execute_query(&next_chunk_query).await {
                            debug!("[NEXT_CHUNK] Link query failed for chunks {} -> {}: {}", chunk.id, next_chunk.id, e);
                        }
                    }

                    // --- CROSS-FILE LINKING (all languages) ---
                    // Extract file references from chunk content and create LINKS_TO edges.
                    // Works bi-directionally to handle race conditions (file A before file B).
                    let chunk_type_lower = chunk.chunk_type.to_lowercase();
                    let parts: Vec<&str> = chunk.id.split('|').collect();
                    let chunk_filepath = if parts.len() >= 2 { parts[1] } else { &chunk.id };
                    let mut chunk_filename = chunk_filepath.rsplit('/').next().unwrap_or(chunk_filepath).to_string();
                    if let Some(idx) = chunk_filename.rfind('.') {
                        chunk_filename.truncate(idx);
                    }
                    // Determine which regex patterns to use based on chunk language
                    let ref_patterns: Vec<(&str, &str)> = if chunk_type_lower.contains("html") {
                        vec![
                            (r#"href=["']([^"']+\.css)["']"#, "html_css_link"),
                            (r#"<script[^>]+src=["']([^"']+)["']"#, "html_script_link"),
                            (r#"<img[^>]+src=["']([^"']+)["']"#, "html_img_link"),
                            (r#"<link[^>]+href=["']([^"']+)["']"#, "html_link_tag"),
                        ]
                    } else if chunk_type_lower.contains("css") {
                        vec![
                            (r#"@import\s+url\(["']?([^"'()]+)["']?\)"#, "css_import"),
                            (r#"@import\s+["']([^"']+)["']"#, "css_import"),
                        ]
                    } else if chunk_type_lower.contains("javascript") || chunk_type_lower.contains("typescript") {
                        vec![
                            (r#"import\s+.*?from\s+["']([^"']+)["']"#, "js_import"),
                            (r#"require\s*\(\s*["']([^"']+)["']\s*\)"#, "js_require"),
                            (r#"import\s*\(\s*["']([^"']+)["']\s*\)"#, "js_dynamic_import"),
                        ]
                    } else if chunk_type_lower.contains("python") {
                        vec![
                            (r#"from\s+(\S+)\s+import"#, "python_from_import"),
                            (r#"import\s+(\S+)"#, "python_import"),
                        ]
                    } else if chunk_type_lower.contains("rust") {
                        vec![
                            (r#"mod\s+(\w+)\s*;"#, "rust_mod"),
                            (r#"use\s+(?:crate::)?(\w+)"#, "rust_use"),
                        ]
                    } else if chunk_type_lower.contains("go") {
                        vec![
                            (r#"import\s+"([^"]+)""#, "go_import"),
                        ]
                    } else if chunk_type_lower.contains("java") {
                        vec![
                            (r#"import\s+([a-zA-Z_][\w.]*)"#, "java_import"),
                        ]
                    } else if chunk_type_lower.contains("c_sharp") {
                        vec![
                            (r#"using\s+([a-zA-Z_][\w.]*)\s*;"#, "csharp_using"),
                        ]
                    } else if chunk_type_lower.contains(r#""c""#) || chunk_type_lower.contains("cpp") {
                        vec![
                            (r#"#include\s+"([^"]+)""#, "c_include"),
                        ]
                    } else if chunk_type_lower.contains("ruby") {
                        vec![
                            (r#"require\s+["']([^"']+)["']"#, "ruby_require"),
                            (r#"require_relative\s+["']([^"']+)["']"#, "ruby_require_relative"),
                        ]
                    } else if chunk_type_lower.contains("php") {
                        vec![
                            (r#"(?:require|include)(?:_once)?\s+["']([^"']+)["']"#, "php_require"),
                        ]
                    } else if chunk_type_lower.contains("swift") {
                        vec![
                            (r#"import\s+(\w+)"#, "swift_import"),
                        ]
                    } else if chunk_type_lower.contains("kotlin") {
                        vec![
                            (r#"import\s+([a-zA-Z_][\w.]*)"#, "kotlin_import"),
                        ]
                    } else if chunk_type_lower.contains("scala") {
                        vec![
                            (r#"import\s+([a-zA-Z_][\w.]*)"#, "scala_import"),
                        ]
                    } else if chunk_type_lower.contains("bash") {
                        vec![
                            (r#"source\s+["']?([^"'\s]+)["']?"#, "bash_source"),
                            (r#"\.\s+["']?([^"'\s]+)["']?"#, "bash_dot_source"),
                        ]
                    } else if chunk_type_lower.contains("markdown") || chunk_type_lower.contains("md") {
                        vec![
                            (r#"\[[^\]]*\]\(([^)]+)\)"#, "markdown_link"),
                        ]
                    } else {
                        vec![]
                    };

                    // Forward linking: extract references from this chunk → find target chunks
                    for (pattern, link_type) in &ref_patterns {
                        if let Ok(re) = regex::Regex::new(pattern) {
                            for caps in re.captures_iter(&content) {
                                if let Some(ref_match) = caps.get(1) {
                                    let ref_path = ref_match.as_str()
                                        .trim_start_matches("./")
                                        .trim_start_matches("../");
                                    let mut ref_filename = ref_path.rsplit('/').next().unwrap_or(ref_path);
                                    if ref_filename.is_empty() { continue; }
                                    if let Some(idx) = ref_filename.rfind('.') {
                                        ref_filename = &ref_filename[..idx];
                                    }

                                    let query = format!(
                                        r#"MATCH (a:Vector_Chunk {{id: "{}"}}) MATCH (b:Vector_Chunk) WHERE b.source_id = "{}" AND (b.id CONTAINS "|{}." OR b.id CONTAINS "/{}." OR b.id CONTAINS "|{}|" OR b.id CONTAINS "/{}|") AND a.id <> b.id MERGE (a)-[r:LINKS_TO {{confidence: 1.0, link_type: "{}"}}]->(b)"#,
                                        chunk.id, emb_event.source_id, ref_filename, ref_filename, ref_filename, ref_filename, link_type
                                    );
                                    if let Err(e) = user_graph.execute_query(&query).await {
                                        debug!("[LINKS_TO] Forward link query failed for {} -> {}: {}", chunk.id, ref_filename, e);
                                    }
                                }
                            }
                        }
                    }

                    // Reverse linking: find existing chunks that reference THIS chunk's filename
                    if !chunk_filename.is_empty() {
                        let reverse_query = format!(
                            r#"MATCH (b:Vector_Chunk {{id: "{}"}}) MATCH (a:Vector_Chunk) WHERE a.source_id = "{}" AND a.id <> "{}" AND a.content CONTAINS "{}" MERGE (a)-[r:LINKS_TO {{confidence: 0.9, link_type: "reverse_content_match"}}]->(b)"#,
                            chunk.id, emb_event.source_id, chunk.id, chunk_filename
                        );
                        if let Err(e) = user_graph.execute_query(&reverse_query).await {
                            debug!("[LINKS_TO] Reverse link query failed for {}: {}", chunk_filename, e);
                        }
                    }


                    // --- CALL fast-fetcher for cross-graph edges ---
                    let req_body = serde_json::json!({
                        "chunk_id": chunk.id,
                        "graph_id": emb_event.source_id,
                        "falkordb_graph_name": user_graph.graph_name,
                        "embedding": chunk.embedding,
                        "chunk_text": content,
                        "top_k": 10
                    });
                    
                    // fast-fetcher endpoint is env config (no hard-coded URL)
                    let fetcher_url = std::env::var("FAST_FETCHER_URL")
                        .expect("Missing required environment variable: FAST_FETCHER_URL");
                    let fetcher_url = format!("{}/ingest", fetcher_url.trim_end_matches('/'));
                    let client = reqwest::Client::new();
                    match client.post(&fetcher_url).json(&req_body).send().await {
                        Ok(res) => {
                            if let Ok(json) = res.json::<serde_json::Value>().await {
                                if let Some(edges) = json.get("discovered_edges").and_then(|v| v.as_array()) {
                                    if !edges.is_empty() {
                                        info!("[fast-fetcher] Discovered {} cross-graph edges for chunk {}", edges.len(), chunk.id);
                                        for edge in edges {
                                            if let Some(edge_arr) = edge.as_array() {
                                                if edge_arr.len() == 2 {
                                                    if let (Some(target_chunk_id), Some(_score)) = (edge_arr[0].as_str(), edge_arr[1].as_f64()) {
                                                        // Architecture Rule: fast-fetcher is strictly responsible for generating cross-graph edges.
                                                        // We no longer write the SIMILAR_TO edge here. fast-fetcher will handle it.
                                                        debug!("[fast-fetcher] Delegated SIMILAR_TO edge creation between {} and {} to fast-fetcher", chunk.id, target_chunk_id);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!("[fast-fetcher] Failed to call cross-graph discovery service: {}", e);
                        }
                    }
                }
            }
            return Ok(());
        }

        debug!("Event did not match any handled schemas, skipping");
        Ok(())
    }
}
