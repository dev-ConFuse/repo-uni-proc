//! Event schema definitions for the event-driven pipeline refactor
//!
//! This module defines all event types used in the ConFuse platform's event-driven architecture.
//! Events are lightweight messages (<10KB) that flow through Kafka, containing only metadata
//! and references, never full content.
//!
//! # Event Types
//! - `RepoIngestRequested`: Triggers repository ingestion
//! - `RepoUpdated`: Signals repository update (incremental processing)
//! - `CodeChunkCreated`: Signals a new chunk has been created
//! - `EmbeddingCreated`: Signals a new embedding has been generated
//! - `RepoIngestFailed`: Signals ingestion failure
//! - `RepoIngestCompleted`: Signals successful ingestion completion
//!
//! # Requirements
//! - All events must include: event_type, event_id, timestamp, correlation_id, payload
//! - Events must be lightweight (<10KB)
//! - No file contents or base64-encoded data in events
//! - credential_ref should be JWT tokens (short-lived)

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Base fields present in all events
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventBase {
    /// Type of the event (e.g., "REPO_INGEST_REQUESTED")
    pub event_type: String,
    
    /// Unique identifier for this event (UUID v4)
    pub event_id: Uuid,
    
    /// Timestamp when the event was created (ISO 8601 UTC)
    pub timestamp: DateTime<Utc>,
    
    /// Correlation ID for tracing requests across services (UUID v4)
    pub correlation_id: Uuid,
}

impl EventBase {
    /// Create a new EventBase with the given event type and correlation ID
    pub fn new(event_type: impl Into<String>, correlation_id: Uuid) -> Self {
        Self {
            event_type: event_type.into(),
            event_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            correlation_id,
        }
    }
}

/// Provider type for repository sources
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Provider {
    Github,
    Gitlab,
    Bitbucket,
}

impl Provider {
    /// Validate provider string
    pub fn validate(&self) -> Result<(), String> {
        // All enum variants are valid
        Ok(())
    }
}

/// Update type for repository updates
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UpdateType {
    Push,
    ForcePush,
    BranchUpdate,
}

/// Error code for failed ingestion
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    CloneFailed,
    AuthFailed,
    InvalidRepo,
    ProcessingFailed,
    Timeout,
}

/// REPO_INGEST_REQUESTED event payload
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RepoIngestRequestedPayload {
    /// Repository ID (UUID v4)
    pub repo_id: Uuid,
    
    /// Repository URL (valid Git URL)
    pub url: String,
    
    /// Branch name (non-empty string)
    pub branch: String,
    
    /// Provider (github, gitlab, bitbucket)
    pub provider: Provider,
    
    /// Commit ID (40-character hex string, SHA-1)
    pub commit_id: String,
    
    /// Credential reference (JWT token, expires in 5 minutes)
    pub credential_ref: String,
    
    /// User ID
    pub user_id: Uuid,
    
    /// Organization ID (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub organization_id: Option<Uuid>,
}

impl RepoIngestRequestedPayload {
    /// Validate the payload fields
    pub fn validate(&self) -> Result<(), String> {
        if self.url.is_empty() {
            return Err("url cannot be empty".to_string());
        }
        if self.branch.is_empty() {
            return Err("branch cannot be empty".to_string());
        }
        if self.commit_id.len() != 40 {
            return Err("commit_id must be 40 characters (SHA-1)".to_string());
        }
        if !self.commit_id.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err("commit_id must be hexadecimal".to_string());
        }
        if self.credential_ref.is_empty() {
            return Err("credential_ref cannot be empty".to_string());
        }
        self.provider.validate()?;
        Ok(())
    }
}

/// REPO_INGEST_REQUESTED event
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RepoIngestRequested {
    #[serde(flatten)]
    pub base: EventBase,
    pub payload: RepoIngestRequestedPayload,
}

impl RepoIngestRequested {
    /// Create a new REPO_INGEST_REQUESTED event
    pub fn new(correlation_id: Uuid, payload: RepoIngestRequestedPayload) -> Self {
        Self {
            base: EventBase::new("REPO_INGEST_REQUESTED", correlation_id),
            payload,
        }
    }

    /// Validate the event
    pub fn validate(&self) -> Result<(), String> {
        self.payload.validate()
    }
}

/// REPO_UPDATED event payload
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RepoUpdatedPayload {
    /// Repository ID (UUID v4)
    pub repo_id: Uuid,
    
    /// Repository URL (valid Git URL)
    pub url: String,
    
    /// Branch name (non-empty string)
    pub branch: String,
    
    /// Provider (github, gitlab, bitbucket)
    pub provider: Provider,
    
    /// Previous commit ID (40-character hex string)
    pub old_commit: String,
    
    /// Current commit ID (40-character hex string)
    pub new_commit: String,
    
    /// Credential reference (JWT token)
    pub credential_ref: String,
    
    /// Update type (push, force_push, branch_update)
    pub update_type: UpdateType,
}

impl RepoUpdatedPayload {
    /// Validate the payload fields
    pub fn validate(&self) -> Result<(), String> {
        if self.url.is_empty() {
            return Err("url cannot be empty".to_string());
        }
        if self.branch.is_empty() {
            return Err("branch cannot be empty".to_string());
        }
        if self.old_commit.len() != 40 {
            return Err("old_commit must be 40 characters (SHA-1)".to_string());
        }
        if !self.old_commit.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err("old_commit must be hexadecimal".to_string());
        }
        if self.new_commit.len() != 40 {
            return Err("new_commit must be 40 characters (SHA-1)".to_string());
        }
        if !self.new_commit.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err("new_commit must be hexadecimal".to_string());
        }
        if self.old_commit == self.new_commit {
            return Err("old_commit and new_commit must be distinct".to_string());
        }
        if self.credential_ref.is_empty() {
            return Err("credential_ref cannot be empty".to_string());
        }
        self.provider.validate()?;
        Ok(())
    }
}

/// REPO_UPDATED event
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RepoUpdated {
    #[serde(flatten)]
    pub base: EventBase,
    pub payload: RepoUpdatedPayload,
}

impl RepoUpdated {
    /// Create a new REPO_UPDATED event
    pub fn new(correlation_id: Uuid, payload: RepoUpdatedPayload) -> Self {
        Self {
            base: EventBase::new("REPO_UPDATED", correlation_id),
            payload,
        }
    }

    /// Validate the event
    pub fn validate(&self) -> Result<(), String> {
        self.payload.validate()
    }
}

/// Metadata for code chunks
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChunkMetadata {
    /// File size in bytes
    pub file_size: u64,
    
    /// Index of this chunk within the file (0-based)
    pub chunk_index: usize,
    
    /// Total number of chunks for this file
    pub total_chunks: usize,
    
    /// Symbols found in this chunk (function names, class names, etc.)
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub symbols: Vec<String>,
}

/// CODE_CHUNK_CREATED event payload
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CodeChunkCreatedPayload {
    /// Chunk ID (UUID v4, deterministic: hash of repo_id + file_path + start_line + commit_id)
    pub chunk_id: Uuid,
    
    /// Repository ID
    pub repo_id: Uuid,
    
    /// File path relative to repository root
    pub file_path: String,
    
    /// Detected language or "unknown"
    pub language: String,
    
    /// Chunk content (UTF-8 string, max 50KB)
    pub content: String,
    
    /// Starting line number (1-based)
    pub start_line: u32,
    
    /// Ending line number (1-based)
    pub end_line: u32,
    
    /// Commit ID (40-character hex string)
    pub commit_id: String,
    
    /// Additional metadata
    pub metadata: ChunkMetadata,
}

impl CodeChunkCreatedPayload {
    /// Validate the payload fields
    pub fn validate(&self) -> Result<(), String> {
        if self.file_path.is_empty() {
            return Err("file_path cannot be empty".to_string());
        }
        if self.language.is_empty() {
            return Err("language cannot be empty".to_string());
        }
        if self.content.is_empty() {
            return Err("content cannot be empty".to_string());
        }
        if self.content.len() > 50 * 1024 {
            return Err("content exceeds 50KB limit".to_string());
        }
        if self.start_line == 0 {
            return Err("start_line must be positive (1-based)".to_string());
        }
        if self.end_line == 0 {
            return Err("end_line must be positive (1-based)".to_string());
        }
        if self.start_line > self.end_line {
            return Err("start_line cannot be greater than end_line".to_string());
        }
        if self.commit_id.len() != 40 {
            return Err("commit_id must be 40 characters (SHA-1)".to_string());
        }
        if !self.commit_id.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err("commit_id must be hexadecimal".to_string());
        }
        if self.metadata.chunk_index >= self.metadata.total_chunks {
            return Err("chunk_index must be less than total_chunks".to_string());
        }
        Ok(())
    }
}

/// CODE_CHUNK_CREATED event
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CodeChunkCreated {
    #[serde(flatten)]
    pub base: EventBase,
    pub payload: CodeChunkCreatedPayload,
}

impl CodeChunkCreated {
    /// Create a new CODE_CHUNK_CREATED event
    pub fn new(correlation_id: Uuid, payload: CodeChunkCreatedPayload) -> Self {
        Self {
            base: EventBase::new("CODE_CHUNK_CREATED", correlation_id),
            payload,
        }
    }

    /// Validate the event
    pub fn validate(&self) -> Result<(), String> {
        self.payload.validate()
    }
}

/// EMBEDDING_CREATED event payload
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EmbeddingCreatedPayload {
    /// Chunk ID that this embedding corresponds to
    pub chunk_id: Uuid,
    
    /// Embedding vector (array of floats)
    pub embedding: Vec<f32>,
    
    /// Model name (e.g., "nvidia/nemotron-3-embed-1b")
    pub model: String,
    
    /// Model version
    pub model_version: String,
    
    /// Embedding dimension (must match embedding array length)
    pub dimension: usize,
}

impl EmbeddingCreatedPayload {
    /// Validate the payload fields
    pub fn validate(&self) -> Result<(), String> {
        if self.embedding.is_empty() {
            return Err("embedding cannot be empty".to_string());
        }
        if self.embedding.len() != self.dimension {
            return Err(format!(
                "embedding length ({}) does not match dimension ({})",
                self.embedding.len(),
                self.dimension
            ));
        }
        if self.model.is_empty() {
            return Err("model cannot be empty".to_string());
        }
        if self.model_version.is_empty() {
            return Err("model_version cannot be empty".to_string());
        }
        if self.dimension == 0 {
            return Err("dimension must be positive".to_string());
        }
        Ok(())
    }
}

/// EMBEDDING_CREATED event
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EmbeddingCreated {
    #[serde(flatten)]
    pub base: EventBase,
    pub payload: EmbeddingCreatedPayload,
}

impl EmbeddingCreated {
    /// Create a new EMBEDDING_CREATED event
    pub fn new(correlation_id: Uuid, payload: EmbeddingCreatedPayload) -> Self {
        Self {
            base: EventBase::new("EMBEDDING_CREATED", correlation_id),
            payload,
        }
    }

    /// Validate the event
    pub fn validate(&self) -> Result<(), String> {
        self.payload.validate()
    }
}

/// REPO_INGEST_FAILED event payload
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RepoIngestFailedPayload {
    /// Repository ID
    pub repo_id: Uuid,
    
    /// Error code (CLONE_FAILED, AUTH_FAILED, INVALID_REPO, PROCESSING_FAILED, TIMEOUT)
    pub error_code: ErrorCode,
    
    /// Human-readable error message
    pub error_message: String,
    
    /// Number of retry attempts
    pub retry_count: u32,
    
    /// Whether this is a fatal error (no more retries)
    pub fatal: bool,
}

impl RepoIngestFailedPayload {
    /// Validate the payload fields
    pub fn validate(&self) -> Result<(), String> {
        if self.error_message.is_empty() {
            return Err("error_message cannot be empty".to_string());
        }
        Ok(())
    }
}

/// REPO_INGEST_FAILED event
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RepoIngestFailed {
    #[serde(flatten)]
    pub base: EventBase,
    pub payload: RepoIngestFailedPayload,
}

impl RepoIngestFailed {
    /// Create a new REPO_INGEST_FAILED event
    pub fn new(correlation_id: Uuid, payload: RepoIngestFailedPayload) -> Self {
        Self {
            base: EventBase::new("REPO_INGEST_FAILED", correlation_id),
            payload,
        }
    }

    /// Validate the event
    pub fn validate(&self) -> Result<(), String> {
        self.payload.validate()
    }
}

/// Statistics for completed ingestion
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngestionStats {
    /// Number of files processed
    pub files_processed: u32,
    
    /// Number of chunks created
    pub chunks_created: u32,
    
    /// Processing duration in milliseconds
    pub processing_duration_ms: u64,
    
    /// Repository size in bytes
    pub repository_size_bytes: u64,
}

/// REPO_INGEST_COMPLETED event payload
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RepoIngestCompletedPayload {
    /// Repository ID
    pub repo_id: Uuid,
    
    /// Commit ID that was processed
    pub commit_id: String,
    
    /// Processing statistics
    pub stats: IngestionStats,
}

impl RepoIngestCompletedPayload {
    /// Validate the payload fields
    pub fn validate(&self) -> Result<(), String> {
        if self.commit_id.len() != 40 {
            return Err("commit_id must be 40 characters (SHA-1)".to_string());
        }
        if !self.commit_id.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err("commit_id must be hexadecimal".to_string());
        }
        Ok(())
    }
}

/// REPO_INGEST_COMPLETED event
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RepoIngestCompleted {
    #[serde(flatten)]
    pub base: EventBase,
    pub payload: RepoIngestCompletedPayload,
}

impl RepoIngestCompleted {
    /// Create a new REPO_INGEST_COMPLETED event
    pub fn new(correlation_id: Uuid, payload: RepoIngestCompletedPayload) -> Self {
        Self {
            base: EventBase::new("REPO_INGEST_COMPLETED", correlation_id),
            payload,
        }
    }

    /// Validate the event
    pub fn validate(&self) -> Result<(), String> {
        self.payload.validate()
    }
}

/// Helper function to check if an event's serialized size is under the 10KB limit
pub fn check_event_size<T: Serialize>(event: &T) -> Result<usize, String> {
    let json = serde_json::to_string(event)
        .map_err(|e| format!("Failed to serialize event: {}", e))?;
    let size = json.len();
    
    if size >= 10 * 1024 {
        return Err(format!("Event size ({} bytes) exceeds 10KB limit", size));
    }
    
    Ok(size)
}



// =============================================================================
// Semantic OS Event Extensions
// =============================================================================
// These events extend the original pipeline to support all source types.

/// SLACK_MESSAGE_RECEIVED — emitted by data-connector when new Slack messages
/// arrive (via webhook or polling). Lightweight: contains channel ref + metadata
/// only, not message content.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SlackMessageReceivedPayload {
    /// Slack workspace ID
    pub workspace_id: String,
    /// Slack channel ID (e.g., C01234ABC)
    pub channel_id: String,
    /// Human-readable channel name (e.g., #engineering)
    pub channel_name: String,
    /// Slack message timestamp (used as message ID)
    pub message_ts: String,
    /// Thread root timestamp (None if not a reply)
    pub thread_ts: Option<String>,
    /// Slack user ID who sent the message
    pub user_id: String,
    /// Message text (kept short — up to 2000 chars)
    pub text_preview: String,
    /// Whether this is a direct message (vs. channel post)
    pub is_dm: bool,
    /// Number of reactions on the message
    pub reaction_count: u32,
    /// Source reference ID (used to fetch full content from data-connector)
    pub source_id: String,
    /// Org / tenant identifier
    pub org_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SlackMessageReceived {
    #[serde(flatten)]
    pub base: EventBase,
    pub payload: SlackMessageReceivedPayload,
}

impl SlackMessageReceived {
    pub fn new(correlation_id: Uuid, payload: SlackMessageReceivedPayload) -> Self {
        Self {
            base: EventBase::new("SLACK_MESSAGE_RECEIVED", correlation_id),
            payload,
        }
    }
}

/// MEETING_TRANSCRIPT_RECEIVED — emitted when a meeting transcript is available
/// from Zoom, Teams, Otter.ai, or an uploaded file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MeetingTranscriptReceivedPayload {
    /// Unique meeting ID from the source platform
    pub meeting_id: String,
    /// Meeting platform: zoom, teams, otter, generic
    pub platform: String,
    /// Meeting title / subject
    pub title: String,
    /// Meeting start time (ISO 8601)
    pub started_at: DateTime<Utc>,
    /// Duration in seconds
    pub duration_secs: Option<u64>,
    /// Participant display names
    pub participants: Vec<String>,
    /// Reference to transcript content in object storage (MinIO path or source URL)
    pub content_ref: String,
    /// Source connector ID that produced this event
    pub source_id: String,
    /// Org / tenant identifier
    pub org_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MeetingTranscriptReceived {
    #[serde(flatten)]
    pub base: EventBase,
    pub payload: MeetingTranscriptReceivedPayload,
}

impl MeetingTranscriptReceived {
    pub fn new(correlation_id: Uuid, payload: MeetingTranscriptReceivedPayload) -> Self {
        Self {
            base: EventBase::new("MEETING_TRANSCRIPT_RECEIVED", correlation_id),
            payload,
        }
    }
}

/// API_SCHEMA_UPDATED — emitted when an API schema changes (OpenAPI, GraphQL, JSON Schema).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ApiSchemaUpdatedPayload {
    /// Unique API source ID
    pub api_id: String,
    /// API name / title
    pub api_name: String,
    /// Schema format: openapi, graphql, json_schema
    pub schema_format: String,
    /// Schema version (e.g., "3.0.0")
    pub schema_version: Option<String>,
    /// Reference to the schema file in object storage
    pub content_ref: String,
    /// Base URL of the API
    pub base_url: Option<String>,
    /// Org / tenant identifier
    pub org_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ApiSchemaUpdated {
    #[serde(flatten)]
    pub base: EventBase,
    pub payload: ApiSchemaUpdatedPayload,
}

impl ApiSchemaUpdated {
    pub fn new(correlation_id: Uuid, payload: ApiSchemaUpdatedPayload) -> Self {
        Self {
            base: EventBase::new("API_SCHEMA_UPDATED", correlation_id),
            payload,
        }
    }
}

/// Reason a chunk was deprecated.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeprecationReason {
    /// The source file was deleted from the repository
    FileDeleted,
    /// The file was renamed — old chunks deprecated, new ones created
    FileRenamed,
    /// Content changed significantly (old chunks replaced by new ones)
    ContentReplaced,
    /// Manual override / admin action
    ManualDeprecation,
    /// Source connection was deleted
    SourceRemoved,
}

/// CHUNK_DEPRECATED — emitted when chunks become stale due to file deletion or replacement.
/// The chunks are NOT deleted — they are retained for temporal queries.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChunkDeprecatedPayload {
    /// IDs of chunks being deprecated
    pub chunk_ids: Vec<String>,
    /// Source / repository ID
    pub source_id: String,
    /// File path that was deleted/changed
    pub file_path: String,
    /// Why the chunk is being deprecated
    pub reason: DeprecationReason,
    /// For FileRenamed: new file path
    pub new_file_path: Option<String>,
    /// For ContentReplaced: new chunk IDs that replace these
    pub replacement_chunk_ids: Vec<String>,
    /// When the deprecation took effect
    pub deprecated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChunkDeprecated {
    #[serde(flatten)]
    pub base: EventBase,
    pub payload: ChunkDeprecatedPayload,
}

impl ChunkDeprecated {
    pub fn new(correlation_id: Uuid, payload: ChunkDeprecatedPayload) -> Self {
        Self {
            base: EventBase::new("CHUNK_DEPRECATED", correlation_id),
            payload,
        }
    }
}

/// ENTITY_MERGED — emitted by the entity resolver when two entity records are
/// found to refer to the same real-world entity and should be canonicalized.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EntityMergedPayload {
    /// Source entity ID (will be deprecated / aliased)
    pub source_entity_id: String,
    /// Target / canonical entity ID (survives)
    pub target_entity_id: String,
    /// Why these entities were merged
    pub merge_reason: String,
    /// Confidence score (0.0–1.0)
    pub confidence: f32,
    /// Service that detected the merge
    pub detected_by: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EntityMerged {
    #[serde(flatten)]
    pub base: EventBase,
    pub payload: EntityMergedPayload,
}

impl EntityMerged {
    pub fn new(correlation_id: Uuid, payload: EntityMergedPayload) -> Self {
        Self {
            base: EventBase::new("ENTITY_MERGED", correlation_id),
            payload,
        }
    }
}

/// RELATIONSHIP_INFERRED — emitted when the graph layer infers a new relationship
/// between entities that was not explicitly stated (e.g., co-occurrence inference).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RelationshipInferredPayload {
    /// Source entity ID
    pub from_entity_id: String,
    /// Target entity ID
    pub to_entity_id: String,
    /// Relationship type (e.g., "COLLABORATES_WITH", "DEPENDS_ON")
    pub relationship_type: String,
    /// Supporting evidence (chunk IDs that support this inference)
    pub evidence_chunk_ids: Vec<Uuid>,
    /// Confidence score (0.0–1.0)
    pub confidence: f32,
    /// Inference method: "co_occurrence", "llm_extraction", "graph_pattern"
    pub inference_method: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RelationshipInferred {
    #[serde(flatten)]
    pub base: EventBase,
    pub payload: RelationshipInferredPayload,
}

impl RelationshipInferred {
    pub fn new(correlation_id: Uuid, payload: RelationshipInferredPayload) -> Self {
        Self {
            base: EventBase::new("RELATIONSHIP_INFERRED", correlation_id),
            payload,
        }
    }
}

/// DOCUMENT_UPDATED — emitted when a document's content changes in a connector
/// (Notion, Confluence, Google Drive, etc.).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DocumentUpdatedPayload {
    /// Document ID in the source platform
    pub document_id: String,
    /// Human-readable document title
    pub title: String,
    /// Source connector ID (notion, confluence, googledrive, etc.)
    pub source_id: String,
    /// Reference to full content in object storage
    pub content_ref: String,
    /// Author of the update
    pub updated_by: Option<String>,
    /// When the document was last modified in the source
    pub source_updated_at: DateTime<Utc>,
    /// SHA-256 hash of new content for deduplication
    pub content_hash: String,
    /// Org / tenant identifier
    pub org_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DocumentUpdated {
    #[serde(flatten)]
    pub base: EventBase,
    pub payload: DocumentUpdatedPayload,
}

impl DocumentUpdated {
    pub fn new(correlation_id: Uuid, payload: DocumentUpdatedPayload) -> Self {
        Self {
            base: EventBase::new("DOCUMENT_UPDATED", correlation_id),
            payload,
        }
    }
}
