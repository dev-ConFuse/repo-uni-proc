//! Configuration for the unified processor
//!
//! All configuration is env-only (`.map.env` / `.secret.env`, see
//! `.cline/rules/env-policy.md`) — required vars fail fast, optional features
//! read `.ok()` with no literal fallback.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Required string env var (fail fast, naming the variable).
fn req(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|_| format!("Missing required environment variable: {name}"))
}

/// Required env var parsed into `T`.
fn req_parse<T: std::str::FromStr>(name: &str) -> Result<T, String> {
    req(name)?
        .parse::<T>()
        .map_err(|_| format!("Invalid value for environment variable: {name}"))
}

/// Required boolean env var (`true`/`false`/`1`/`0`).
fn req_bool(name: &str) -> Result<bool, String> {
    match req(name)?.to_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        other => Err(format!("Invalid boolean for environment variable {name}: {other}")),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    pub pipeline: PipelineConfig,
    pub falkordb: FalkordbConfig,
    pub kafka: KafkaConfig,
    pub web: WebConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub workers: usize,
    pub auth_middleware_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub database_url: String,
    pub max_connections: u32,
}

impl DatabaseConfig {
    pub fn from_env() -> Result<Self, String> {
        Ok(Self {
            database_url: req("DATABASE_URL")?,
            max_connections: req_parse("DB_MAX_CONNECTIONS")?,
        })
    }
}

/// Temporary storage configuration for repos and documents
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    pub max_file_size: usize,
    pub chunk_size: usize,
    pub max_batch_size: usize,
    pub timeout: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FalkordbConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: Option<String>,
    pub use_tls: bool,
    /// Embedding dimension used when creating the vector index.
    pub embedding_dim: usize,
    /// Connection-level timeout in seconds.
    pub timeout_secs: u64,
}

impl FalkordbConfig {
    pub fn from_env() -> Result<Self, String> {
        Ok(Self {
            host: req("FALKORDB_HOST")?,
            port: req_parse("FALKORDB_PORT")?,
            username: req("FALKORDB_USERNAME")?,
            // Optional: FalkorDB endpoints without auth run without a password.
            password: std::env::var("FALKORDB_PASSWORD").ok().filter(|v| !v.is_empty()),
            use_tls: req_bool("FALKORDB_USE_TLS")?,
            embedding_dim: req_parse("FALKORDB_EMBEDDING_DIM")?,
            timeout_secs: req_parse("FALKORDB_TIMEOUT_SECS")?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebConfig {
    /// Enable/disable web scraping feature.
    pub enabled: bool,
    /// Default max pages per crawl.
    pub max_pages: usize,
    /// Default max depth per crawl.
    pub max_depth: usize,
    /// Default delay between requests (ms).
    pub crawl_delay_ms: u64,
    /// HTTP User-Agent string.
    pub user_agent: String,
    /// Per-request HTTP timeout (seconds).
    pub request_timeout_secs: u64,
    /// Maximum concurrent crawl jobs.
    pub max_concurrent_crawls: usize,
}

impl WebConfig {
    pub fn from_env() -> Result<Self, String> {
        Ok(Self {
            enabled: req_bool("WEB_SCRAPING_ENABLED")?,
            max_pages: req_parse("WEB_MAX_PAGES")?,
            max_depth: req_parse("WEB_MAX_DEPTH")?,
            crawl_delay_ms: req_parse("WEB_CRAWL_DELAY_MS")?,
            user_agent: req("WEB_USER_AGENT")?,
            request_timeout_secs: req_parse("WEB_REQUEST_TIMEOUT_SECS")?,
            max_concurrent_crawls: req_parse("WEB_MAX_CONCURRENT_CRAWLS")?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub group_id: String,
    pub client_id: String,
    pub auto_offset_reset: String,
    pub enable_auto_commit: bool,
}

impl KafkaConfig {
    pub fn from_env() -> Result<Self, String> {
        Ok(Self {
            bootstrap_servers: req("KAFKA_BOOTSTRAP_SERVERS")?,
            group_id: req("REPO_UNI_PROC_KAFKA_GROUP_ID")?,
            client_id: req("KAFKA_CLIENT_ID")?,
            auto_offset_reset: req("KAFKA_AUTO_OFFSET_RESET")?,
            enable_auto_commit: req_bool("KAFKA_ENABLE_AUTO_COMMIT")?,
        })
    }
}



impl Config {
    pub fn from_env() -> crate::core::Result<Self> {
        let map_err = crate::core::error::ProcessorError::ConfigError;

        let host = req("HOST").map_err(map_err)?;
        // PORT may arrive under deploy-platform aliases — all env, no literals.
        let port: u16 = req("PORT")
            .or_else(|_| req("REPO_UNI_PROC_PORT"))
            .or_else(|_| req("HTTP_PORT"))
            .map_err(map_err)?
            .parse()
            .map_err(|_| map_err("Invalid value for environment variable: PORT".to_string()))?;

        let auth_middleware_url = req("AUTH_MIDDLEWARE_URL").map_err(map_err)?;
        let workers = req_parse("WORKERS").map_err(map_err)?;

        // Pipeline Config
        let pipeline = PipelineConfig {
            max_file_size: req_parse("PIPELINE_MAX_FILE_SIZE").map_err(map_err)?,
            chunk_size: req_parse("PIPELINE_CHUNK_SIZE").map_err(map_err)?,
            max_batch_size: req_parse("PIPELINE_MAX_BATCH_SIZE").map_err(map_err)?,
            timeout: Duration::from_secs(req_parse("PIPELINE_TIMEOUT_SECS").map_err(map_err)?),
        };

        let database = DatabaseConfig::from_env()
            .map_err(crate::core::error::ProcessorError::ConfigError)?;
        let falkordb = FalkordbConfig::from_env()
            .map_err(crate::core::error::ProcessorError::ConfigError)?;
        let kafka = KafkaConfig::from_env()
            .map_err(crate::core::error::ProcessorError::ConfigError)?;
        let web = WebConfig::from_env()
            .map_err(crate::core::error::ProcessorError::ConfigError)?;
        Ok(Self {
            server: ServerConfig {
                host,
                port,
                workers,
                auth_middleware_url,
            },
            database,
            pipeline,
            falkordb,
            kafka,
            web,
        })
    }
}
