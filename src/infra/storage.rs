//! Storage functionality for processed data
//!
//! Uses FalkorDB for graph/vector,
//! Azure Blob Storage for chunks, and PostgreSQL for metadata.

use crate::core::{Result, ProcessorError};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use uuid::Uuid;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct PostgresStorage {
    pool: PgPool,
}

impl PostgresStorage {
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(20) // Increased from 10 for better concurrency
            .acquire_timeout(std::time::Duration::from_secs(30)) // Add explicit timeout
            .connect(database_url)
            .await
            .map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;

        Self::create_tables(&pool).await?;

        Ok(Self { pool })
    }

    async fn create_tables(pool: &PgPool) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS processing_jobs (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                source_id VARCHAR(255) NOT NULL,
                user_id VARCHAR(255) NOT NULL,
                status VARCHAR(255) NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                completed_at TIMESTAMP WITH TIME ZONE,
                error_message TEXT
            )
            "#
        )
        .execute(pool)
        .await
        .map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;

        sqlx::query("ALTER TABLE processing_jobs ADD COLUMN IF NOT EXISTS user_id VARCHAR(255) DEFAULT 'system'")
            .execute(pool)
            .await
            .ok();

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_processing_jobs_user_id ON processing_jobs(user_id)
            "#
        )
        .execute(pool)
        .await
        .map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS file_metadata (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                source_id VARCHAR(255) NOT NULL,
                user_id VARCHAR(255) NOT NULL,
                filename VARCHAR(255) NOT NULL,
                file_type VARCHAR(255) NOT NULL,
                language VARCHAR(255),
                size_bytes INTEGER,
                line_count INTEGER,
                processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                UNIQUE(source_id, filename)
            )
            "#
        )
        .execute(pool)
        .await
        .map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;

        sqlx::query("ALTER TABLE file_metadata ADD COLUMN IF NOT EXISTS user_id VARCHAR(255) DEFAULT 'system'")
            .execute(pool)
            .await
            .ok();

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_file_metadata_source_id ON file_metadata(source_id)
            "#
        )
        .execute(pool)
        .await
        .map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_file_metadata_user_id ON file_metadata(user_id)
            "#
        )
        .execute(pool)
        .await
        .map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS chunk_snapshot (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                source_id VARCHAR(255) NOT NULL,
                filename VARCHAR(255) NOT NULL,
                start_byte INTEGER NOT NULL,
                end_byte INTEGER NOT NULL,
                chunk_key VARCHAR(255) NOT NULL,
                chunk_hash VARCHAR(255) NOT NULL,
                commit_id VARCHAR(255),
                embedding_model VARCHAR(255),
                last_indexed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                tombstone BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
            "#
        )
        .execute(pool)
        .await
        .map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_chunk_snapshot_source_file ON chunk_snapshot(source_id, filename)
            "#
        )
        .execute(pool)
        .await
        .map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_chunk_snapshot_key ON chunk_snapshot(chunk_key)
            "#
        )
        .execute(pool)
        .await
        .map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;

        Ok(())
    }

    pub async fn store_file_metadata(&self, metadata: FileMetadata, user_id: &str) -> Result<Uuid> {
        // Validate user_id is not empty
        if user_id.is_empty() {
            return Err(ProcessorError::ValidationError("user_id cannot be empty".to_string()));
        }

        let id = Uuid::new_v4();

        sqlx::query(
            r#"
            INSERT INTO file_metadata (id, source_id, user_id, filename, file_type, language, size_bytes, line_count)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (source_id, filename) DO UPDATE SET
                file_type = EXCLUDED.file_type,
                language = EXCLUDED.language,
                size_bytes = EXCLUDED.size_bytes,
                line_count = EXCLUDED.line_count,
                processed_at = NOW()
            "#
        )
        .bind(id)
        .bind(&metadata.source_id)
        .bind(user_id)
        .bind(&metadata.filename)
        .bind(&metadata.file_type)
        .bind(&metadata.language)
        .bind(metadata.size_bytes)
        .bind(metadata.line_count)
        .execute(&self.pool)
        .await
        .map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;

        Ok(id)
    }

    pub async fn get_file_metadata(&self, source_id: &str, filename: &str, user_id: &str) -> Result<Option<FileMetadata>> {
        // Validate user_id is not empty
        if user_id.is_empty() {
            return Err(ProcessorError::ValidationError("user_id cannot be empty".to_string()));
        }

        let row = sqlx::query(
            r#"
            SELECT id, source_id, user_id, filename, file_type, language, size_bytes, line_count, processed_at 
            FROM file_metadata 
            WHERE source_id = $1 AND filename = $2 AND user_id = $3
            "#
        )
        .bind(source_id)
        .bind(filename)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;

        if let Some(row) = row {
            Ok(Some(FileMetadata {
                id: row.get("id"),
                source_id: row.get("source_id"),
                user_id: row.get("user_id"),
                filename: row.get("filename"),
                file_type: row.get("file_type"),
                language: row.get("language"),
                size_bytes: row.get("size_bytes"),
                line_count: row.get("line_count"),
                processed_at: row.get("processed_at"),
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn list_files_by_source(&self, source_id: &str, user_id: &str) -> Result<Vec<FileMetadata>> {
        // Validate user_id is not empty
        if user_id.is_empty() {
            return Err(ProcessorError::ValidationError("user_id cannot be empty".to_string()));
        }

        let rows = sqlx::query(
            r#"
            SELECT id, source_id, user_id, filename, file_type, language, size_bytes, line_count, processed_at 
            FROM file_metadata 
            WHERE source_id = $1 AND user_id = $2
            ORDER BY filename
            "#
        )
        .bind(source_id)
        .bind(user_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;

        let mut files = Vec::new();
        for row in rows {
            files.push(FileMetadata {
                id: row.get("id"),
                source_id: row.get("source_id"),
                user_id: row.get("user_id"),
                filename: row.get("filename"),
                file_type: row.get("file_type"),
                language: row.get("language"),
                size_bytes: row.get("size_bytes"),
                line_count: row.get("line_count"),
                processed_at: row.get("processed_at"),
            });
        }

        Ok(files)
    }

    pub async fn create_processing_job(&self, source_id: &str, user_id: &str) -> Result<Uuid> {
        // Validate user_id is not empty
        if user_id.is_empty() {
            return Err(ProcessorError::ValidationError("user_id cannot be empty".to_string()));
        }

        let job_id = Uuid::new_v4();
        let status = "pending";

        sqlx::query(
            r#"
            INSERT INTO processing_jobs (id, source_id, user_id, status)
            VALUES ($1, $2, $3, $4)
            "#
        )
        .bind(job_id)
        .bind(source_id)
        .bind(user_id)
        .bind(status)
        .execute(&self.pool)
        .await
        .map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;

        Ok(job_id)
    }

    pub async fn update_job_status(&self, job_id: Uuid, status: &str, error_message: Option<&str>) -> Result<()> {
        let query = if status == "completed" {
            r#"
            UPDATE processing_jobs 
            SET status = $1, completed_at = NOW(), error_message = $2 
            WHERE id = $3
            "#
        } else {
            r#"
            UPDATE processing_jobs 
            SET status = $1, error_message = $2 
            WHERE id = $3
            "#
        };

        sqlx::query(query)
            .bind(status)
            .bind(error_message)
            .bind(job_id)
            .execute(&self.pool)
            .await
            .map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;

        Ok(())
    }

    pub async fn get_job_status(&self, job_id: Uuid, user_id: &str) -> Result<Option<ProcessingJob>> {
        // Validate user_id is not empty
        if user_id.is_empty() {
            return Err(ProcessorError::ValidationError("user_id cannot be empty".to_string()));
        }

        let row = sqlx::query(
            r#"
            SELECT id, source_id, user_id, status, created_at, completed_at, error_message 
            FROM processing_jobs 
            WHERE id = $1 AND user_id = $2
            "#
        )
        .bind(job_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;

        if let Some(row) = row {
            Ok(Some(ProcessingJob {
                id: row.get("id"),
                source_id: row.get("source_id"),
                user_id: row.get("user_id"),
                status: row.get("status"),
                created_at: row.get("created_at"),
                completed_at: row.get("completed_at"),
                error_message: row.get("error_message"),
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn delete_file_metadata(&self, source_id: &str, filename: &str) -> Result<u64> {
        let res = sqlx::query("DELETE FROM file_metadata WHERE source_id = $1 AND filename = $2")
            .bind(source_id)
            .bind(filename)
            .execute(&self.pool)
            .await
            .map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;
            
        Ok(res.rows_affected())
    }

    pub async fn delete_chunks_for_source(&self, source_id: &str) -> Result<u64> {
        let mut total_deleted = 0;

        // 1. Delete file metadata
        let metadata_res = sqlx::query("DELETE FROM file_metadata WHERE source_id = $1")
            .bind(source_id)
            .execute(&self.pool)
            .await
            .map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;
        total_deleted += metadata_res.rows_affected();

        // 2. Delete processing jobs
        let jobs_res = sqlx::query("DELETE FROM processing_jobs WHERE source_id = $1")
            .bind(source_id)
            .execute(&self.pool)
            .await
            .map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;
        total_deleted += jobs_res.rows_affected();

        // 3. Delete chunk snapshots
        let snapshots_res = sqlx::query("DELETE FROM chunk_snapshot WHERE source_id = $1")
            .bind(source_id)
            .execute(&self.pool)
            .await
            .map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;
        total_deleted += snapshots_res.rows_affected();

        Ok(total_deleted)
    }

    pub async fn store_chunk_snapshot(&self, snapshot: ChunkSnapshot) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO chunk_snapshot (id, source_id, filename, start_byte, end_byte, chunk_key, chunk_hash, commit_id, embedding_model, last_indexed_at, tombstone)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (id) DO UPDATE SET
                start_byte = EXCLUDED.start_byte,
                end_byte = EXCLUDED.end_byte,
                chunk_key = EXCLUDED.chunk_key,
                chunk_hash = EXCLUDED.chunk_hash,
                commit_id = EXCLUDED.commit_id,
                embedding_model = EXCLUDED.embedding_model,
                last_indexed_at = EXCLUDED.last_indexed_at,
                tombstone = EXCLUDED.tombstone,
                updated_at = NOW()
            "#
        )
        .bind(snapshot.id)
        .bind(&snapshot.source_id)
        .bind(&snapshot.filename)
        .bind(snapshot.start_byte)
        .bind(snapshot.end_byte)
        .bind(&snapshot.chunk_key)
        .bind(&snapshot.chunk_hash)
        .bind(&snapshot.commit_id)
        .bind(&snapshot.embedding_model)
        .bind(snapshot.last_indexed_at)
        .bind(snapshot.tombstone)
        .execute(&self.pool)
        .await
        .map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;

        Ok(())
    }

    pub async fn store_chunk_snapshots(&self, snapshots: Vec<ChunkSnapshot>) -> Result<()> {
        let mut tx = self.pool.begin().await.map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;
        
        for snapshot in snapshots {
            sqlx::query(
                r#"
                INSERT INTO chunk_snapshot (id, source_id, filename, start_byte, end_byte, chunk_key, chunk_hash, commit_id, embedding_model, last_indexed_at, tombstone)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (id) DO UPDATE SET
                    start_byte = EXCLUDED.start_byte,
                    end_byte = EXCLUDED.end_byte,
                    chunk_key = EXCLUDED.chunk_key,
                    chunk_hash = EXCLUDED.chunk_hash,
                    commit_id = EXCLUDED.commit_id,
                    embedding_model = EXCLUDED.embedding_model,
                    last_indexed_at = EXCLUDED.last_indexed_at,
                    tombstone = EXCLUDED.tombstone,
                    updated_at = NOW()
                "#
            )
            .bind(snapshot.id)
            .bind(&snapshot.source_id)
            .bind(&snapshot.filename)
            .bind(snapshot.start_byte)
            .bind(snapshot.end_byte)
            .bind(&snapshot.chunk_key)
            .bind(&snapshot.chunk_hash)
            .bind(&snapshot.commit_id)
            .bind(&snapshot.embedding_model)
            .bind(snapshot.last_indexed_at)
            .bind(snapshot.tombstone)
            .execute(&mut *tx)
            .await
            .map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;
        }
        
        tx.commit().await.map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    /// Transactionally apply chunk diffs for a file:
    /// - Inserts new chunks
    /// - Sets tombstone=TRUE for deleted chunks
    pub async fn atomic_replace_file_chunks(
        &self,
        _source_id: &str,
        _filename: &str,
        to_insert: Vec<ChunkSnapshot>,
        to_delete: Vec<Uuid>,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await.map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;

        // 1. Mark missing chunks as tombstones
        if !to_delete.is_empty() {
            sqlx::query(
                r#"
                UPDATE chunk_snapshot
                SET tombstone = TRUE, updated_at = NOW()
                WHERE id = ANY($1)
                "#
            )
            .bind(&to_delete)
            .execute(&mut *tx)
            .await
            .map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;
        }

        // 2. Insert new chunks
        for snapshot in to_insert {
            sqlx::query(
                r#"
                INSERT INTO chunk_snapshot (id, source_id, filename, start_byte, end_byte, chunk_key, chunk_hash, commit_id, embedding_model, last_indexed_at, tombstone)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (id) DO UPDATE SET
                    start_byte = EXCLUDED.start_byte,
                    end_byte = EXCLUDED.end_byte,
                    chunk_key = EXCLUDED.chunk_key,
                    chunk_hash = EXCLUDED.chunk_hash,
                    commit_id = EXCLUDED.commit_id,
                    embedding_model = EXCLUDED.embedding_model,
                    last_indexed_at = EXCLUDED.last_indexed_at,
                    tombstone = EXCLUDED.tombstone,
                    updated_at = NOW()
                "#
            )
            .bind(snapshot.id)
            .bind(&snapshot.source_id)
            .bind(&snapshot.filename)
            .bind(snapshot.start_byte)
            .bind(snapshot.end_byte)
            .bind(&snapshot.chunk_key)
            .bind(&snapshot.chunk_hash)
            .bind(&snapshot.commit_id)
            .bind(&snapshot.embedding_model)
            .bind(snapshot.last_indexed_at)
            .bind(snapshot.tombstone)
            .execute(&mut *tx)
            .await
            .map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;
        }

        tx.commit().await.map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    pub async fn get_chunk_snapshots(&self, source_id: &str, filename: &str) -> Result<Vec<ChunkSnapshot>> {
        let rows = sqlx::query(
            r#"
            SELECT id, source_id, filename, start_byte, end_byte, chunk_key, chunk_hash, commit_id, embedding_model, last_indexed_at, tombstone, created_at, updated_at
            FROM chunk_snapshot
            WHERE source_id = $1 AND filename = $2
            "#
        )
        .bind(source_id)
        .bind(filename)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;

        let mut snapshots = Vec::new();
        for row in rows {
            snapshots.push(ChunkSnapshot {
                id: row.get("id"),
                source_id: row.get("source_id"),
                filename: row.get("filename"),
                start_byte: row.get("start_byte"),
                end_byte: row.get("end_byte"),
                chunk_key: row.get("chunk_key"),
                chunk_hash: row.get("chunk_hash"),
                commit_id: row.get("commit_id"),
                embedding_model: row.get("embedding_model"),
                last_indexed_at: row.get("last_indexed_at"),
                tombstone: row.get("tombstone"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            });
        }
        Ok(snapshots)
    }

    pub async fn mark_chunks_deleted(&self, source_id: &str, chunk_keys: &[String]) -> Result<()> {
        if chunk_keys.is_empty() {
            return Ok(());
        }
        sqlx::query(
            r#"
            UPDATE chunk_snapshot
            SET tombstone = TRUE, updated_at = NOW()
            WHERE source_id = $1 AND chunk_key = ANY($2)
            "#
        )
        .bind(source_id)
        .bind(chunk_keys)
        .execute(&self.pool)
        .await
        .map_err(|e| ProcessorError::DatabaseError(e.to_string()))?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub id: Uuid,
    pub source_id: String,
    pub user_id: String,
    pub filename: String,
    pub file_type: String,
    pub language: Option<String>,
    pub size_bytes: Option<i32>,
    pub line_count: Option<i32>,
    pub processed_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
pub struct ProcessingJob {
    pub id: Uuid,
    pub source_id: String,
    pub user_id: String,
    pub status: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChunkSnapshot {
    pub id: Uuid,
    pub source_id: String,
    pub filename: String,
    pub start_byte: i32,
    pub end_byte: i32,
    pub chunk_key: String,
    pub chunk_hash: String,
    pub commit_id: Option<String>,
    pub embedding_model: Option<String>,
    pub last_indexed_at: chrono::DateTime<chrono::Utc>,
    pub tombstone: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

pub struct GraphSync {
    falkordb_storage: Arc<FalkordbStorage>,
}

impl GraphSync {
    pub fn new(falkordb_storage: Arc<FalkordbStorage>) -> Self {
        Self { falkordb_storage }
    }

    pub async fn trigger_relationship_building(&self, source_id: &str, user_id: &str) -> Result<GraphSyncResponse> {
        tracing::info!("GraphSync: building cross-file relationships for source_id={} user_id={}", source_id, user_id);
        
        let user_graph = self.falkordb_storage.with_user_graph(user_id);
        let query = format!("MATCH (c:Vector_Chunk {{source_id: '{}'}}) RETURN c.id, c.content, c.chunk_type, c.metadata", source_id.replace('\'', "\\'"));
        
        let results = match user_graph.execute_query(&query).await {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Failed to fetch chunks for graph sync: {}", e);
                return Err(crate::core::ProcessorError::DatabaseError(e.to_string()));
            }
        };

        let parsed = parse_graphdb_response(results, &["id", "content", "chunk_type", "metadata"]);

        let mut chunks = Vec::new();
        for row in parsed {
            let id = row.get("id").and_then(|v| v.as_str()).unwrap_or_default().to_string();
            let content = row.get("content").and_then(|v| v.as_str()).unwrap_or_default().to_string();
            let chunk_type_str = row.get("chunk_type").and_then(|v| v.as_str()).unwrap_or_default().to_string();
            let metadata_str = row.get("metadata").and_then(|v| v.as_str()).unwrap_or("{}").to_string();
            
            // Parse chunk type back to enum
            let chunk_type: crate::core::chunking::ChunkType = if chunk_type_str.contains("Code") {
                crate::core::chunking::ChunkType::Code {
                    language: "unknown".to_string(),
                    semantic_type: crate::core::chunking::CodeSemanticType::File,
                }
            } else if chunk_type_str.contains("Document") {
                crate::core::chunking::ChunkType::Document {
                    format: "unknown".to_string(),
                    semantic_type: crate::core::chunking::DocumentSemanticType::Paragraph,
                }
            } else {
                crate::core::chunking::ChunkType::Web {
                    url: "unknown".to_string(),
                    semantic_type: crate::core::chunking::WebSemanticType::Paragraph,
                }
            };

            // Parse metadata back to ChunkMetadata
            let metadata = serde_json::from_str::<crate::core::chunking::ChunkMetadata>(&metadata_str)
                .unwrap_or_default();

            // Reconstruct a dummy Chunk struct just for SymbolIndex
            use uuid::Uuid;
            let mut chunk = crate::core::chunking::Chunk::new(
                source_id.to_string(),
                id.clone(), // use id as file_path to avoid losing it
                content,
                chunk_type,
                crate::core::chunking::ChunkLevel::Structural,
            );
            chunk.metadata = metadata;
            if let Ok(uuid) = Uuid::parse_str(&id.split('|').next().unwrap_or(&id)) {
                chunk.id = uuid;
            }
            chunks.push(chunk);
        }

        tracing::info!("GraphSync: Fetched {} code chunks for cross-file resolution", chunks.len());

        let symbol_index = crate::processors::symbol::SymbolIndex::build(&chunks);
        let mut symbol_rels = symbol_index.resolve_cross_file_references(&chunks);

        // Fallback: Connect chunks intelligently
        use std::collections::{HashSet, HashMap};
        let mut connected_chunks = HashSet::new();
        for rel in &symbol_rels {
            connected_chunks.insert(rel.source_chunk_id);
            connected_chunks.insert(rel.target_chunk_id);
        }

        // Group by file path (which is stored in chunk's metadata or we can extract from composite ID)
        // Wait, the dummy chunk's `file_path` holds the composite ID `uuid|actual_file_path`.
        let mut chunks_by_file: HashMap<String, Vec<&crate::core::chunking::Chunk>> = HashMap::new();
        for chunk in &chunks {
            let actual_path = chunk.file_path.split('|').nth(1).unwrap_or("unknown").to_string();
            chunks_by_file.entry(actual_path).or_default().push(chunk);
        }

        let mut all_file_first_chunks = Vec::new();

        for (_file_path, file_chunks) in &chunks_by_file {
            // Sort chunks by line number if possible, or just preserve order
            if file_chunks.is_empty() { continue; }
            
            all_file_first_chunks.push(file_chunks[0].id);
            
            // Connect chunks within the same file sequentially with NEXT_CHUNK
            for i in 0..file_chunks.len().saturating_sub(1) {
                let source = file_chunks[i].id;
                let target = file_chunks[i + 1].id;
                
                let edge = crate::graph::models::ChunkRelationship::new(
                    source,
                    target,
                    crate::graph::models::ChunkRelationType::NextChunk,
                    1.0,
                ).with_evidence(vec![
                    crate::graph::models::RelationshipEvidence {
                        evidence_type: "sequential".to_string(),
                        location: "same_file".to_string(),
                        snippet: None,
                    }
                ]);
                symbol_rels.push(edge);
                connected_chunks.insert(source);
                connected_chunks.insert(target);
            }
            
            // Connect the File chunk (first chunk) to all granular chunks with ParentChild (CONTAINS)
            if file_chunks.len() > 1 {
                let file_chunk_id = file_chunks[0].id;
                for i in 1..file_chunks.len() {
                    let child_id = file_chunks[i].id;
                    let edge = crate::graph::models::ChunkRelationship::new(
                        file_chunk_id,
                        child_id,
                        crate::graph::models::ChunkRelationType::ParentChild,
                        1.0,
                    ).with_evidence(vec![
                        crate::graph::models::RelationshipEvidence {
                            evidence_type: "hierarchy".to_string(),
                            location: "same_file_contains".to_string(),
                            snippet: None,
                        }
                    ]);
                    symbol_rels.push(edge);
                }
            }
        }


        tracing::info!("GraphSync: Found {} cross-file relationships (including fallbacks)", symbol_rels.len());

        let chunk_map: std::collections::HashMap<uuid::Uuid, &String> = chunks
            .iter()
            .map(|c| (c.id, &c.file_path))
            .collect();

        let mut rels_added = 0;
        for rel in symbol_rels {
            let source_composite = chunk_map.get(&rel.source_chunk_id);
            let target_composite = chunk_map.get(&rel.target_chunk_id);
            
            if let (Some(s_comp), Some(t_comp)) = (source_composite, target_composite) {
                let metadata_val = serde_json::to_value(&rel.metadata).unwrap_or(serde_json::json!({}));
                if let Err(e) = user_graph.store_relationship(
                    s_comp,
                    t_comp,
                    rel.relationship_type.label(),
                    rel.confidence as f64,
                    &metadata_val
                ).await {
                    tracing::warn!("Failed to store cross-file relationship: {}", e);
                } else {
                    rels_added += 1;
                }
            }
        }

        Ok(GraphSyncResponse {
            success: true,
            message: "Cross-file relationship building complete".to_string(),
            data: Some(GraphSyncData {
                source_id: source_id.to_string(),
                chunks_found: chunks.len(),
                episodes_added: rels_added,
                errors: vec![],
                timestamp: chrono::Utc::now().to_rfc3339(),
            }),
        })
    }

    pub async fn get_graph_status(&self, source_id: &str) -> Result<GraphStatus> {
        tracing::debug!(
            "graph_sync: get_graph_status called for source_id={} (stub)",
            source_id
        );
        Ok(GraphStatus {
            processed: true,
            episode_count: 0,
            node_count: 0,
            edge_count: 0,
            last_updated: chrono::Utc::now().to_rfc3339(),
        })
    }
}

#[derive(Debug)]
pub struct GraphSyncResponse {
    pub success: bool,
    pub message: String,
    pub data: Option<GraphSyncData>,
}

#[derive(Debug)]
pub struct GraphSyncData {
    pub source_id: String,
    pub chunks_found: usize,
    pub episodes_added: usize,
    pub errors: Vec<String>,
    pub timestamp: String,
}

#[derive(Debug, Deserialize)]
pub struct GraphStatus {
    pub processed: bool,
    pub episode_count: usize,
    pub node_count: usize,
    pub edge_count: usize,
    pub last_updated: String,
}

/// Chunk metadata â€” shared across storage backends
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ProcessedChunk {
    pub id: String,
    pub source_id: String,
    pub workspace_id: String,
    pub filename: String,
    pub content_type: String,
    pub language: String,
    pub content: String,
    pub metadata: ChunkMetadata,
    pub processed_at: chrono::DateTime<chrono::Utc>,
    pub chunk_hash: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChunkMetadata {
    pub line_count: usize,
    pub character_count: usize,
    pub word_count: usize,
    pub embedding_model: Option<String>,
    pub processing_time_ms: u64,
}// FalkorDB Storage Adapter for Unified Processor
//
// Provides graph + vector storage backed by FalkorDB over Redis protocol.
// Replaces the former Memgraph/Bolt implementation.
//
use anyhow::{anyhow, Context};
use bb8_redis::{bb8::Pool, RedisConnectionManager};
use redis::cmd;
use serde_json::Value;
use std::sync::Arc;
use tracing::{info, warn};

/// FalkorDB storage backend for chunks and embeddings.
#[derive(Clone)]
pub struct FalkordbStorage {
    pool: Arc<Pool<RedisConnectionManager>>,
    /// Logical graph name â€” used in log messages and query prefixes.
    pub graph_name: String,
    /// Whether a vector index was successfully created at startup.
    _has_vector_index: bool,
}

impl FalkordbStorage {
    pub fn new(pool: Arc<Pool<RedisConnectionManager>>, graph_name: &str, has_vector_index: bool) -> Self {
        Self {
            pool,
            graph_name: graph_name.to_string(),
            _has_vector_index: has_vector_index,
        }
    }

    pub fn get_pool(&self) -> Arc<Pool<RedisConnectionManager>> {
        self.pool.clone()
    }

    /// Create a new FalkordbStorage instance targeting the per-user graph `graph-<user_id>`.
    /// This shares the underlying connection pool but routes all queries to the user's own graph.
    pub fn with_user_graph(&self, user_id: &str) -> Self {
        let graph_name = format!("graph-{}", user_id);
        Self {
            pool: self.pool.clone(),
            graph_name,
            _has_vector_index: self._has_vector_index,
        }
    }

    pub async fn ensure_user_graph(&self) -> Result<()> {
        // NOTE: user creation and index creation is handled ONLY by auth-middleware.
        // We do nothing here to comply with the strict separation of concerns.
        Ok(())
    }


    pub async fn execute_query(&self, cypher: &str) -> Result<redis::Value> {
        let mut conn = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("FalkorDB pool get error: {:#}", e);
                return Err(anyhow::anyhow!("Failed to get redis connection: {}", e).into());
            }
        };
        let res = cmd("GRAPH.QUERY")
            .arg(&self.graph_name)
            .arg(cypher)
            .arg("--compact")
            .query_async::<_, redis::Value>(&mut *conn)
            .await;
            
        match res {
            Ok(v) => Ok(v),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("already indexed") || msg.contains("already exists") {
                    Ok(redis::Value::Nil)
                } else {
                    let truncated_cypher = if cypher.len() > 300 {
                        format!("{}... [TRUNCATED]", &cypher[..300])
                    } else {
                        cypher.to_string()
                    };
                    tracing::error!("FalkorDB query error (cypher: {}): {:?}", truncated_cypher, e);
                    Err(e).context("Failed to execute GRAPH.QUERY")?
                }
            }
        }
    }

    // â”€â”€â”€ Schema / index management â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    pub async fn create_vector_index(&self, index_name: &str, dimension: usize) -> Result<()> {
        let cypher = format!(
            r#"CREATE VECTOR INDEX FOR (c:Vector_Chunk) ON (c.embeddings) OPTIONS {{dimension: {}, similarityFunction: 'cosine'}}"#,
            dimension
        );

        match self.execute_query(&cypher).await {
            Ok(_) => {
                info!("Created FalkorDB vector index '{}' (dim={})", index_name, dimension);
                Ok(())
            }
            Err(e) => {
                let msg = format!("{:#}", e);
                if msg.contains("already indexed") || msg.contains("already exists") {
                    info!("FalkorDB vector index '{}' already exists (dim={})", index_name, dimension);
                    Ok(())
                } else {
                    warn!("Vector index creation warning: {}", e);
                    Err(anyhow!("Vector index execution failed: {}", e).into())
                }
            }
        }
    }

    pub async fn ensure_indexes(&self) -> Result<()> {
        let stmts = [
            "CREATE INDEX FOR (c:Vector_Chunk) ON (c.source_id)",
            "CREATE INDEX FOR (c:Vector_Chunk) ON (c.id)",
            "CREATE INDEX FOR (c:Vector_Chunk) ON (c.chunk_type)",
            "CREATE INDEX FOR (c:Vector_Chunk) ON (c.owner_id)",
        ];
        for stmt in &stmts {
            if let Err(e) = self.execute_query(stmt).await {
                let msg = format!("{:#}", e);
                if !msg.contains("already indexed") && !msg.contains("already exists") {
                    warn!("Index creation warning ({}): {}", stmt, msg);
                }
            }
        }
        info!("FalkorDB structural indexes ensured for graph '{}'", self.graph_name);
        Ok(())
    }

    // â”€â”€â”€ Write operations â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    pub async fn update_chunk_embedding(
        &self,
        chunk_id: &str,
        embedding: &[f32],
    ) -> Result<()> {
        let embedding_str = format!("{:?}", embedding);
        let cypher = format!(
            r#"MATCH (c:Vector_Chunk {{id: "{chunk_id}"}})
               SET c.embeddings = vecf32({embedding_str})
            "#
        );
        self.execute_query(&cypher).await?;
        Ok(())
    }

    pub async fn store_chunk_with_embedding(
        &self,
        chunk_id: &str,
        source_id: &str,
        content: &str,
        embedding: &[f32],
        chunk_type: &str,
        metadata: &Value,
        model: &str,
        owner_id: &str,
        repo_file_path: &str,
        language: &str,
    ) -> Result<()> {
        // FalkorDB uses CYPHER parameters string interpolation
        let metadata_str = serde_json::to_string(metadata).unwrap_or_else(|_| "{}".to_string());
        let now = chrono::Utc::now().timestamp_millis();

        // Doing string replace on backslashes and double quotes. Also strip null bytes which terminate C-strings in Redis.
        let content_esc = content.replace('\0', "").replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n").replace('\r', "\\r");
        let metadata_esc = metadata_str.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n").replace('\r', "\\r");
        let chunk_type_esc = chunk_type.replace('\\', "\\\\").replace('"', "\\\"");
        let owner_id_esc = owner_id.replace('"', "\\\"");
        let repo_file_path_esc = repo_file_path.replace('\\', "\\\\").replace('"', "\\\"");
        let language_esc = language.replace('\\', "\\\\").replace('"', "\\\"");
        
        let file_name = std::path::Path::new(repo_file_path).file_name().and_then(|s| s.to_str()).unwrap_or("");
        let file_name_esc = file_name.replace('\\', "\\\\").replace('"', "\\\"");
        let repository_name_esc = source_id.replace('\\', "\\\\").replace('"', "\\\"");
        
        let embedding_str = format!("{:?}", embedding); // Standard [f32, f32, ...] format
        
        // Build the embedding clause conditionally: skip setting `c.embedding` when
        // the vector is empty (Step 1.5 direct-store) so we don't write a zero-dim
        // value that conflicts with the vector index.  The embeddings callback will
        // populate the real vector later via MERGE.
        let embedding_clause = if embedding.is_empty() {
            String::new()
        } else {
            format!(r#"c.embeddings  = vecf32({embedding_str}),"#)
        };

        // Let's use direct parameters through standard CYPHER dict if supported, otherwise explicit formatting
        let cypher = format!(
            r#"MERGE (c:Vector_Chunk {{id: "{chunk_id}"}})
               ON CREATE SET c.created_at = {now}
               SET c.source_id   = "{source_id}",
                   c.content     = "{content_esc}",
                   {embedding_clause}
                   c.chunk_type  = "{chunk_type_esc}",
                   c.metadata    = "{metadata_esc}",
                   c.model       = "{model}",
                   c.owner_id    = "{owner_id_esc}",
                   c.repo_file_path = "{repo_file_path_esc}",
                   c.file_name   = "{file_name_esc}",
                   c.repository_name = "{repository_name_esc}",
                   c.language    = "{language_esc}",
                   c.updated_at  = {now}
            "#,
            chunk_id = chunk_id,
            source_id = source_id,
            content_esc = content_esc,
            embedding_clause = embedding_clause,
            chunk_type_esc = chunk_type_esc,
            metadata_esc = metadata_esc,
            model = model,
            owner_id_esc = owner_id_esc,
            repo_file_path_esc = repo_file_path_esc,
            file_name_esc = file_name_esc,
            repository_name_esc = repository_name_esc,
            language_esc = language_esc,
            now = now
        );

        self.execute_query(&cypher).await?;
        info!("Stored chunk '{}' in FalkorDB graph '{}'", chunk_id, self.graph_name);
        Ok(())
    }

    pub async fn store_relationship(
        &self,
        from_chunk_id: &str,
        to_chunk_id: &str,
        relationship_type: &str,
        confidence: f64,
        metadata: &Value,
    ) -> Result<()> {
        let metadata_esc = serde_json::to_string(metadata)
            .unwrap_or_else(|_| "{}".to_string())
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n")
            .replace('\r', "\\r");
        let now = chrono::Utc::now().timestamp_millis();
        let rel_type = sanitise_rel_type(relationship_type);

        let cypher = format!(
            r#"
            MERGE (a:Vector_Chunk {{id: "{from_id}"}})
            MERGE (b:Vector_Chunk {{id: "{to_id}"}})
            MERGE (a)-[r:{rel_type}]->(b)
            SET r.confidence = {confidence},
                r.metadata   = "{metadata_esc}",
                r.created_at = {now}
            "#,
            from_id = from_chunk_id,
            to_id = to_chunk_id,
            rel_type = rel_type,
            confidence = confidence,
            metadata_esc = metadata_esc,
            now = now
        );

        self.execute_query(&cypher).await?;
        info!("Stored relationship '{}' between '{}' â†’ '{}' in FalkorDB", rel_type, from_chunk_id, to_chunk_id);
        Ok(())
    }

    // â”€â”€â”€ Entity operations â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    /// Ensure indexes exist for `Code_Entity` nodes.
    pub async fn ensure_entity_indexes(&self) -> Result<()> {
        let stmts = [
            "CREATE INDEX FOR (e:Code_Entity) ON (e.name)",
            "CREATE INDEX FOR (e:Code_Entity) ON (e.entity_type)",
            "CREATE INDEX FOR (e:Code_Entity) ON (e.source_id)",
            "CREATE INDEX FOR (e:Code_Entity) ON (e.qualified_name)",
            "CREATE INDEX FOR (e:Code_Entity) ON (e.owner_id)",
        ];
        for stmt in &stmts {
            if let Err(e) = self.execute_query(stmt).await {
                let msg = format!("{:#}", e);
                if !msg.contains("already indexed") && !msg.contains("already exists") {
                    warn!("Code_Entity index creation warning ({}): {}", stmt, msg);
                }
            }
        }
        info!("FalkorDB Code_Entity indexes ensured for graph '{}'", self.graph_name);
        Ok(())
    }

    /// Store a code entity in FalkorDB (MERGE on qualified_name for idempotency).
    pub async fn store_entity(
        &self,
        entity_id: &str,
        name: &str,
        qualified_name: &str,
        entity_type: &str,
        source_id: &str,
        file_path: &str,
        start_line: usize,
        end_line: usize,
        signature: Option<&str>,
        visibility: Option<&str>,
        metadata: &Value,
        owner_id: &str,
    ) -> Result<()> {
        let name_esc = name.replace('"', "\\\"");
        let qname_esc = qualified_name.replace('"', "\\\"");
        let type_esc = entity_type.replace('"', "\\\"");
        let source_esc = source_id.replace('"', "\\\"");
        let path_esc = file_path.replace('"', "\\\"");
        let sig_esc = signature.unwrap_or("").replace('"', "\\\"");
        let vis_esc = visibility.unwrap_or("public").replace('"', "\\\"");
        let metadata_esc = serde_json::to_string(metadata)
            .unwrap_or_else(|_| "{}".to_string()).replace('"', "\\\"");
        let owner_id_esc = owner_id.replace('"', "\\\"");
        let now = chrono::Utc::now().timestamp_millis();

        let cypher = format!(
            r#"MERGE (e:Code_Entity {{qualified_name: "{qname_esc}"}})
               ON CREATE SET e.created_at = {now}
               SET e.id          = "{entity_id}",
                   e.name        = "{name_esc}",
                   e.entity_type = "{type_esc}",
                   e.source_id   = "{source_esc}",
                   e.file_path   = "{path_esc}",
                   e.start_line  = {start_line},
                   e.end_line    = {end_line},
                   e.signature   = "{sig_esc}",
                   e.visibility  = "{vis_esc}",
                   e.metadata    = "{metadata_esc}",
                   e.owner_id    = "{owner_id_esc}",
                   e.updated_at  = {now}
            "#
        );

        self.execute_query(&cypher).await?;
        info!("Stored Code_Entity '{}' ({}) in FalkorDB graph '{}'", name, entity_type, self.graph_name);
        Ok(())
    }

    /// Store a relationship between two entities.
    pub async fn store_entity_relationship(
        &self,
        from_entity_qname: &str,
        to_entity_qname: &str,
        relationship_type: &str,
        confidence: f64,
        detection_method: &str,
        metadata: &Value,
    ) -> Result<()> {
        let from_esc = from_entity_qname.replace('"', "\\\"");
        let to_esc = to_entity_qname.replace('"', "\\\"");
        let method_esc = detection_method.replace('"', "\\\"");
        let metadata_esc = serde_json::to_string(metadata)
            .unwrap_or_else(|_| "{}".to_string()).replace('"', "\\\"");
        let now = chrono::Utc::now().timestamp_millis();
        let rel_type = sanitise_rel_type(relationship_type);

        let cypher = format!(
            r#"
            MATCH (a:Code_Entity {{qualified_name: "{from_esc}"}})
            MATCH (b:Code_Entity {{qualified_name: "{to_esc}"}})
            MERGE (a)-[r:{rel_type}]->(b)
            SET r.confidence       = {confidence},
                r.detection_method = "{method_esc}",
                r.metadata         = "{metadata_esc}",
                r.created_at       = {now}
            "#
        );

        self.execute_query(&cypher).await?;
        info!("Stored entity relationship '{}' between '{}' â†’ '{}' in FalkorDB", rel_type, from_entity_qname, to_entity_qname);
        Ok(())
    }

    /// Query entities by name pattern.
    pub async fn find_entities_by_name(&self, name_pattern: &str, owner_id: &str) -> Result<Vec<Value>> {
        let pattern_esc = name_pattern.replace('"', "\\\"");
        let owner_id_esc = owner_id.replace('"', "\\\"");
        let cypher = format!(
            r#"
            MATCH (e:Code_Entity)
            WHERE e.name =~ "(?i).*{pattern_esc}.*" AND e.owner_id = "{owner_id_esc}"
            RETURN e.id            AS entity_id,
                   e.name          AS name,
                   e.qualified_name AS qualified_name,
                   e.entity_type   AS entity_type,
                   e.source_id     AS source_id,
                   e.file_path     AS file_path
            ORDER BY e.name ASC
            LIMIT 100
            "#
        );

        let res = self.execute_query(&cypher).await?;
        Ok(parse_graphdb_response(res, &["entity_id", "name", "qualified_name", "entity_type", "source_id", "file_path"]))
    }

    /// Query entities by source_id.
    pub async fn get_entities_by_source(&self, source_id: &str, owner_id: &str) -> Result<Vec<Value>> {
        let source_esc = source_id.replace('"', "\\\"");
        let owner_id_esc = owner_id.replace('"', "\\\"");
        let cypher = format!(
            r#"
            MATCH (e:Code_Entity {{source_id: "{source_esc}"}})
            WHERE e.owner_id = "{owner_id_esc}"
            RETURN e.id            AS entity_id,
                   e.name          AS name,
                   e.qualified_name AS qualified_name,
                   e.entity_type   AS entity_type,
                   e.file_path     AS file_path,
                   e.start_line    AS start_line,
                   e.end_line      AS end_line
            ORDER BY e.file_path, e.start_line ASC
            "#
        );

        let res = self.execute_query(&cypher).await?;
        Ok(parse_graphdb_response(res, &["entity_id", "name", "qualified_name", "entity_type", "file_path", "start_line", "end_line"]))
    }

    /// Store a repository metadata node.
    pub async fn store_repository(
        &self,
        repo_id: &str,
        name: &str,
        url: &str,
        branch: &str,
        commit_hash: &str,
        repo_type: &str,
        owner_id: &str,
    ) -> Result<()> {
        let name_esc = name.replace('"', "\\\"");
        let url_esc = url.replace('"', "\\\"");
        let branch_esc = branch.replace('"', "\\\"");
        let commit_esc = commit_hash.replace('"', "\\\"");
        let type_esc = repo_type.replace('"', "\\\"");
        let owner_id_esc = owner_id.replace('"', "\\\"");
        let now = chrono::Utc::now().timestamp_millis();

        let cypher = format!(
            r#"MERGE (r:Repository {{id: "{repo_id}"}})
               ON CREATE SET r.created_at = {now}
               SET r.name        = "{name_esc}",
                   r.url         = "{url_esc}",
                   r.branch      = "{branch_esc}",
                   r.commit_hash = "{commit_esc}",
                   r.repo_type   = "{type_esc}",
                   r.owner_id    = "{owner_id_esc}",
                   r.updated_at  = {now}
            "#
        );

        self.execute_query(&cypher).await?;
        info!("Stored Repository '{}' in FalkorDB graph '{}'", name, self.graph_name);
        Ok(())
    }

    /// Link an entity to its source repository.
    pub async fn link_entity_to_repository(
        &self,
        entity_qname: &str,
        repo_id: &str,
    ) -> Result<()> {
        let entity_esc = entity_qname.replace('"', "\\\"");
        let now = chrono::Utc::now().timestamp_millis();

        let cypher = format!(
            r#"
            MATCH (e:Code_Entity {{qualified_name: "{entity_esc}"}})
            MATCH (r:Repository {{id: "{repo_id}"}})
            MERGE (e)-[rel:BELONGS_TO]->(r)
            SET rel.created_at = {now}
            "#
        );

        self.execute_query(&cypher).await?;
        Ok(())
    }

    // â”€â”€â”€ Web page operations â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    /// Ensure indexes exist for `Web_Page` nodes.
    /// Called once during startup alongside `ensure_indexes()`.
    pub async fn ensure_web_indexes(&self) -> Result<()> {
        let stmts = [
            "CREATE INDEX FOR (p:Web_Page) ON (p.url)",
            "CREATE INDEX FOR (p:Web_Page) ON (p.domain)",
            "CREATE INDEX FOR (p:Web_Page) ON (p.source_id)",
            "CREATE INDEX FOR (p:Web_Page) ON (p.owner_id)",
            "CREATE INDEX FOR (r:Repository) ON (r.owner_id)",
        ];
        for stmt in &stmts {
            if let Err(e) = self.execute_query(stmt).await {
                let msg = format!("{:#}", e);
                if !msg.contains("already indexed") && !msg.contains("already exists") {
                    warn!("Web_Page index creation warning ({}): {}", stmt, msg);
                }
            }
        }
        info!("FalkorDB Web_Page indexes ensured for graph '{}'", self.graph_name);
        Ok(())
    }

    /// Store a web page node in FalkorDB (MERGE on URL for idempotency).
    pub async fn store_web_page(
        &self,
        url: &str,
        domain: &str,
        title: &str,
        description: &str,
        word_count: usize,
        source_id: &str,
        owner_id: &str,
    ) -> Result<()> {
        let url_esc = url.replace('"', "\\\"");
        let domain_esc = domain.replace('"', "\\\"");
        let title_esc = title.replace('"', "\\\"");
        let desc_esc = description.replace('"', "\\\"");
        let owner_id_esc = owner_id.replace('"', "\\\"");
        let now = chrono::Utc::now().timestamp_millis();

        let cypher = format!(
            r#"MERGE (p:Web_Page {{url: "{url_esc}"}})
               ON CREATE SET p.created_at = {now}
               SET p.domain     = "{domain_esc}",
                   p.title      = "{title_esc}",
                   p.description = "{desc_esc}",
                   p.word_count = {word_count},
                   p.source_id  = "{source_id}",
                   p.owner_id   = "{owner_id_esc}",
                   p.updated_at = {now}
            "#
        );

        self.execute_query(&cypher).await?;
        info!("Stored Web_Page '{}' in FalkorDB graph '{}'", url, self.graph_name);
        Ok(())
    }

    /// Store a link relationship between two web pages.
    pub async fn store_web_link(
        &self,
        from_url: &str,
        to_url: &str,
        link_text: &str,
        link_type: &str,
    ) -> Result<()> {
        let from_esc = from_url.replace('"', "\\\"");
        let to_esc = to_url.replace('"', "\\\"");
        let text_esc = link_text.replace('"', "\\\"");
        let type_esc = link_type.replace('"', "\\\"");
        let now = chrono::Utc::now().timestamp_millis();

        let cypher = format!(
            r#"
            MATCH (a:Web_Page {{url: "{from_esc}"}})
            MERGE (b:Web_Page {{url: "{to_esc}"}})
            MERGE (a)-[r:LINKS_TO]->(b)
            SET r.link_text  = "{text_esc}",
                r.link_type  = "{type_esc}",
                r.created_at = {now}
            "#
        );

        self.execute_query(&cypher).await?;
        Ok(())
    }

    // â”€â”€â”€ Read operations â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    pub async fn get_chunks_by_source(&self, source_id: &str, owner_id: &str) -> Result<Vec<Value>> {
        let owner_id_esc = owner_id.replace('"', "\\\"");
        let cypher = format!(
            r#"
            MATCH (c:Vector_Chunk {{source_id: "{source_id}"}})
            WHERE c.owner_id = "{owner_id_esc}"
            RETURN c.id         AS chunk_id,
                   c.content    AS content,
                   c.chunk_type AS chunk_type,
                   c.metadata   AS metadata,
                   c.model      AS model,
                   c.created_at AS created_at
            ORDER BY c.created_at ASC
            "#
        );

        let res = self.execute_query(&cypher).await?;
        Ok(parse_graphdb_response(res, &["chunk_id", "content", "chunk_type", "metadata", "model", "created_at"]))
    }

    pub async fn get_chunk_content(&self, chunk_id: &str) -> Result<Option<String>> {
        let chunk_id_esc = chunk_id.replace('"', "\\\"");
        let cypher = format!(r#"MATCH (c:Vector_Chunk {{id: "{chunk_id_esc}"}}) RETURN c.content AS content"#);
        let res = self.execute_query(&cypher).await?;
        let parsed = parse_graphdb_response(res, &["content"]);
        if let Some(first) = parsed.first() {
            if let Some(content) = first.get("content").and_then(|v| v.as_str()) {
                return Ok(Some(content.to_string()));
            }
        }
        Ok(None)
    }


    pub async fn search_similar_chunks(
        &self,
        query_vector: &[f32],
        limit: usize,
        threshold: f64,
        owner_id: &str,
    ) -> Result<Vec<Value>> {
        let embedding_str = serde_json::to_string(query_vector).expect("query vector serializes");
        let owner_id_esc = owner_id.replace('"', "\\\"");

        // FalkorDB db.idx.vector.queryNodes syntax
        let cypher = format!(
            r#"
            CALL db.idx.vector.queryNodes('Vector_Chunk', 'embeddings', {limit}, vecf32({embedding_str})) YIELD node, score
            WHERE score >= {threshold} AND node.owner_id = "{owner_id_esc}"
            RETURN node.id         AS chunk_id,
                   node.content    AS content,
                   node.chunk_type AS chunk_type,
                   node.source_id  AS source_id,
                   score           AS score
            ORDER BY score DESC
            "#
        );

        let res = self.execute_query(&cypher).await?;
        Ok(parse_graphdb_response(res, &["chunk_id", "content", "chunk_type", "source_id", "score"]))
    }

    pub async fn delete_chunks_by_ids(&self, chunk_ids: &[Uuid]) -> Result<usize> {
        if chunk_ids.is_empty() {
            return Ok(0);
        }
        
        let ids_str = chunk_ids.iter()
            .map(|id| format!("'{}'", id))
            .collect::<Vec<_>>()
            .join(", ");
            
        let cypher = format!(
            r#"
            MATCH (n:Vector_Chunk)
            WHERE n.id IN [{ids_str}]
            WITH collect(n) AS nodes, count(n) AS count
            UNWIND nodes AS n
            DETACH DELETE n
            RETURN count AS total
            "#
        );
        
        let res = self.execute_query(&cypher).await?;
        Ok(extract_total(res))
    }

    /// Deep delete all graph artifacts associated with a source.


    pub async fn get_graph_stats(&self) -> Result<GraphStats> {
        let cypher = r#"
            MATCH (c:Vector_Chunk)
            OPTIONAL MATCH (c)-[r]->()
            RETURN count(DISTINCT c) AS chunk_count,
                   count(r)           AS relationship_count
        "#;

        let res = self.execute_query(cypher).await?;
        let parsed = parse_graphdb_response(res, &["chunk_count", "relationship_count"]);
        
        let mut stats = GraphStats::default();
        if let Some(first) = parsed.first() {
            stats.chunk_count = first.get("chunk_count").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
            stats.relationship_count = first.get("relationship_count").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
        }
        Ok(stats)
    }

    pub async fn run_node_similarity(&self, _min_similarity: f64) -> Result<Vec<Value>> {
        // FalkorDB alternative to node_similarity...
        // For now returning empty until custom node similarity exists in this implementation
        Ok(vec![])
    }

    pub async fn detect_communities(&self) -> Result<Vec<Value>> {
        // FalkorDB Louvain / community detection might require different ALGO call
        // For now returning empty
        Ok(vec![])
    }

    pub async fn find_path(
        &self,
        from_chunk_id: &str,
        to_chunk_id: &str,
    ) -> Result<Vec<Value>> {
        let cypher = format!(
            r#"
            MATCH path = shortestPath(
                (a:Vector_Chunk {{id: "{from_chunk_id}"}})-[*]-(b:Vector_Chunk {{id: "{to_chunk_id}"}})
            )
            UNWIND nodes(path) AS n
            RETURN n.id AS node_id, n.content AS content
            "#
        );

        let res = self.execute_query(&cypher).await?;
        Ok(parse_graphdb_response(res, &["node_id", "content"]))
    }
}

// â”€â”€â”€ Graph statistics â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

#[derive(Debug, Clone, Default)]
pub struct GraphStats {
    pub chunk_count: usize,
    pub relationship_count: usize,
}

// â”€â”€â”€ Factory function â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

pub async fn create_falkordb_storage(
    host: &str,
    port: u16,
    graph_name: &str,
    username: &str,
    password: &str,
    use_tls: bool,
    _embedding_dim: usize,
) -> Result<Arc<FalkordbStorage>> {
    // Construct standard redis:// or rediss:// connection URL
    // We forcibly use `redis` (plaintext) because the FalkorDB cloud instance on port 50860
    // does not use TLS; a stale FALKORDB_USE_TLS=true would cause a TLS handshake timeout.
    let scheme = "redis";
    let auth = if username.is_empty() && password.is_empty() {
        "".to_string()
    } else if username.is_empty() {
        format!(":{}@", password)
    } else {
        format!("{}:{}@", username, password)
    };
    
    let url = format!("{}://{}{}:{}", scheme, auth, host, port);

    info!(
        "Building FalkorDB connection pool → {}:{} tls={} user={} graph='{}'",
        host, port, use_tls, username, graph_name
    );

    let manager = RedisConnectionManager::new(url)
        .context("Failed to configure redis connection")?;

    // Build the pool sized for concurrent ingestion bursts:
    // 1. Larger pool to handle concurrent requests
    // 2. Generous connection timeout
    // 3. Lazy connections — bb8 does NOT connect at build time
    let pool = Pool::builder()
        .max_size(32) // concurrent request headroom
        .connection_timeout(std::time::Duration::from_secs(30))
        .build(manager)
        .await
        .context("Failed to build FalkorDB connection pool")?;

    let pool = Arc::new(pool);

    // ── Smoke-test the connection once so we surface auth / network errors
    //    at startup rather than on the first real request.
    match tokio::time::timeout(
        std::time::Duration::from_secs(60),
        pool.get(),
    ).await {
        Ok(Ok(mut conn)) => {
            match tokio::time::timeout(
                std::time::Duration::from_secs(15),
                redis::cmd("PING").query_async::<_, String>(&mut *conn),
            ).await {
                Ok(Ok(pong)) => {
                    info!("FalkorDB PING → {} (connection verified)", pong);
                }
                Ok(Err(e)) => {
                    warn!("FalkorDB PING failed (auth or protocol error): {}. \
                           Pool created — operations will fail until resolved.", e);
                }
                Err(_) => {
                    warn!("FalkorDB PING timed out after 15s. \
                           Pool created — operations will retry on each request.");
                }
            }
        }
        Ok(Err(e)) => {
            // bb8 could not establish a connection within 30 seconds.
            // Log the FULL error (kind + detail) so the logs are useful.
            warn!(
                "FalkorDB initial connection failed after 30s: {:#}. \
                 Pool created — will retry on each request.",
                e
            );
        }
        Err(_) => {
            warn!("FalkorDB pool.get() timed out after 60s. \
                   Pool created — will retry on each request.");
        }
    }

    // Per-user graphs (`graph-<user_id>`) are initialized lazily via
    // `ensure_user_graph()` when processing requests for each user.
    let storage = Arc::new(FalkordbStorage::new(pool, graph_name, false));
    info!("FalkorDB pool ready (pool_size=32, timeout=30s, lazy connections — per-user graphs on demand)");
    Ok(storage)
}


// â”€â”€â”€ Helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

/// Parses FalkorDB `--compact` response to generic JSON structure
pub fn parse_graphdb_response(res: redis::Value, expected_headers: &[&str]) -> Vec<Value> {
    // A compact response is typically:
    // [ [headers array], [ [row 1 values array], [row 2 values array] ], [stats array] ]
    let mut out = Vec::new();
    if let redis::Value::Bulk(ref top) = res {
        if top.len() >= 2 {
            if let redis::Value::Bulk(ref rows) = top[1] {
                for row_val in rows {
                    if let redis::Value::Bulk(ref cols) = row_val {
                        let mut map = serde_json::Map::new();
                        for (i, col_val) in cols.iter().enumerate() {
                            if i < expected_headers.len() {
                                let key = expected_headers[i].to_string();
                                let mut val = Value::Null;
                                // Simple interpretation of redis values into json
                                match col_val {
                                    redis::Value::Int(ref n) => { val = Value::from(*n); }
                                    redis::Value::Data(ref d) => {
                                        if let Ok(s) = String::from_utf8(d.clone()) {
                                            val = Value::String(s);
                                        }
                                    }
                                    redis::Value::Status(ref s) => { val = Value::String(s.clone()); }
                                    redis::Value::Bulk(ref _a) => {
                                        /* recursively or leave as null for now, typically properties come back formatted */
                                    }
                                    _ => {}
                                }
                                map.insert(key, val);
                            }
                        }
                        out.push(Value::Object(map));
                    }
                }
            }
        }
    }
    out
}

fn sanitise_rel_type(raw: &str) -> String {
    raw.chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .flat_map(|c| c.to_uppercase())
        .collect()
}

fn extract_total(res: redis::Value) -> usize {
    let parsed = parse_graphdb_response(res, &["total"]);
    if let Some(first) = parsed.first() {
        if let Some(total) = first.get("total").and_then(|v| v.as_u64()) {
            return total as usize;
        }
    }
    0
}
