// protocol.rs - Database Network Protocol Definitions
// Bu modül modern database'lerin network communication protocol'ünü implement eder
// JSON-RPC benzeri ama database-specific operations için optimize edilmiş

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;
use crate::{DatabaseError, ComparisonOperator, SortDirection, IndexType, IsolationLevel};

/// Protocol version - backward compatibility için
pub const PROTOCOL_VERSION: &str = "1.0";

/// Request message - client'tan server'a gönderilen
/// Bu design modern RPC framework'lerinden (gRPC, JSON-RPC) ilham alır
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseRequest {
    /// Unique request identifier - response matching için
    /// Client'lar concurrent request'leri track etmek için kullanır
    pub id: String,

    /// API method name - hangi operation yapılacağı
    pub method: String,

    /// Method-specific parameters - flexible JSON format
    pub params: Value,

    /// Protocol version - compatibility check için
    #[serde(default = "default_version")]
    pub version: String,

    /// Optional client metadata - debugging ve monitoring için
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

/// Response message - server'dan client'a dönen
/// Success veya error state'lerini handle eder
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseResponse {
    /// Request ID - hangi request'e ait olduğu
    pub id: String,

    /// Success result (mutually exclusive with error)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,

    /// Error details (mutually exclusive with result)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ProtocolError>,

    /// Protocol version
    #[serde(default = "default_version")]
    pub version: String,

    /// Server metadata - performance metrics, timing etc.
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

/// Protocol-level error information
/// HTTP status codes'dan ilham alınmış error code system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolError {
    /// Error code - standardized error types
    pub code: i32,

    /// Human-readable error message
    pub message: String,

    /// Additional error context
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

/// Standard protocol error codes
/// Bu codes HTTP status codes ve database-specific patterns'ı combine eder
pub mod error_codes {
    // Client errors (4xx equivalent)
    pub const INVALID_REQUEST: i32 = 400;
    pub const INVALID_METHOD: i32 = 404;
    pub const INVALID_PARAMS: i32 = 422;
    pub const DOCUMENT_NOT_FOUND: i32 = 404;
    pub const DOCUMENT_ALREADY_EXISTS: i32 = 409;
    pub const VERSION_MISMATCH: i32 = 409;

    // Server errors (5xx equivalent)
    pub const INTERNAL_ERROR: i32 = 500;
    pub const LOCK_ERROR: i32 = 503;
    pub const TRANSACTION_ERROR: i32 = 500;
    pub const STORAGE_ERROR: i32 = 507;

    // Database-specific errors (6xx custom range)
    pub const QUERY_ERROR: i32 = 600;
    pub const INDEX_ERROR: i32 = 601;
    pub const SERIALIZATION_ERROR: i32 = 602;
    pub const WAL_ERROR: i32 = 603;
}

/// Method-specific parameter structures
/// Her database operation için type-safe parameter definitions

/// CREATE operation parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateParams {
    /// Document data to insert
    pub data: Value,

    /// Optional specific document ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Uuid>,

    /// Transaction ID if operation is transactional
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<Uuid>,
}

/// READ operation parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadParams {
    /// Single document ID to read
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Uuid>,

    /// Multiple document IDs for batch read
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ids: Option<Vec<Uuid>>,

    /// Pagination offset
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,

    /// Result limit
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,

    /// Transaction ID if operation is transactional
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<Uuid>,
}

/// UPDATE operation parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateParams {
    /// Document ID to update
    pub id: Uuid,

    /// New document data
    pub data: Value,

    /// Expected version for optimistic locking
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_version: Option<u64>,

    /// Transaction ID if operation is transactional
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<Uuid>,
}

/// DELETE operation parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteParams {
    /// Document ID to delete
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Uuid>,

    /// Multiple document IDs for batch delete
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ids: Option<Vec<Uuid>>,

    /// Expected version for optimistic locking
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_version: Option<u64>,

    /// Transaction ID if operation is transactional
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<Uuid>,
}

/// QUERY operation parameters - advanced search capabilities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryParams {
    /// Where clauses for filtering
    #[serde(default)]
    pub where_clauses: Vec<WhereClauseParams>,

    /// Sort specifications
    #[serde(default)]
    pub sort_clauses: Vec<SortClauseParams>,

    /// Field projection - which fields to return
    #[serde(skip_serializing_if = "Option::is_none")]
    pub projection: Option<ProjectionParams>,

    /// Result pagination
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,

    /// Result limit
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,

    /// Transaction ID if operation is transactional
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<Uuid>,
}

/// Where clause for query filtering
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhereClauseParams {
    /// JSON path to field (e.g., "profile.age")
    pub path: String,

    /// Comparison operator
    pub operator: ComparisonOperator,

    /// Value to compare against
    pub value: Value,
}

/// Sort clause for query ordering
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortClauseParams {
    /// JSON path to field for sorting
    pub path: String,

    /// Sort direction
    pub direction: SortDirection,
}

/// Projection specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionParams {
    /// Fields to include (mutually exclusive with exclude)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include: Option<Vec<String>>,

    /// Fields to exclude (mutually exclusive with include)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclude: Option<Vec<String>>,
}

/// INDEX operation parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexParams {
    /// Index name
    pub name: String,

    /// Fields to index
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<String>>,

    /// Index type
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_type: Option<IndexType>,

    /// Operation type: "create", "drop", "list", "stats"
    pub operation: String,
}

/// TRANSACTION operation parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionParams {
    /// Transaction operation: "begin", "commit", "rollback"
    pub operation: String,

    /// Transaction ID (for commit/rollback)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<Uuid>,

    /// Isolation level (for begin)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub isolation_level: Option<IsolationLevel>,

    /// Timeout in seconds (for begin)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,
}

/// Response result types - type-safe response handling

/// CREATE operation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateResult {
    /// Created document ID
    pub id: Uuid,

    /// Document version
    pub version: u64,

    /// Creation timestamp
    pub created_at: DateTime<Utc>,

    /// Full document (if requested)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub document: Option<Value>,
}

/// READ operation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadResult {
    /// Single document (for single ID queries)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub document: Option<Value>,

    /// Multiple documents (for batch/list queries)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub documents: Option<Vec<Value>>,

    /// Total count (for pagination)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_count: Option<usize>,
}

/// UPDATE operation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateResult {
    /// Updated document ID
    pub id: Uuid,

    /// New document version
    pub version: u64,

    /// Update timestamp
    pub updated_at: DateTime<Utc>,

    /// Updated document (if requested)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub document: Option<Value>,
}

/// DELETE operation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteResult {
    /// Number of documents deleted
    pub deleted_count: usize,

    /// List of deleted document IDs
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_ids: Option<Vec<Uuid>>,
}

/// QUERY operation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// Matching documents
    pub documents: Vec<Value>,

    /// Total count (without limit)
    pub total_count: usize,

    /// Query execution time in milliseconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_time_ms: Option<u64>,

    /// Whether index was used for optimization
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_used: Option<String>,
}

/// INDEX operation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexResult {
    /// Index name
    pub name: String,

    /// Operation result message
    pub message: String,

    /// Index statistics (for stats operation)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<Value>,

    /// List of indexes (for list operation)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexes: Option<Vec<Value>>,
}

/// TRANSACTION operation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionResult {
    /// Transaction ID
    pub transaction_id: Uuid,

    /// Operation result message
    pub message: String,

    /// Transaction status
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

/// Connection statistics - monitoring için
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionStats {
    /// Connection ID
    pub connection_id: String,

    /// Client address
    pub client_address: String,

    /// Connection start time
    pub connected_at: DateTime<Utc>,

    /// Total requests processed
    pub requests_processed: u64,

    /// Total bytes sent
    pub bytes_sent: u64,

    /// Total bytes received
    pub bytes_received: u64,

    /// Last activity time
    pub last_activity: DateTime<Utc>,
}

/// Server statistics - health monitoring için
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerStats {
    /// Server start time
    pub started_at: DateTime<Utc>,

    /// Active connections count
    pub active_connections: usize,

    /// Total requests served
    pub total_requests: u64,

    /// Average response time (milliseconds)
    pub avg_response_time_ms: f64,

    /// Current memory usage (estimated)
    pub memory_usage_bytes: usize,

    /// Database statistics
    pub database_stats: Value,
}

// Helper functions

fn default_version() -> String {
    PROTOCOL_VERSION.to_string()
}

impl DatabaseRequest {
    /// Creates a new request with unique ID
    pub fn new(method: &str, params: Value) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            method: method.to_string(),
            params,
            version: default_version(),
            metadata: HashMap::new(),
        }
    }

    /// Adds metadata to request
    pub fn with_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }

    /// Validates request structure
    pub fn validate(&self) -> Result<(), ProtocolError> {
        // Version check
        if self.version != PROTOCOL_VERSION {
            return Err(ProtocolError {
                code: error_codes::INVALID_REQUEST,
                message: format!("Unsupported protocol version: {}", self.version),
                data: None,
            });
        }

        // Method validation
        if self.method.is_empty() {
            return Err(ProtocolError {
                code: error_codes::INVALID_METHOD,
                message: "Method cannot be empty".to_string(),
                data: None,
            });
        }

        // ID validation
        if self.id.is_empty() {
            return Err(ProtocolError {
                code: error_codes::INVALID_REQUEST,
                message: "Request ID cannot be empty".to_string(),
                data: None,
            });
        }

        Ok(())
    }
}

impl DatabaseResponse {
    /// Creates a successful response
    pub fn success(request_id: &str, result: Value) -> Self {
        Self {
            id: request_id.to_string(),
            result: Some(result),
            error: None,
            version: default_version(),
            metadata: HashMap::new(),
        }
    }

    /// Creates an error response
    pub fn error(request_id: &str, error: ProtocolError) -> Self {
        Self {
            id: request_id.to_string(),
            result: None,
            error: Some(error),
            version: default_version(),
            metadata: HashMap::new(),
        }
    }

    /// Adds performance metadata
    pub fn with_timing(mut self, duration_ms: u64) -> Self {
        self.metadata.insert("duration_ms".to_string(), duration_ms.to_string());
        self
    }

    /// Adds server metadata
    pub fn with_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }
}

impl From<DatabaseError> for ProtocolError {
    /// Converts internal database errors to protocol errors
    /// Bu mapping layer modern API design'ının önemli bir parçası
    fn from(db_error: DatabaseError) -> Self {
        match db_error {
            DatabaseError::DocumentNotFound { id } => ProtocolError {
                code: error_codes::DOCUMENT_NOT_FOUND,
                message: format!("Document not found: {}", id),
                data: Some(serde_json::json!({ "document_id": id })),
            },

            DatabaseError::DocumentAlreadyExists { id } => ProtocolError {
                code: error_codes::DOCUMENT_ALREADY_EXISTS,
                message: format!("Document already exists: {}", id),
                data: Some(serde_json::json!({ "document_id": id })),
            },

            DatabaseError::VersionMismatch { expected, actual } => ProtocolError {
                code: error_codes::VERSION_MISMATCH,
                message: format!("Version mismatch: expected {}, got {}", expected, actual),
                data: Some(serde_json::json!({
                    "expected_version": expected,
                    "actual_version": actual
                })),
            },

            DatabaseError::LockError { reason } => ProtocolError {
                code: error_codes::LOCK_ERROR,
                message: format!("Lock error: {}", reason),
                data: None,
            },

            DatabaseError::TransactionError { message } => ProtocolError {
                code: error_codes::TRANSACTION_ERROR,
                message: format!("Transaction error: {}", message),
                data: None,
            },

            DatabaseError::InvalidQuery { message } => ProtocolError {
                code: error_codes::QUERY_ERROR,
                message: format!("Invalid query: {}", message),
                data: None,
            },

            DatabaseError::SerializationError { message } => ProtocolError {
                code: error_codes::SERIALIZATION_ERROR,
                message: format!("Serialization error: {}", message),
                data: None,
            },

            DatabaseError::ValidationError { field, message } => ProtocolError {
                code: error_codes::INVALID_PARAMS,
                message: format!("Validation error in field '{}': {}", field, message),
                data: Some(serde_json::json!({ "field": field })),
            },

            DatabaseError::CapacityExceeded { max_capacity } => ProtocolError {
                code: error_codes::STORAGE_ERROR,
                message: format!("Storage capacity exceeded: max {} documents", max_capacity),
                data: Some(serde_json::json!({ "max_capacity": max_capacity })),
            },

            DatabaseError::DocumentTooLarge { size, max_size } => ProtocolError {
                code: error_codes::INVALID_PARAMS,
                message: format!("Document too large: {} bytes, max {} bytes", size, max_size),
                data: Some(serde_json::json!({
                    "document_size": size,
                    "max_size": max_size
                })),
            },
        }
    }
}

/// Message framing for TCP communication
/// Length-prefixed messages için helper functions
pub mod framing {
    use std::io;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Message frame structure
    /// [4 bytes length][message data]
    /// Bu approach Redis, MongoDB gibi database'ler tarafından kullanılır

    /// Writes a framed message to TCP stream
    /// Length prefix ile message boundary'lerini belirleriz
    pub async fn write_message<W>(writer: &mut W, message: &[u8]) -> io::Result<()>
    where
        W: AsyncWriteExt + Unpin,
    {
        // 4-byte length prefix (little-endian)
        let length = message.len() as u32;
        writer.write_all(&length.to_le_bytes()).await?;

        // Message data
        writer.write_all(message).await?;
        writer.flush().await?;

        Ok(())
    }

    /// Reads a framed message from TCP stream
    /// Length prefix'i okuyup exact message size'ını alırız
    pub async fn read_message<R>(reader: &mut R) -> io::Result<Vec<u8>>
    where
        R: AsyncReadExt + Unpin,
    {
        // Read 4-byte length prefix
        let mut length_bytes = [0u8; 4];
        reader.read_exact(&mut length_bytes).await?;
        let length = u32::from_le_bytes(length_bytes);

        // Prevent excessive memory allocation (DoS protection)
        const MAX_MESSAGE_SIZE: u32 = 100 * 1024 * 1024; // 100MB
        if length > MAX_MESSAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Message too large: {} bytes", length),
            ));
        }

        // Read message data
        let mut message = vec![0u8; length as usize];
        reader.read_exact(&mut message).await?;

        Ok(message)
    }
}

/// Request/Response helper methods
impl CreateParams {
    pub fn new(data: Value) -> Self {
        Self {
            data,
            id: None,
            transaction_id: None,
        }
    }

    pub fn with_id(mut self, id: Uuid) -> Self {
        self.id = Some(id);
        self
    }

    pub fn with_transaction(mut self, tx_id: Uuid) -> Self {
        self.transaction_id = Some(tx_id);
        self
    }
}

impl ReadParams {
    pub fn by_id(id: Uuid) -> Self {
        Self {
            id: Some(id),
            ids: None,
            offset: None,
            limit: None,
            transaction_id: None,
        }
    }

    pub fn by_ids(ids: Vec<Uuid>) -> Self {
        Self {
            id: None,
            ids: Some(ids),
            offset: None,
            limit: None,
            transaction_id: None,
        }
    }

    pub fn list() -> Self {
        Self {
            id: None,
            ids: None,
            offset: None,
            limit: None,
            transaction_id: None,
        }
    }

    pub fn with_pagination(mut self, offset: usize, limit: usize) -> Self {
        self.offset = Some(offset);
        self.limit = Some(limit);
        self
    }

    pub fn with_transaction(mut self, tx_id: Uuid) -> Self {
        self.transaction_id = Some(tx_id);
        self
    }
}

impl QueryParams {
    pub fn new() -> Self {
        Self {
            where_clauses: Vec::new(),
            sort_clauses: Vec::new(),
            projection: None,
            offset: None,
            limit: None,
            transaction_id: None,
        }
    }

    pub fn with_where(mut self, path: &str, operator: ComparisonOperator, value: Value) -> Self {
        self.where_clauses.push(WhereClauseParams {
            path: path.to_string(),
            operator,
            value,
        });
        self
    }

    pub fn with_sort(mut self, path: &str, direction: SortDirection) -> Self {
        self.sort_clauses.push(SortClauseParams {
            path: path.to_string(),
            direction,
        });
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_projection_include(mut self, fields: Vec<String>) -> Self {
        self.projection = Some(ProjectionParams {
            include: Some(fields),
            exclude: None,
        });
        self
    }
}

/// Standard method names - consistency için constants
pub mod methods {
    pub const CREATE: &str = "create";
    pub const READ: &str = "read";
    pub const UPDATE: &str = "update";
    pub const DELETE: &str = "delete";
    pub const QUERY: &str = "query";
    pub const INDEX: &str = "index";
    pub const TRANSACTION: &str = "transaction";
    pub const STATS: &str = "stats";
    pub const PING: &str = "ping";
}
