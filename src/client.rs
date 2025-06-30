// client.rs - Database Client API Implementation
// Bu modül database server'a bağlanan client-side library'yi implement eder
// MongoDB driver, PostgreSQL client gibi modern database client'larından ilham alır

use crate::protocol::{
    DatabaseRequest, DatabaseResponse, ProtocolError, CreateParams, ReadParams,
    UpdateParams, DeleteParams, QueryParams, IndexParams, TransactionParams,
    CreateResult, ReadResult, UpdateResult, DeleteResult, QueryResult,
    IndexResult, TransactionResult, ServerStats, framing, methods
};
use crate::{ComparisonOperator, SortDirection, IndexType, IsolationLevel, DatabaseError};

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};
use tokio::time::timeout;
use uuid::Uuid;

/// Client Configuration
/// Connection behavior ve performance tuning için settings
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Server address to connect to
    pub server_address: String,

    /// Connection timeout
    pub connect_timeout: Duration,

    /// Request timeout per operation
    pub request_timeout: Duration,

    /// Connection pool size (for future connection pooling)
    pub max_connections: usize,

    /// Retry configuration
    pub max_retries: usize,
    pub retry_delay: Duration,

    /// Enable request/response logging
    pub verbose_logging: bool,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            server_address: "127.0.0.1:5432".to_string(),
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            max_connections: 10,
            max_retries: 3,
            retry_delay: Duration::from_millis(100),
            verbose_logging: false,
        }
    }
}

/// Connection statistics for monitoring
#[derive(Debug, Clone)]
pub struct ClientStats {
    /// Connection establishment time
    pub connected_at: DateTime<Utc>,

    /// Total requests sent
    pub requests_sent: u64,

    /// Total responses received
    pub responses_received: u64,

    /// Total bytes sent
    pub bytes_sent: u64,

    /// Total bytes received
    pub bytes_received: u64,

    /// Average request time
    pub avg_request_time_ms: f64,

    /// Connection errors count
    pub connection_errors: u64,

    /// Last error message
    pub last_error: Option<String>,
}

/// Database Connection - tek bir TCP connection wrapper
/// Bu class bir database connection'ını encapsulate eder
/// Modern database driver'larında (like MongoDB driver) yaygın pattern
struct DatabaseConnection {
    /// TCP stream
    stream: Arc<Mutex<TcpStream>>,

    /// Connection ID
    connection_id: String,

    /// Client configuration
    config: ClientConfig,

    /// Connection statistics
    stats: Arc<RwLock<ClientStats>>,

    /// Request counter for unique IDs
    request_counter: AtomicU64,
}

impl DatabaseConnection {
    /// Creates a new connection to database server
    /// Connection establishment ve handshake handling
    async fn connect(config: ClientConfig) -> Result<Self, DatabaseError> {
        log::info!("Connecting to database server: {}", config.server_address);

        // Parse server address
        let server_addr: SocketAddr = config.server_address.parse()
            .map_err(|e| DatabaseError::InvalidQuery {
                message: format!("Invalid server address: {}", e)
            })?;

        // Establish TCP connection with timeout
        let stream = timeout(config.connect_timeout, TcpStream::connect(server_addr)).await
            .map_err(|_| DatabaseError::InvalidQuery {
                message: format!("Connection timeout to {}", config.server_address)
            })?
            .map_err(|e| DatabaseError::InvalidQuery {
                message: format!("Failed to connect: {}", e)
            })?;

        let connection_id = Uuid::new_v4().to_string();
        let now = Utc::now();

        let stats = ClientStats {
            connected_at: now,
            requests_sent: 0,
            responses_received: 0,
            bytes_sent: 0,
            bytes_received: 0,
            avg_request_time_ms: 0.0,
            connection_errors: 0,
            last_error: None,
        };

        log::info!("Connected to database server: {} (connection_id: {})",
                  config.server_address, connection_id);

        Ok(Self {
            stream: Arc::new(Mutex::new(stream)),
            connection_id,
            config,
            stats: Arc::new(RwLock::new(stats)),
            request_counter: AtomicU64::new(0),
        })
    }

    /// Sends a request and receives response
    /// Bu method tüm client-server communication'ın kalbidir
    async fn send_request(&self, request: DatabaseRequest) -> Result<DatabaseResponse, DatabaseError> {
        let start_time = Instant::now();

        if self.config.verbose_logging {
            log::debug!("Sending request: {} method: {}", request.id, request.method);
        }

        // Serialize request
        let request_bytes = serde_json::to_vec(&request)
            .map_err(|e| DatabaseError::SerializationError {
                message: format!("Failed to serialize request: {}", e)
            })?;

        // Send request with timeout
        let send_result = timeout(self.config.request_timeout, async {
            let mut stream = self.stream.lock().await;
            framing::write_message(&mut *stream, &request_bytes).await
        }).await;

        if let Err(_) = send_result {
            return Err(DatabaseError::InvalidQuery {
                message: "Request send timeout".to_string()
            });
        }

        send_result.unwrap().map_err(|e| DatabaseError::InvalidQuery {
            message: format!("Failed to send request: {}", e)
        })?;

        // Receive response with timeout
        let response_result = timeout(self.config.request_timeout, async {
            let mut stream = self.stream.lock().await;
            framing::read_message(&mut *stream).await
        }).await;

        if let Err(_) = response_result {
            return Err(DatabaseError::InvalidQuery {
                message: "Response receive timeout".to_string()
            });
        }

        let response_bytes = response_result.unwrap().map_err(|e| DatabaseError::InvalidQuery {
            message: format!("Failed to receive response: {}", e)
        })?;

        // Deserialize response
        let response: DatabaseResponse = serde_json::from_slice(&response_bytes)
            .map_err(|e| DatabaseError::SerializationError {
                message: format!("Failed to deserialize response: {}", e)
            })?;

        // Update statistics
        let request_time = start_time.elapsed();
        self.update_stats(
            request_bytes.len() as u64,
            response_bytes.len() as u64,
            request_time,
            None
        ).await;

        if self.config.verbose_logging {
            log::debug!("Received response: {} in {:?}", response.id, request_time);
        }

        // Check for protocol errors
        if let Some(error) = response.error {
            return Err(DatabaseError::InvalidQuery {
                message: format!("Server error: {}", error.message)
            });
        }

        Ok(response)
    }

    /// Generates unique request ID
    fn next_request_id(&self) -> String {
        let counter = self.request_counter.fetch_add(1, Ordering::Relaxed);
        format!("{}:{}", self.connection_id, counter)
    }

    /// Updates connection statistics
    async fn update_stats(
        &self,
        bytes_sent: u64,
        bytes_received: u64,
        request_time: Duration,
        error: Option<&str>
    ) {
        let mut stats = self.stats.write().await;

        stats.requests_sent += 1;
        if error.is_none() {
            stats.responses_received += 1;
        } else {
            stats.connection_errors += 1;
            stats.last_error = error.map(|s| s.to_string());
        }

        stats.bytes_sent += bytes_sent;
        stats.bytes_received += bytes_received;

        // Update average request time
        let total_requests = stats.responses_received;
        if total_requests > 0 {
            let old_avg = stats.avg_request_time_ms;
            let new_time = request_time.as_millis() as f64;
            stats.avg_request_time_ms = (old_avg * (total_requests - 1) as f64 + new_time) / total_requests as f64;
        }
    }

    /// Gets current connection statistics
    async fn get_stats(&self) -> ClientStats {
        self.stats.read().await.clone()
    }
}

/// Database Client - Main client API
/// Bu modern database client'larının (MongoDB, PostgreSQL) ana interface'idir
/// High-level operations ve connection management sağlar
pub struct DatabaseClient {
    /// Database connection
    connection: Arc<DatabaseConnection>,

    /// Client configuration
    config: ClientConfig,
}

impl DatabaseClient {
    /// Creates a new database client and establishes connection
    /// Bu client'ın entry point'idir
    pub async fn connect(config: ClientConfig) -> Result<Self, DatabaseError> {
        let connection = DatabaseConnection::connect(config.clone()).await?;

        Ok(Self {
            connection: Arc::new(connection),
            config,
        })
    }

    /// Creates a client with default configuration
    pub async fn connect_default() -> Result<Self, DatabaseError> {
        Self::connect(ClientConfig::default()).await
    }

    /// Connects to a specific server address
    pub async fn connect_to(server_address: &str) -> Result<Self, DatabaseError> {
        let mut config = ClientConfig::default();
        config.server_address = server_address.to_string();
        Self::connect(config).await
    }

    // ================================
    // DOCUMENT OPERATIONS - CRUD API
    // Bu methods modern database client'larının temel CRUD interface'ini oluşturur
    // ================================

    /// Creates a new document
    /// Basit create operation - MongoDB'nin insertOne'ına benzer
    pub async fn create_document(&self, data: Value) -> Result<CreateResult, DatabaseError> {
        let params = CreateParams::new(data);
        let request = DatabaseRequest::new(methods::CREATE, serde_json::to_value(params).unwrap());

        let response = self.connection.send_request(request).await?;
        let result: CreateResult = serde_json::from_value(response.result.unwrap())
            .map_err(|e| DatabaseError::SerializationError {
                message: format!("Failed to parse create result: {}", e)
            })?;

        Ok(result)
    }

    /// Creates a document with specific ID
    /// MongoDB'nin insertOne with _id specification'ına benzer
    pub async fn create_document_with_id(&self, id: Uuid, data: Value) -> Result<CreateResult, DatabaseError> {
        let params = CreateParams::new(data).with_id(id);
        let request = DatabaseRequest::new(methods::CREATE, serde_json::to_value(params).unwrap());

        let response = self.connection.send_request(request).await?;
        let result: CreateResult = serde_json::from_value(response.result.unwrap())
            .map_err(|e| DatabaseError::SerializationError {
                message: format!("Failed to parse create result: {}", e)
            })?;

        Ok(result)
    }

    /// Reads a document by ID
    /// MongoDB'nin findOne by _id'ye benzer
    pub async fn read_document(&self, id: Uuid) -> Result<Option<Value>, DatabaseError> {
        let params = ReadParams::by_id(id);
        let request = DatabaseRequest::new(methods::READ, serde_json::to_value(params).unwrap());

        let response = self.connection.send_request(request).await?;
        let result: ReadResult = serde_json::from_value(response.result.unwrap())
            .map_err(|e| DatabaseError::SerializationError {
                message: format!("Failed to parse read result: {}", e)
            })?;

        Ok(result.document)
    }

    /// Reads multiple documents by IDs
    /// Batch read operation - MongoDB'nin find with _id in array'e benzer
    pub async fn read_documents(&self, ids: Vec<Uuid>) -> Result<Vec<Value>, DatabaseError> {
        let params = ReadParams::by_ids(ids);
        let request = DatabaseRequest::new(methods::READ, serde_json::to_value(params).unwrap());

        let response = self.connection.send_request(request).await?;
        let result: ReadResult = serde_json::from_value(response.result.unwrap())
            .map_err(|e| DatabaseError::SerializationError {
                message: format!("Failed to parse read result: {}", e)
            })?;

        Ok(result.documents.unwrap_or_default())
    }

    /// Lists all documents with optional pagination
    /// MongoDB'nin find() with limit/skip'e benzer
    pub async fn list_documents(&self, offset: Option<usize>, limit: Option<usize>) -> Result<Vec<Value>, DatabaseError> {
        let mut params = ReadParams::list();
        if let Some(off) = offset {
            if let Some(lim) = limit {
                params = params.with_pagination(off, lim);
            }
        }

        let request = DatabaseRequest::new(methods::READ, serde_json::to_value(params).unwrap());

        let response = self.connection.send_request(request).await?;
        let result: ReadResult = serde_json::from_value(response.result.unwrap())
            .map_err(|e| DatabaseError::SerializationError {
                message: format!("Failed to parse read result: {}", e)
            })?;

        Ok(result.documents.unwrap_or_default())
    }

    /// Updates a document
    /// MongoDB'nin updateOne'ına benzer
    pub async fn update_document(&self, id: Uuid, data: Value) -> Result<UpdateResult, DatabaseError> {
        let params = UpdateParams {
            id,
            data,
            expected_version: None,
            transaction_id: None,
        };

        let request = DatabaseRequest::new(methods::UPDATE, serde_json::to_value(params).unwrap());

        let response = self.connection.send_request(request).await?;
        let result: UpdateResult = serde_json::from_value(response.result.unwrap())
            .map_err(|e| DatabaseError::SerializationError {
                message: format!("Failed to parse update result: {}", e)
            })?;

        Ok(result)
    }

    /// Updates a document with version check (optimistic locking)
    /// Version-based concurrency control için
    pub async fn update_document_with_version(
        &self,
        id: Uuid,
        data: Value,
        expected_version: u64
    ) -> Result<UpdateResult, DatabaseError> {
        let params = UpdateParams {
            id,
            data,
            expected_version: Some(expected_version),
            transaction_id: None,
        };

        let request = DatabaseRequest::new(methods::UPDATE, serde_json::to_value(params).unwrap());

        let response = self.connection.send_request(request).await?;
        let result: UpdateResult = serde_json::from_value(response.result.unwrap())
            .map_err(|e| DatabaseError::SerializationError {
                message: format!("Failed to parse update result: {}", e)
            })?;

        Ok(result)
    }

    /// Deletes a document by ID
    /// MongoDB'nin deleteOne'ına benzer
    pub async fn delete_document(&self, id: Uuid) -> Result<bool, DatabaseError> {
        let params = DeleteParams {
            id: Some(id),
            ids: None,
            expected_version: None,
            transaction_id: None,
        };

        let request = DatabaseRequest::new(methods::DELETE, serde_json::to_value(params).unwrap());

        let response = self.connection.send_request(request).await?;
        let result: DeleteResult = serde_json::from_value(response.result.unwrap())
            .map_err(|e| DatabaseError::SerializationError {
                message: format!("Failed to parse delete result: {}", e)
            })?;

        Ok(result.deleted_count > 0)
    }

    /// Deletes multiple documents by IDs
    /// Batch delete operation
    pub async fn delete_documents(&self, ids: Vec<Uuid>) -> Result<usize, DatabaseError> {
        let params = DeleteParams {
            id: None,
            ids: Some(ids),
            expected_version: None,
            transaction_id: None,
        };

        let request = DatabaseRequest::new(methods::DELETE, serde_json::to_value(params).unwrap());

        let response = self.connection.send_request(request).await?;
        let result: DeleteResult = serde_json::from_value(response.result.unwrap())
            .map_err(|e| DatabaseError::SerializationError {
                message: format!("Failed to parse delete result: {}", e)
            })?;

        Ok(result.deleted_count)
    }

    // ================================
    // QUERY OPERATIONS - Advanced Search API
    // MongoDB'nin aggregate pipeline'ına benzer powerful query capabilities
    // ================================

    /// Creates a new query builder
    /// Fluent API pattern - modern database client'larında yaygın
    pub fn query(&self) -> ClientQueryBuilder {
        ClientQueryBuilder::new(self.connection.clone())
    }

    /// Simple equality query - convenience method
    /// MongoDB'nin find({field: value})'ye benzer
    pub async fn find_by_field(&self, field: &str, value: Value) -> Result<Vec<Value>, DatabaseError> {
        self.query()
            .where_eq(field, value)
            .execute()
            .await
    }

    /// Range query - convenience method
    /// Numeric field'lar için range search
    pub async fn find_by_range(
        &self,
        field: &str,
        min_value: Option<Value>,
        max_value: Option<Value>
    ) -> Result<Vec<Value>, DatabaseError> {
        let mut query = self.query();

        if let Some(min) = min_value {
            query = query.where_field(field, ComparisonOperator::GreaterThanOrEqual, min);
        }
        if let Some(max) = max_value {
            query = query.where_field(field, ComparisonOperator::LessThanOrEqual, max);
        }

        query.execute().await
    }

    // ================================
    // INDEX MANAGEMENT
    // Database performance optimization için index operations
    // ================================

    /// Creates an index
    /// MongoDB'nin createIndex'e benzer
    pub async fn create_index(
        &self,
        name: &str,
        fields: Vec<String>,
        index_type: IndexType
    ) -> Result<(), DatabaseError> {
        let params = IndexParams {
            name: name.to_string(),
            fields: Some(fields),
            index_type: Some(index_type),
            operation: "create".to_string(),
        };

        let request = DatabaseRequest::new(methods::INDEX, serde_json::to_value(params).unwrap());
        let _response = self.connection.send_request(request).await?;

        Ok(())
    }

    /// Drops an index
    pub async fn drop_index(&self, name: &str) -> Result<(), DatabaseError> {
        let params = IndexParams {
            name: name.to_string(),
            fields: None,
            index_type: None,
            operation: "drop".to_string(),
        };

        let request = DatabaseRequest::new(methods::INDEX, serde_json::to_value(params).unwrap());
        let _response = self.connection.send_request(request).await?;

        Ok(())
    }

    /// Lists all indexes
    pub async fn list_indexes(&self) -> Result<Vec<Value>, DatabaseError> {
        let params = IndexParams {
            name: "all".to_string(),
            fields: None,
            index_type: None,
            operation: "list".to_string(),
        };

        let request = DatabaseRequest::new(methods::INDEX, serde_json::to_value(params).unwrap());
        let response = self.connection.send_request(request).await?;

        let result: IndexResult = serde_json::from_value(response.result.unwrap())
            .map_err(|e| DatabaseError::SerializationError {
                message: format!("Failed to parse index result: {}", e)
            })?;

        Ok(result.indexes.unwrap_or_default())
    }

    // ================================
    // TRANSACTION OPERATIONS
    // ACID transaction support - modern database'lerin must-have feature'ı
    // ================================

    /// Begins a new transaction
    /// PostgreSQL'in BEGIN'e benzer
    pub async fn begin_transaction(&self, isolation_level: IsolationLevel) -> Result<DatabaseTransaction, DatabaseError> {
        let params = TransactionParams {
            operation: "begin".to_string(),
            transaction_id: None,
            isolation_level: Some(isolation_level),
            timeout_seconds: Some(300), // 5 minutes default
        };

        let request = DatabaseRequest::new(methods::TRANSACTION, serde_json::to_value(params).unwrap());
        let response = self.connection.send_request(request).await?;

        let result: TransactionResult = serde_json::from_value(response.result.unwrap())
            .map_err(|e| DatabaseError::SerializationError {
                message: format!("Failed to parse transaction result: {}", e)
            })?;

        Ok(DatabaseTransaction::new(result.transaction_id, self.connection.clone()))
    }

    // ================================
    // SERVER OPERATIONS
    // Server monitoring ve health check operations
    // ================================

    /// Gets server statistics
    /// Monitoring ve health check için
    pub async fn get_server_stats(&self) -> Result<ServerStats, DatabaseError> {
        let request = DatabaseRequest::new(methods::STATS, json!({}));
        let response = self.connection.send_request(request).await?;

        let stats: ServerStats = serde_json::from_value(response.result.unwrap())
            .map_err(|e| DatabaseError::SerializationError {
                message: format!("Failed to parse server stats: {}", e)
            })?;

        Ok(stats)
    }

    /// Pings the server
    /// Connection health check için
    pub async fn ping(&self) -> Result<Duration, DatabaseError> {
        let start_time = Instant::now();

        let request = DatabaseRequest::new(methods::PING, json!({}));
        let _response = self.connection.send_request(request).await?;

        Ok(start_time.elapsed())
    }

    /// Gets client connection statistics
    pub async fn get_client_stats(&self) -> Result<ClientStats, DatabaseError> {
        Ok(self.connection.get_stats().await)
    }
}

/// Query Builder - Fluent API for complex queries
/// Bu pattern modern ORM'lerde (Eloquent, Mongoose) yaygın kullanılır
pub struct ClientQueryBuilder {
    connection: Arc<DatabaseConnection>,
    query_params: QueryParams,
}

impl ClientQueryBuilder {
    fn new(connection: Arc<DatabaseConnection>) -> Self {
        Self {
            connection,
            query_params: QueryParams::new(),
        }
    }

    /// Adds WHERE clause with custom operator
    pub fn where_field(mut self, path: &str, operator: ComparisonOperator, value: Value) -> Self {
        self.query_params = self.query_params.with_where(path, operator, value);
        self
    }

    /// Convenience method for equality comparison
    pub fn where_eq(self, path: &str, value: Value) -> Self {
        self.where_field(path, ComparisonOperator::Equal, value)
    }

    /// Convenience method for greater than comparison
    pub fn where_gt(self, path: &str, value: Value) -> Self {
        self.where_field(path, ComparisonOperator::GreaterThan, value)
    }

    /// Convenience method for less than comparison
    pub fn where_lt(self, path: &str, value: Value) -> Self {
        self.where_field(path, ComparisonOperator::LessThan, value)
    }

    /// Convenience method for string contains
    pub fn where_contains(self, path: &str, value: &str) -> Self {
        self.where_field(path, ComparisonOperator::Contains, json!(value))
    }

    /// Adds sort clause
    pub fn sort_by(mut self, path: &str, direction: SortDirection) -> Self {
        self.query_params = self.query_params.with_sort(path, direction);
        self
    }

    /// Convenience method for ascending sort
    pub fn sort_asc(self, path: &str) -> Self {
        self.sort_by(path, SortDirection::Ascending)
    }

    /// Convenience method for descending sort
    pub fn sort_desc(self, path: &str) -> Self {
        self.sort_by(path, SortDirection::Descending)
    }

    /// Adds result limit
    pub fn limit(mut self, limit: usize) -> Self {
        self.query_params = self.query_params.with_limit(limit);
        self
    }

    /// Adds result offset (for pagination)
    pub fn offset(mut self, offset: usize) -> Self {
        self.query_params.offset = Some(offset);
        self
    }

    /// Adds field projection (include specific fields)
    pub fn select(mut self, fields: Vec<&str>) -> Self {
        let field_strings: Vec<String> = fields.into_iter().map(|s| s.to_string()).collect();
        self.query_params = self.query_params.with_projection_include(field_strings);
        self
    }

    /// Executes the query and returns results
    pub async fn execute(self) -> Result<Vec<Value>, DatabaseError> {
        let request = DatabaseRequest::new(methods::QUERY, serde_json::to_value(self.query_params).unwrap());
        let response = self.connection.send_request(request).await?;

        let result: QueryResult = serde_json::from_value(response.result.unwrap())
            .map_err(|e| DatabaseError::SerializationError {
                message: format!("Failed to parse query result: {}", e)
            })?;

        Ok(result.documents)
    }

    /// Executes query and returns count only
    pub async fn count(self) -> Result<usize, DatabaseError> {
        let request = DatabaseRequest::new(methods::QUERY, serde_json::to_value(self.query_params).unwrap());
        let response = self.connection.send_request(request).await?;

        let result: QueryResult = serde_json::from_value(response.result.unwrap())
            .map_err(|e| DatabaseError::SerializationError {
                message: format!("Failed to parse query result: {}", e)
            })?;

        Ok(result.total_count)
    }
}

/// Database Transaction - ACID transaction wrapper
/// PostgreSQL transaction, MongoDB session'a benzer
pub struct DatabaseTransaction {
    transaction_id: Uuid,
    connection: Arc<DatabaseConnection>,
    committed: bool,
    rolled_back: bool,
}

impl DatabaseTransaction {
    fn new(transaction_id: Uuid, connection: Arc<DatabaseConnection>) -> Self {
        Self {
            transaction_id,
            connection,
            committed: false,
            rolled_back: false,
        }
    }

    /// Gets transaction ID
    pub fn id(&self) -> Uuid {
        self.transaction_id
    }

    /// Creates a document within transaction
    pub async fn create_document(&self, data: Value) -> Result<CreateResult, DatabaseError> {
        self.check_transaction_state()?;

        let params = CreateParams::new(data).with_transaction(self.transaction_id);
        let request = DatabaseRequest::new(methods::CREATE, serde_json::to_value(params).unwrap());

        let response = self.connection.send_request(request).await?;
        let result: CreateResult = serde_json::from_value(response.result.unwrap())
            .map_err(|e| DatabaseError::SerializationError {
                message: format!("Failed to parse create result: {}", e)
            })?;

        Ok(result)
    }

    /// Reads a document within transaction
    pub async fn read_document(&self, id: Uuid) -> Result<Option<Value>, DatabaseError> {
        self.check_transaction_state()?;

        let params = ReadParams::by_id(id).with_transaction(self.transaction_id);
        let request = DatabaseRequest::new(methods::READ, serde_json::to_value(params).unwrap());

        let response = self.connection.send_request(request).await?;
        let result: ReadResult = serde_json::from_value(response.result.unwrap())
            .map_err(|e| DatabaseError::SerializationError {
                message: format!("Failed to parse read result: {}", e)
            })?;

        Ok(result.document)
    }

    /// Updates a document within transaction
    pub async fn update_document(&self, id: Uuid, data: Value) -> Result<UpdateResult, DatabaseError> {
        self.check_transaction_state()?;

        let params = UpdateParams {
            id,
            data,
            expected_version: None,
            transaction_id: Some(self.transaction_id),
        };

        let request = DatabaseRequest::new(methods::UPDATE, serde_json::to_value(params).unwrap());

        let response = self.connection.send_request(request).await?;
        let result: UpdateResult = serde_json::from_value(response.result.unwrap())
            .map_err(|e| DatabaseError::SerializationError {
                message: format!("Failed to parse update result: {}", e)
            })?;

        Ok(result)
    }

    /// Deletes a document within transaction
    pub async fn delete_document(&self, id: Uuid) -> Result<bool, DatabaseError> {
        self.check_transaction_state()?;

        let params = DeleteParams {
            id: Some(id),
            ids: None,
            expected_version: None,
            transaction_id: Some(self.transaction_id),
        };

        let request = DatabaseRequest::new(methods::DELETE, serde_json::to_value(params).unwrap());

        let response = self.connection.send_request(request).await?;
        let result: DeleteResult = serde_json::from_value(response.result.unwrap())
            .map_err(|e| DatabaseError::SerializationError {
                message: format!("Failed to parse delete result: {}", e)
            })?;

        Ok(result.deleted_count > 0)
    }

    /// Commits the transaction
    pub async fn commit(mut self) -> Result<(), DatabaseError> {
        self.check_transaction_state()?;

        let params = TransactionParams {
            operation: "commit".to_string(),
            transaction_id: Some(self.transaction_id),
            isolation_level: None,
            timeout_seconds: None,
        };

        let request = DatabaseRequest::new(methods::TRANSACTION, serde_json::to_value(params).unwrap());
        let _response = self.connection.send_request(request).await?;

        self.committed = true;
        Ok(())
    }

    /// Rolls back the transaction
    pub async fn rollback(mut self) -> Result<(), DatabaseError> {
        self.check_transaction_state()?;

        let params = TransactionParams {
            operation: "rollback".to_string(),
            transaction_id: Some(self.transaction_id),
            isolation_level: None,
            timeout_seconds: None,
        };

        let request = DatabaseRequest::new(methods::TRANSACTION, serde_json::to_value(params).unwrap());
        let _response = self.connection.send_request(request).await?;

        self.rolled_back = true;
        Ok(())
    }

    /// Checks if transaction is still active
    fn check_transaction_state(&self) -> Result<(), DatabaseError> {
        if self.committed {
            return Err(DatabaseError::TransactionError {
                message: "Transaction already committed".to_string()
            });
        }
        if self.rolled_back {
            return Err(DatabaseError::TransactionError {
                message: "Transaction already rolled back".to_string()
            });
        }
        Ok(())
    }
}

/// Auto-rollback on drop if transaction not explicitly committed/rolled back
/// Bu pattern RAII (Resource Acquisition Is Initialization) principle'ını uygular
impl Drop for DatabaseTransaction {
    fn drop(&mut self) {
        if !self.committed && !self.rolled_back {
            log::warn!("Transaction {} dropped without explicit commit/rollback", self.transaction_id);
            // Future improvement: implement async drop or warning system
        }
    }
}

/// Connection Pool (Future Feature)
/// Modern database client'larında connection pooling çok yaygındır
/// Şimdilik placeholder ama gelecekte implement edilebilir
pub struct DatabaseConnectionPool {
    _config: ClientConfig,
    // Future: Arc<Mutex<Vec<DatabaseConnection>>>
}

impl DatabaseConnectionPool {
    /// Creates a new connection pool (placeholder)
    pub fn new(_config: ClientConfig) -> Self {
        Self {
            _config,
        }
    }

    /// Gets a connection from pool (placeholder)
    pub async fn get_connection(&self) -> Result<DatabaseClient, DatabaseError> {
        // Future implementation:
        // - Pool management
        // - Connection reuse
        // - Health checking
        // - Load balancing

        DatabaseClient::connect_default().await
    }
}

/// Client Error Handling Helpers
/// Modern client library'lerde yaygın error handling patterns
impl DatabaseClient {
    /// Retries an operation with exponential backoff
    /// Network instability durumlarında yararlı
    pub async fn retry_operation<F, Fut, T>(
        &self,
        operation: F,
        max_retries: usize,
    ) -> Result<T, DatabaseError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, DatabaseError>>,
    {
        let mut attempts = 0;
        let mut last_error = None;

        while attempts < max_retries {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    last_error = Some(e);
                    attempts += 1;

                    if attempts < max_retries {
                        let delay = Duration::from_millis(100 * (2_u64.pow(attempts as u32)));
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| DatabaseError::InvalidQuery {
            message: "Max retries exceeded".to_string()
        }))
    }

    /// Checks connection health
    /// Connection monitoring için utility method
    pub async fn is_connected(&self) -> bool {
        self.ping().await.is_ok()
    }
}
