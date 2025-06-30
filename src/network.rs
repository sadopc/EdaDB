// network.rs - TCP Database Server Implementation
// Bu modül modern database sistemlerinin network layer'ını implement eder
// PostgreSQL, MongoDB, Redis gibi database'lerin TCP server architecture'ından ilham alır

use crate::protocol::{
    DatabaseRequest, DatabaseResponse, ProtocolError, CreateParams, ReadParams,
    UpdateParams, DeleteParams, QueryParams, IndexParams, TransactionParams,
    CreateResult, ReadResult, UpdateResult, DeleteResult, QueryResult,
    IndexResult, TransactionResult, ConnectionStats, ServerStats,
    error_codes, methods, framing
};
use crate::{
    MemoryStorage, PersistentMemoryStorage, TransactionalStorage,
    CrudDatabase, QueryableDatabase, DatabaseError,
    ComparisonOperator, SortDirection, IndexType, IsolationLevel
};

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, atomic::{AtomicU64, AtomicUsize, Ordering}};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, Mutex, mpsc};
use tokio::time::timeout;
use uuid::Uuid;

/// Database Server Configuration
/// Production deployment için fine-tuning parameters
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Server bind address (e.g., "127.0.0.1:5432")
    pub bind_address: String,

    /// Maximum concurrent connections
    /// Bu limit memory usage ve server stability için kritik
    pub max_connections: usize,

    /// Connection timeout (idle connections için)
    /// Resource cleanup için gerekli
    pub connection_timeout: Duration,

    /// Request processing timeout
    /// DoS protection ve server responsiveness için
    pub request_timeout: Duration,

    /// Maximum request size (bytes)
    /// Memory DoS prevention için
    pub max_request_size: usize,

    /// Connection pool cleanup interval
    /// Dead connection'ları temizlemek için
    pub cleanup_interval: Duration,

    /// Enable detailed logging
    pub verbose_logging: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1:5432".to_string(), // PostgreSQL default port tribute
            max_connections: 1000,                       // Reasonable default for memory database
            connection_timeout: Duration::from_secs(300), // 5 minutes idle timeout
            request_timeout: Duration::from_secs(30),     // 30 seconds per request
            max_request_size: 16 * 1024 * 1024,          // 16MB max request size
            cleanup_interval: Duration::from_secs(60),    // 1 minute cleanup cycle
            verbose_logging: false,                       // Production'da false olmalı
        }
    }
}

/// Client Connection Context
/// Her TCP connection için state management
/// Bu pattern modern database'lerde session management için kullanılır
#[derive(Debug)]
pub struct ClientConnection {
    /// Unique connection identifier
    pub id: String,

    /// Client socket address
    pub address: SocketAddr,

    /// Connection establishment time
    pub connected_at: DateTime<Utc>,

    /// Last activity timestamp - idle timeout için
    pub last_activity: Arc<Mutex<DateTime<Utc>>>,

    /// Connection statistics
    pub stats: Arc<Mutex<ConnectionStats>>,

    /// Optional authentication context (future feature)
    pub auth_context: Option<String>,

    /// Active transactions for this connection
    /// Her connection'ın kendi transaction'ları olabilir
    pub active_transactions: Arc<RwLock<HashMap<Uuid, DateTime<Utc>>>>,
}

impl ClientConnection {
    pub fn new(id: String, address: SocketAddr) -> Self {
        let now = Utc::now();
        let stats = ConnectionStats {
            connection_id: id.clone(),
            client_address: address.to_string(),
            connected_at: now,
            requests_processed: 0,
            bytes_sent: 0,
            bytes_received: 0,
            last_activity: now,
        };

        Self {
            id: id.clone(),
            address,
            connected_at: now,
            last_activity: Arc::new(Mutex::new(now)),
            stats: Arc::new(Mutex::new(stats)),
            auth_context: None,
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Updates last activity timestamp
    /// Connection timeout management için kullanılır
    pub async fn update_activity(&self) {
        let now = Utc::now();
        *self.last_activity.lock().await = now;

        let mut stats = self.stats.lock().await;
        stats.last_activity = now;
    }

    /// Updates request statistics
    pub async fn update_stats(&self, bytes_received: u64, bytes_sent: u64) {
        let mut stats = self.stats.lock().await;
        stats.requests_processed += 1;
        stats.bytes_received += bytes_received;
        stats.bytes_sent += bytes_sent;
    }

    /// Checks if connection is idle beyond timeout
    pub async fn is_idle(&self, timeout: Duration) -> bool {
        let last_activity = *self.last_activity.lock().await;
        Utc::now().signed_duration_since(last_activity).to_std().unwrap_or(Duration::ZERO) > timeout
    }
}

/// Connection Pool Manager
/// Active connection'ları track eder ve resource management yapar
/// Bu pattern enterprise database'lerde connection lifecycle management için kritik
#[derive(Debug)]
pub struct ConnectionPool {
    /// Active connections map
    connections: Arc<RwLock<HashMap<String, Arc<ClientConnection>>>>,

    /// Connection count atomics - performance metrics için
    total_connections: Arc<AtomicUsize>,
    peak_connections: Arc<AtomicUsize>,

    /// Server configuration
    config: ServerConfig,

    /// Cleanup task handle
    cleanup_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl ConnectionPool {
    pub fn new(config: ServerConfig) -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            total_connections: Arc::new(AtomicUsize::new(0)),
            peak_connections: Arc::new(AtomicUsize::new(0)),
            config,
            cleanup_handle: Arc::new(Mutex::new(None)),
        }
    }

    /// Adds a new connection to the pool
    /// Max connection limit check ile DoS protection
    pub async fn add_connection(&self, connection: Arc<ClientConnection>) -> Result<(), String> {
        let current_count = self.total_connections.load(Ordering::Relaxed);

        // Max connection limit check
        if current_count >= self.config.max_connections {
            return Err(format!("Connection limit reached: {}", self.config.max_connections));
        }

        // Add to pool
        {
            let mut connections = self.connections.write().await;
            connections.insert(connection.id.clone(), connection.clone());
        }

        // Update metrics
        let new_count = self.total_connections.fetch_add(1, Ordering::Relaxed) + 1;

        // Update peak if necessary
        let mut peak = self.peak_connections.load(Ordering::Relaxed);
        while new_count > peak {
            match self.peak_connections.compare_exchange_weak(
                peak,
                new_count,
                Ordering::Relaxed,
                Ordering::Relaxed
            ) {
                Ok(_) => break,
                Err(current) => peak = current,
            }
        }

        if self.config.verbose_logging {
            log::info!("Connection added: {} (total: {})",
                      connection.id, new_count);
        }

        Ok(())
    }

    /// Removes a connection from the pool
    pub async fn remove_connection(&self, connection_id: &str) {
        {
            let mut connections = self.connections.write().await;
            connections.remove(connection_id);
        }

        // Safely decrement connection count (fetch_sub returns previous value)
        let prev_count = self.total_connections.fetch_sub(1, Ordering::Relaxed);
        let new_count = prev_count.saturating_sub(1);

        if self.config.verbose_logging {
            log::info!("Connection removed: {} (total: {})",
                      connection_id, new_count);
        }
    }

    /// Gets connection by ID
    pub async fn get_connection(&self, connection_id: &str) -> Option<Arc<ClientConnection>> {
        let connections = self.connections.read().await;
        connections.get(connection_id).cloned()
    }

    /// Returns current connection count
    pub fn connection_count(&self) -> usize {
        self.total_connections.load(Ordering::Relaxed)
    }

    /// Returns peak connection count
    pub fn peak_connection_count(&self) -> usize {
        self.peak_connections.load(Ordering::Relaxed)
    }

    /// Starts background cleanup task
    /// Idle connection'ları periyodik olarak temizler
    pub fn start_cleanup_task(self: &Arc<Self>) {
        let connections = Arc::clone(&self.connections);
        let config = self.config.clone();
        let total_counter = Arc::clone(&self.total_connections);
        let cleanup_handle = Arc::clone(&self.cleanup_handle);

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.cleanup_interval);

            loop {
                interval.tick().await;

                // Find idle connections
                let mut idle_connections = Vec::new();
                {
                    let connections_read = connections.read().await;
                    for (id, conn) in connections_read.iter() {
                        if conn.is_idle(config.connection_timeout).await {
                            idle_connections.push(id.clone());
                        }
                    }
                }

                // Remove idle connections
                if !idle_connections.is_empty() {
                    let mut connections_write = connections.write().await;
                    for conn_id in idle_connections {
                        connections_write.remove(&conn_id);
                        
                        // Safely decrement count (prevent underflow)
                        let current = total_counter.load(Ordering::Relaxed);
                        if current > 0 {
                            total_counter.fetch_sub(1, Ordering::Relaxed);
                        }

                        if config.verbose_logging {
                            log::info!("Removed idle connection: {}", conn_id);
                        }
                    }
                }
            }
        });

        // Store cleanup handle for graceful shutdown
        tokio::spawn(async move {
            let mut cleanup_handle_guard = cleanup_handle.lock().await;
            *cleanup_handle_guard = Some(handle);
        });
    }

    /// Stops cleanup task and clears all connections
    pub async fn shutdown(&self) {
        // Stop cleanup task
        if let Some(handle) = self.cleanup_handle.lock().await.take() {
            handle.abort();
        }

        // Clear all connections
        {
            let mut connections = self.connections.write().await;
            connections.clear();
        }

        self.total_connections.store(0, Ordering::Relaxed);
        log::info!("Connection pool shutdown completed");
    }
}

/// Database Server - Ana TCP server implementation
/// Bu class modern database server'ların network katmanını implement eder
pub struct DatabaseServer {
    /// Server configuration
    config: ServerConfig,

    /// Connection pool manager
    connection_pool: Arc<ConnectionPool>,

    /// Database storage (persistent)
    storage: Arc<PersistentMemoryStorage>,

    /// Transactional storage wrapper
    transactional_storage: Arc<TransactionalStorage>,

    /// Server statistics
    server_stats: Arc<RwLock<ServerStats>>,

    /// Request counter for performance metrics
    total_requests: Arc<AtomicU64>,

    /// Average response time tracking
    total_response_time: Arc<AtomicU64>,

    /// Server start time
    start_time: DateTime<Utc>,

    /// Shutdown signal channel
    shutdown_tx: Arc<Mutex<Option<mpsc::Sender<()>>>>,
}

impl DatabaseServer {
    /// Creates a new database server instance
    /// Storage initialization ve configuration validation ile başlar
    pub async fn new(
        config: ServerConfig,
        storage: Arc<PersistentMemoryStorage>
    ) -> Result<Self, DatabaseError> {

        log::info!("Initializing database server on {}", config.bind_address);

        // Transactional wrapper oluştur
        let base_storage = storage.storage().clone();
        let transactional_storage = Arc::new(TransactionalStorage::new(base_storage));

        // Connection pool oluştur
        let connection_pool = Arc::new(ConnectionPool::new(config.clone()));

        // Server statistics initialize et
        let start_time = Utc::now();
        let server_stats = ServerStats {
            started_at: start_time,
            active_connections: 0,
            total_requests: 0,
            avg_response_time_ms: 0.0,
            memory_usage_bytes: 0,
            database_stats: json!({}),
        };

        let (shutdown_tx, _) = mpsc::channel(1);

        Ok(Self {
            config,
            connection_pool,
            storage,
            transactional_storage,
            server_stats: Arc::new(RwLock::new(server_stats)),
            total_requests: Arc::new(AtomicU64::new(0)),
            total_response_time: Arc::new(AtomicU64::new(0)),
            start_time,
            shutdown_tx: Arc::new(Mutex::new(Some(shutdown_tx))),
        })
    }

    /// Starts the TCP server and begins listening for connections
    /// Ana server loop - modern async/await pattern kullanır
    pub async fn start(&self) -> Result<(), DatabaseError> {
        log::info!("Starting database server...");

        // TCP listener oluştur
        let listener = TcpListener::bind(&self.config.bind_address).await
            .map_err(|e| DatabaseError::InvalidQuery {
                message: format!("Failed to bind to {}: {}", self.config.bind_address, e)
            })?;

        log::info!("Database server listening on {}", self.config.bind_address);

        // Connection pool cleanup task başlat
        self.connection_pool.start_cleanup_task();

        // Shutdown signal receiver
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
        *self.shutdown_tx.lock().await = Some(shutdown_tx);

        // Main server loop - accept connections ve handle requests
        loop {
            tokio::select! {
                // New connection acceptance
                result = listener.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            log::info!("New connection from: {}", addr);

                            // Connection context oluştur
                            let connection_id = Uuid::new_v4().to_string();
                            let connection = Arc::new(ClientConnection::new(connection_id, addr));

                            // Connection pool'a ekle
                            match self.connection_pool.add_connection(connection.clone()).await {
                                Ok(_) => {
                                    // Spawn connection handler
                                    let server_ref = self.clone_refs();
                                    tokio::spawn(async move {
                                        if let Err(e) = server_ref.handle_connection(stream, connection).await {
                                            log::error!("Connection handling error: {:?}", e);
                                        }
                                    });
                                },
                                Err(e) => {
                                    log::warn!("Connection rejected: {}", e);
                                    // Gracefully close connection
                                    drop(stream);
                                }
                            }
                        },
                        Err(e) => {
                            log::error!("Failed to accept connection: {}", e);
                        }
                    }
                },

                // Shutdown signal
                _ = shutdown_rx.recv() => {
                    log::info!("Shutdown signal received");
                    break;
                }
            }
        }

        log::info!("Database server stopped");
        Ok(())
    }

    /// Helper method to clone necessary references for connection handling
    /// Rust ownership model gereği, Arc references'ları clone etmemiz gerekir
    fn clone_refs(&self) -> DatabaseServerHandler {
        DatabaseServerHandler {
            config: self.config.clone(),
            connection_pool: Arc::clone(&self.connection_pool),
            storage: Arc::clone(&self.storage),
            transactional_storage: Arc::clone(&self.transactional_storage),
            server_stats: Arc::clone(&self.server_stats),
            total_requests: Arc::clone(&self.total_requests),
            total_response_time: Arc::clone(&self.total_response_time),
        }
    }

    /// Graceful server shutdown
    pub async fn shutdown(&self) -> Result<(), DatabaseError> {
        log::info!("Initiating graceful shutdown...");

        // Send shutdown signal
        if let Some(tx) = self.shutdown_tx.lock().await.take() {
            let _ = tx.send(()).await;
        }

        // Shutdown connection pool
        self.connection_pool.shutdown().await;

        // Shutdown storage systems
        self.storage.shutdown().await?;

        log::info!("Graceful shutdown completed");
        Ok(())
    }

    /// Returns current server statistics
    pub async fn get_stats(&self) -> ServerStats {
        let mut stats = self.server_stats.read().await.clone();

        // Update dynamic statistics
        stats.active_connections = self.connection_pool.connection_count();
        stats.total_requests = self.total_requests.load(Ordering::Relaxed);

        // Calculate average response time
        let total_time = self.total_response_time.load(Ordering::Relaxed);
        if stats.total_requests > 0 {
            stats.avg_response_time_ms = total_time as f64 / stats.total_requests as f64;
        }

        // Get database statistics
        if let Ok(db_stats) = self.storage.stats().await {
            stats.database_stats = serde_json::to_value(db_stats).unwrap_or(json!({}));
        }

        stats
    }
}

/// Connection Handler - her connection için ayrı handler
/// Bu pattern scalability ve isolation için kritik
#[derive(Clone)]
struct DatabaseServerHandler {
    config: ServerConfig,
    connection_pool: Arc<ConnectionPool>,
    storage: Arc<PersistentMemoryStorage>,
    transactional_storage: Arc<TransactionalStorage>,
    server_stats: Arc<RwLock<ServerStats>>,
    total_requests: Arc<AtomicU64>,
    total_response_time: Arc<AtomicU64>,
}

impl DatabaseServerHandler {
    /// Handles a single TCP connection
    /// Message framing, request processing ve error handling içerir
    async fn handle_connection(
        &self,
        mut stream: TcpStream,
        connection: Arc<ClientConnection>,
    ) -> Result<(), DatabaseError> {

        log::info!("Handling connection: {}", connection.id);

        // Connection loop - her request için bir iteration
        loop {
            // Request timeout ile DoS protection
            let request_result = timeout(
                self.config.request_timeout,
                self.read_request(&mut stream)
            ).await;

            match request_result {
                Ok(Ok(Some(request))) => {
                    // Valid request received
                    connection.update_activity().await;

                    let request_size = serde_json::to_vec(&request)
                        .map(|v| v.len())
                        .unwrap_or(0);

                    // Request size validation
                    if request_size > self.config.max_request_size {
                        let error_response = DatabaseResponse::error(
                            &request.id,
                            ProtocolError {
                                code: error_codes::INVALID_REQUEST,
                                message: format!("Request too large: {} bytes", request_size),
                                data: None,
                            }
                        );

                        self.send_response(&mut stream, &error_response).await?;
                        continue;
                    }

                    // Process request with timing
                    let start_time = Instant::now();
                    let response = self.process_request(request, &connection).await;
                    let processing_time = start_time.elapsed();

                    // Add timing metadata
                    let response_with_timing = response.with_timing(processing_time.as_millis() as u64);

                    // Send response
                    self.send_response(&mut stream, &response_with_timing).await?;

                    // Update statistics
                    let response_size = serde_json::to_vec(&response_with_timing)
                        .map(|v| v.len())
                        .unwrap_or(0);

                    connection.update_stats(request_size as u64, response_size as u64).await;

                    // Update server metrics
                    self.total_requests.fetch_add(1, Ordering::Relaxed);
                    self.total_response_time.fetch_add(processing_time.as_millis() as u64, Ordering::Relaxed);

                    if self.config.verbose_logging {
                        log::debug!("Request processed in {:?}: {}",
                                   processing_time, response_with_timing.id);
                    }
                },

                Ok(Ok(None)) => {
                    // Connection closed by client
                    log::info!("Client closed connection: {}", connection.id);
                    break;
                },

                Ok(Err(e)) => {
                    // Request parsing error
                    log::error!("Request parsing error: {:?}", e);

                    // Send generic error response
                    let error_response = DatabaseResponse::error(
                        "unknown",
                        ProtocolError {
                            code: error_codes::INVALID_REQUEST,
                            message: format!("Request parsing failed: {}", e),
                            data: None,
                        }
                    );

                    if self.send_response(&mut stream, &error_response).await.is_err() {
                        break; // Connection broken
                    }
                },

                Err(_) => {
                    // Timeout
                    log::warn!("Request timeout for connection: {}", connection.id);

                    let timeout_response = DatabaseResponse::error(
                        "timeout",
                        ProtocolError {
                            code: error_codes::INTERNAL_ERROR,
                            message: "Request timeout".to_string(),
                            data: None,
                        }
                    );

                    if self.send_response(&mut stream, &timeout_response).await.is_err() {
                        break; // Connection broken
                    }
                }
            }
        }

        // Cleanup connection
        self.connection_pool.remove_connection(&connection.id).await;

        // Cleanup active transactions for this connection
        let active_txs = connection.active_transactions.read().await;
        for tx_id in active_txs.keys() {
            if let Err(e) = self.transactional_storage.rollback_transaction(tx_id).await {
                log::warn!("Failed to rollback transaction {} on connection close: {:?}", tx_id, e);
            }
        }

        log::info!("Connection handler finished: {}", connection.id);
        Ok(())
    }

    /// Reads a request from TCP stream using framing protocol
    async fn read_request(&self, stream: &mut TcpStream) -> Result<Option<DatabaseRequest>, DatabaseError> {
        // Try to read framed message
        match framing::read_message(stream).await {
            Ok(message_bytes) => {
                // Parse JSON request
                let request: DatabaseRequest = serde_json::from_slice(&message_bytes)
                    .map_err(|e| DatabaseError::SerializationError {
                        message: format!("Invalid request JSON: {}", e)
                    })?;

                // Validate request
                request.validate().map_err(|e| DatabaseError::InvalidQuery {
                    message: format!("Request validation failed: {}", e.message)
                })?;

                Ok(Some(request))
            },
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Connection closed
                Ok(None)
            },
            Err(e) => {
                Err(DatabaseError::InvalidQuery {
                    message: format!("Failed to read request: {}", e)
                })
            }
        }
    }

    /// Sends a response to TCP stream using framing protocol
    async fn send_response(&self, stream: &mut TcpStream, response: &DatabaseResponse) -> Result<(), DatabaseError> {
        let response_bytes = serde_json::to_vec(response)
            .map_err(|e| DatabaseError::SerializationError {
                message: format!("Failed to serialize response: {}", e)
            })?;

        framing::write_message(stream, &response_bytes).await
            .map_err(|e| DatabaseError::InvalidQuery {
                message: format!("Failed to send response: {}", e)
            })?;

        Ok(())
    }

    /// Processes a database request and returns response
    /// Bu method'da tüm database operations route edilir
    async fn process_request(
        &self,
        request: DatabaseRequest,
        connection: &Arc<ClientConnection>
    ) -> DatabaseResponse {

        if self.config.verbose_logging {
            log::debug!("Processing request: {} method: {}", request.id, request.method);
        }

        // Method dispatch - modern RPC pattern
        let result = match request.method.as_str() {
            methods::CREATE => self.handle_create_request(&request).await,
            methods::READ => self.handle_read_request(&request).await,
            methods::UPDATE => self.handle_update_request(&request).await,
            methods::DELETE => self.handle_delete_request(&request).await,
            methods::QUERY => self.handle_query_request(&request).await,
            methods::INDEX => self.handle_index_request(&request).await,
            methods::TRANSACTION => self.handle_transaction_request(&request, connection).await,
            methods::STATS => self.handle_stats_request(&request).await,
            methods::PING => self.handle_ping_request(&request).await,
            _ => Err(ProtocolError {
                code: error_codes::INVALID_METHOD,
                message: format!("Unknown method: {}", request.method),
                data: None,
            })
        };

        // Convert result to response
        match result {
            Ok(result_value) => DatabaseResponse::success(&request.id, result_value),
            Err(protocol_error) => DatabaseResponse::error(&request.id, protocol_error),
        }
    }

    // ================================
    // REQUEST HANDLERS - Her database operation için ayrı handler
    // Bu pattern modern RPC framework'lerinde yaygın kullanılır
    // ================================

    /// Handles CREATE requests - new document insertion
    async fn handle_create_request(&self, request: &DatabaseRequest) -> Result<Value, ProtocolError> {
        let params: CreateParams = serde_json::from_value(request.params.clone())
            .map_err(|e| ProtocolError {
                code: error_codes::INVALID_PARAMS,
                message: format!("Invalid create parameters: {}", e),
                data: None,
            })?;

        // Transactional vs non-transactional operation
        let document = if let Some(tx_id) = params.transaction_id {
            // Transactional create
            self.transactional_storage.transactional_create(&tx_id, params.data).await
                .map_err(ProtocolError::from)?
        } else {
            // Direct create
            if let Some(doc_id) = params.id {
                self.storage.create_with_id(doc_id, params.data).await
                    .map_err(ProtocolError::from)?
            } else {
                self.storage.create(params.data).await
                    .map_err(ProtocolError::from)?
            }
        };

        // Build result
        let result = CreateResult {
            id: document.metadata.id,
            version: document.metadata.version,
            created_at: document.metadata.created_at,
            document: Some(document.data),
        };

        Ok(serde_json::to_value(result).unwrap())
    }

    /// Handles READ requests - document retrieval
    async fn handle_read_request(&self, request: &DatabaseRequest) -> Result<Value, ProtocolError> {
        let params: ReadParams = serde_json::from_value(request.params.clone())
            .map_err(|e| ProtocolError {
                code: error_codes::INVALID_PARAMS,
                message: format!("Invalid read parameters: {}", e),
                data: None,
            })?;

        // Route based on read type
        if let Some(tx_id) = params.transaction_id {
            // Transactional read
            if let Some(doc_id) = params.id {
                let doc_data = self.transactional_storage.transactional_read(&tx_id, &doc_id).await
                    .map_err(ProtocolError::from)?;

                let result = ReadResult {
                    document: doc_data,
                    documents: None,
                    total_count: None,
                };

                Ok(serde_json::to_value(result).unwrap())
            } else {
                Err(ProtocolError {
                    code: error_codes::INVALID_PARAMS,
                    message: "Transactional reads require document ID".to_string(),
                    data: None,
                })
            }
        } else if let Some(doc_id) = params.id {
            // Single document read
            let document = self.storage.read_by_id(&doc_id).await
                .map_err(ProtocolError::from)?;

            let result = ReadResult {
                document: document.map(|d| d.data),
                documents: None,
                total_count: None,
            };

            Ok(serde_json::to_value(result).unwrap())
        } else if let Some(doc_ids) = params.ids {
            // Batch read
            let documents = self.storage.read_by_ids(&doc_ids).await
                .map_err(ProtocolError::from)?;

            let doc_data: Vec<Value> = documents.into_iter().map(|d| d.data).collect();

            let result = ReadResult {
                document: None,
                documents: Some(doc_data.clone()),
                total_count: Some(doc_data.len()),
            };

            Ok(serde_json::to_value(result).unwrap())
        } else {
            // List all documents
            let documents = self.storage.read_all(params.offset, params.limit).await
                .map_err(ProtocolError::from)?;

            let doc_data: Vec<Value> = documents.into_iter().map(|d| d.data).collect();

            let result = ReadResult {
                document: None,
                documents: Some(doc_data.clone()),
                total_count: Some(doc_data.len()),
            };

            Ok(serde_json::to_value(result).unwrap())
        }
    }

    /// Handles UPDATE requests - document modification
    async fn handle_update_request(&self, request: &DatabaseRequest) -> Result<Value, ProtocolError> {
        let params: UpdateParams = serde_json::from_value(request.params.clone())
            .map_err(|e| ProtocolError {
                code: error_codes::INVALID_PARAMS,
                message: format!("Invalid update parameters: {}", e),
                data: None,
            })?;

        // Transactional vs non-transactional operation
        let document = if let Some(tx_id) = params.transaction_id {
            // Transactional update
            self.transactional_storage.transactional_update(&tx_id, &params.id, params.data).await
                .map_err(ProtocolError::from)?
        } else {
            // Direct update with optional version check
            if let Some(expected_version) = params.expected_version {
                self.storage.update_with_version(&params.id, params.data, expected_version).await
                    .map_err(ProtocolError::from)?
            } else {
                self.storage.update(&params.id, params.data).await
                    .map_err(ProtocolError::from)?
            }
        };

        // Build result
        let result = UpdateResult {
            id: document.metadata.id,
            version: document.metadata.version,
            updated_at: document.metadata.updated_at,
            document: Some(document.data),
        };

        Ok(serde_json::to_value(result).unwrap())
    }

    /// Handles DELETE requests - document removal
    async fn handle_delete_request(&self, request: &DatabaseRequest) -> Result<Value, ProtocolError> {
        let params: DeleteParams = serde_json::from_value(request.params.clone())
            .map_err(|e| ProtocolError {
                code: error_codes::INVALID_PARAMS,
                message: format!("Invalid delete parameters: {}", e),
                data: None,
            })?;

        // Route based on delete type
        if let Some(tx_id) = params.transaction_id {
            // Transactional delete
            if let Some(doc_id) = params.id {
                let deleted = self.transactional_storage.transactional_delete(&tx_id, &doc_id).await
                    .map_err(ProtocolError::from)?;

                let result = DeleteResult {
                    deleted_count: if deleted { 1 } else { 0 },
                    deleted_ids: if deleted { Some(vec![doc_id]) } else { None },
                };

                Ok(serde_json::to_value(result).unwrap())
            } else {
                Err(ProtocolError {
                    code: error_codes::INVALID_PARAMS,
                    message: "Transactional deletes require document ID".to_string(),
                    data: None,
                })
            }
        } else if let Some(doc_id) = params.id {
            // Single document delete
            let deleted = if let Some(expected_version) = params.expected_version {
                self.storage.delete_with_version(&doc_id, expected_version).await
                    .map_err(ProtocolError::from)?
            } else {
                self.storage.delete(&doc_id).await
                    .map_err(ProtocolError::from)?
            };

            let result = DeleteResult {
                deleted_count: if deleted { 1 } else { 0 },
                deleted_ids: if deleted { Some(vec![doc_id]) } else { None },
            };

            Ok(serde_json::to_value(result).unwrap())
        } else if let Some(doc_ids) = params.ids {
            // Batch delete
            let deleted_count = self.storage.delete_batch(&doc_ids).await
                .map_err(ProtocolError::from)?;

            let result = DeleteResult {
                deleted_count,
                deleted_ids: Some(doc_ids),
            };

            Ok(serde_json::to_value(result).unwrap())
        } else {
            Err(ProtocolError {
                code: error_codes::INVALID_PARAMS,
                message: "Delete operation requires document ID(s)".to_string(),
                data: None,
            })
        }
    }

    /// Handles QUERY requests - advanced document search
    async fn handle_query_request(&self, request: &DatabaseRequest) -> Result<Value, ProtocolError> {
        let params: QueryParams = serde_json::from_value(request.params.clone())
            .map_err(|e| ProtocolError {
                code: error_codes::INVALID_PARAMS,
                message: format!("Invalid query parameters: {}", e),
                data: None,
            })?;

        // Build query using existing query engine
        let storage_arc = Arc::new(self.storage.storage().clone());
        let mut query_builder = storage_arc.query();

        // Add where clauses
        for where_clause in params.where_clauses {
            query_builder = query_builder.where_field(
                &where_clause.path,
                where_clause.operator,
                where_clause.value
            );
        }

        // Add sort clauses
        for sort_clause in params.sort_clauses {
            query_builder = query_builder.sort_by(&sort_clause.path, sort_clause.direction);
        }

        // Add projection
        if let Some(projection) = params.projection {
            if let Some(include_fields) = projection.include {
                let field_refs: Vec<&str> = include_fields.iter().map(|s| s.as_str()).collect();
                query_builder = query_builder.select(field_refs);
            } else if let Some(exclude_fields) = projection.exclude {
                let field_refs: Vec<&str> = exclude_fields.iter().map(|s| s.as_str()).collect();
                query_builder = query_builder.exclude(field_refs);
            }
        }

        // Add pagination
        if let Some(offset) = params.offset {
            query_builder = query_builder.offset(offset);
        }
        if let Some(limit) = params.limit {
            query_builder = query_builder.limit(limit);
        }

        // Execute query with timing
        let start_time = Instant::now();
        let documents = query_builder.execute().await
            .map_err(ProtocolError::from)?;
        let execution_time = start_time.elapsed();

        // Build result
        let result = QueryResult {
            documents: documents.clone(),
            total_count: documents.len(),
            execution_time_ms: Some(execution_time.as_millis() as u64),
            index_used: None, // Bu bilgiyi query engine'den alabilirz ama demo için None
        };

        Ok(serde_json::to_value(result).unwrap())
    }

    /// Handles INDEX requests - index management
    async fn handle_index_request(&self, request: &DatabaseRequest) -> Result<Value, ProtocolError> {
        let params: IndexParams = serde_json::from_value(request.params.clone())
            .map_err(|e| ProtocolError {
                code: error_codes::INVALID_PARAMS,
                message: format!("Invalid index parameters: {}", e),
                data: None,
            })?;

        let storage_ref = self.storage.storage();

        match params.operation.as_str() {
            "create" => {
                let fields = params.fields.ok_or_else(|| ProtocolError {
                    code: error_codes::INVALID_PARAMS,
                    message: "Index creation requires fields".to_string(),
                    data: None,
                })?;

                let index_type = params.index_type.unwrap_or(IndexType::Hash);

                let field_refs: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
                storage_ref.create_index(&params.name, field_refs, index_type)
                    .map_err(ProtocolError::from)?;

                let result = IndexResult {
                    name: params.name,
                    message: "Index created successfully".to_string(),
                    stats: None,
                    indexes: None,
                };

                Ok(serde_json::to_value(result).unwrap())
            },

            "drop" => {
                storage_ref.drop_index(&params.name)
                    .map_err(ProtocolError::from)?;

                let result = IndexResult {
                    name: params.name,
                    message: "Index dropped successfully".to_string(),
                    stats: None,
                    indexes: None,
                };

                Ok(serde_json::to_value(result).unwrap())
            },

            "list" => {
                let indexes = storage_ref.list_indexes()
                    .map_err(ProtocolError::from)?;

                let index_values: Vec<Value> = indexes.into_iter()
                    .map(|idx| serde_json::to_value(idx).unwrap())
                    .collect();

                let result = IndexResult {
                    name: "all".to_string(),
                    message: format!("Found {} indexes", index_values.len()),
                    stats: None,
                    indexes: Some(index_values),
                };

                Ok(serde_json::to_value(result).unwrap())
            },

            "stats" => {
                let stats = storage_ref.get_index_stats(&params.name)
                    .map_err(ProtocolError::from)?;

                let result = IndexResult {
                    name: params.name,
                    message: "Index statistics retrieved".to_string(),
                    stats: Some(serde_json::to_value(stats).unwrap()),
                    indexes: None,
                };

                Ok(serde_json::to_value(result).unwrap())
            },

            _ => Err(ProtocolError {
                code: error_codes::INVALID_PARAMS,
                message: format!("Unknown index operation: {}", params.operation),
                data: None,
            })
        }
    }

    /// Handles TRANSACTION requests - transaction lifecycle management
    async fn handle_transaction_request(
        &self,
        request: &DatabaseRequest,
        connection: &Arc<ClientConnection>
    ) -> Result<Value, ProtocolError> {
        let params: TransactionParams = serde_json::from_value(request.params.clone())
            .map_err(|e| ProtocolError {
                code: error_codes::INVALID_PARAMS,
                message: format!("Invalid transaction parameters: {}", e),
                data: None,
            })?;

        match params.operation.as_str() {
            "begin" => {
                let isolation_level = params.isolation_level.unwrap_or(IsolationLevel::ReadCommitted);
                let timeout = params.timeout_seconds.map(Duration::from_secs);

                let tx_id = self.transactional_storage.begin_transaction(isolation_level, timeout).await
                    .map_err(ProtocolError::from)?;

                // Track transaction in connection
                {
                    let mut active_txs = connection.active_transactions.write().await;
                    active_txs.insert(tx_id, Utc::now());
                }

                let result = TransactionResult {
                    transaction_id: tx_id,
                    message: "Transaction started".to_string(),
                    status: Some("active".to_string()),
                };

                Ok(serde_json::to_value(result).unwrap())
            },

            "commit" => {
                let tx_id = params.transaction_id.ok_or_else(|| ProtocolError {
                    code: error_codes::INVALID_PARAMS,
                    message: "Commit requires transaction ID".to_string(),
                    data: None,
                })?;

                self.transactional_storage.commit_transaction(&tx_id).await
                    .map_err(ProtocolError::from)?;

                // Remove from connection tracking
                {
                    let mut active_txs = connection.active_transactions.write().await;
                    active_txs.remove(&tx_id);
                }

                let result = TransactionResult {
                    transaction_id: tx_id,
                    message: "Transaction committed".to_string(),
                    status: Some("committed".to_string()),
                };

                Ok(serde_json::to_value(result).unwrap())
            },

            "rollback" => {
                let tx_id = params.transaction_id.ok_or_else(|| ProtocolError {
                    code: error_codes::INVALID_PARAMS,
                    message: "Rollback requires transaction ID".to_string(),
                    data: None,
                })?;

                self.transactional_storage.rollback_transaction(&tx_id).await
                    .map_err(ProtocolError::from)?;

                // Remove from connection tracking
                {
                    let mut active_txs = connection.active_transactions.write().await;
                    active_txs.remove(&tx_id);
                }

                let result = TransactionResult {
                    transaction_id: tx_id,
                    message: "Transaction rolled back".to_string(),
                    status: Some("aborted".to_string()),
                };

                Ok(serde_json::to_value(result).unwrap())
            },

            _ => Err(ProtocolError {
                code: error_codes::INVALID_PARAMS,
                message: format!("Unknown transaction operation: {}", params.operation),
                data: None,
            })
        }
    }

    /// Handles STATS requests - server statistics
    async fn handle_stats_request(&self, _request: &DatabaseRequest) -> Result<Value, ProtocolError> {
        // Get current server statistics
        let mut stats = self.server_stats.read().await.clone();

        // Update dynamic statistics
        stats.active_connections = self.connection_pool.connection_count();
        stats.total_requests = self.total_requests.load(Ordering::Relaxed);

        // Calculate average response time
        let total_time = self.total_response_time.load(Ordering::Relaxed);
        if stats.total_requests > 0 {
            stats.avg_response_time_ms = total_time as f64 / stats.total_requests as f64;
        }

        // Get database statistics
        if let Ok(db_stats) = self.storage.stats().await {
            stats.database_stats = serde_json::to_value(db_stats).unwrap_or(json!({}));
        }

        Ok(serde_json::to_value(stats).unwrap())
    }

    /// Handles PING requests - server health check
    async fn handle_ping_request(&self, _request: &DatabaseRequest) -> Result<Value, ProtocolError> {
        Ok(json!({
            "message": "pong",
            "timestamp": Utc::now(),
            "server_uptime_seconds": Utc::now().signed_duration_since(self.server_stats.read().await.started_at).num_seconds()
        }))
    }
}
