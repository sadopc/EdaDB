use crate::row::Row;
use crate::table::Table;
use crate::types::{DataType, TypedValue};
use crate::parser::{SqlStatement, SqlValue, ColumnDefinition, WhereClause, Condition, Assignment};
use crate::executor::{QueryExecutor, QueryResult};
use crate::errors::DbError;
use crate::query_planner::QueryCache;
use crate::transaction::{TransactionManager, TransactionId, IsolationLevel};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Database {
    pub tables: HashMap<String, Table>,
    pub data_directory: String,
    #[serde(skip)]
    executor: QueryExecutor,
    #[serde(skip)]
    query_cache: QueryCache,
    #[serde(skip)]
    transaction_manager: Arc<RwLock<TransactionManager>>,
    #[serde(skip)]
    current_transaction_id: Option<TransactionId>,
}

/// Veritabanı dump formatı - tek dosyada tüm veri
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseDump {
    pub version: String,
    pub timestamp: String,
    pub tables: HashMap<String, Table>,
    pub metadata: DatabaseMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseMetadata {
    pub table_count: usize,
    pub total_rows: usize,
    pub created_at: String,
    pub description: String,
}

impl Drop for Database {
    fn drop(&mut self) {
        println!("📤 Program kapanıyor, tablolar kaydediliyor...");
        if let Err(e) = self.save_tables() {
            eprintln!("⚠️  Tablolar kaydedilirken hata: {}", e);
        }
    }
}

impl Database {
    pub fn new() -> Self {
        let mut db = Self {
            tables: HashMap::new(),
            data_directory: "data".to_string(),
            executor: QueryExecutor::new(),
            query_cache: QueryCache::new(100, 300), // 100 entries, 5 minutes TTL
            transaction_manager: Arc::new(RwLock::new(TransactionManager::new())),
            current_transaction_id: None,
        };
        
        // Veri dizinini oluştur
        db.ensure_data_directory();
        
        // Mevcut tabloları yükle
        if let Err(e) = db.load_tables() {
            eprintln!("⚠️  Tablolar yüklenirken hata: {}", e);
        }
        
        db
    }

    pub fn new_with_directory(data_directory: String) -> Self {
        let mut db = Self {
            tables: HashMap::new(),
            data_directory,
            executor: QueryExecutor::new(),
            query_cache: QueryCache::new(100, 300), // 100 entries, 5 minutes TTL
            transaction_manager: Arc::new(RwLock::new(TransactionManager::new())),
            current_transaction_id: None,
        };
        
        db.ensure_data_directory();
        
        if let Err(e) = db.load_tables() {
            eprintln!("⚠️  Tablolar yüklenirken hata: {}", e);
        }
        
        db
    }

    fn ensure_data_directory(&self) {
        if !Path::new(&self.data_directory).exists() {
            if let Err(e) = fs::create_dir_all(&self.data_directory) {
                eprintln!("⚠️  Veri dizini oluşturulamadı: {}", e);
            }
        }
    }

    /// Tüm verileri tek bir .dbdump.json dosyasına export eder
    pub fn export_dump(&self, file_path: Option<&str>) -> Result<String, DbError> {
        let dump_path = file_path.unwrap_or("database.dbdump.json");
        
        // Timestamp oluştur
        let timestamp = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string();
        
        // Metadata hesapla
        let total_rows = self.tables.values().map(|t| t.get_all_rows().len()).sum();
        let metadata = DatabaseMetadata {
            table_count: self.tables.len(),
            total_rows,
            created_at: timestamp.clone(),
            description: format!("Database dump with {} tables and {} total rows", self.tables.len(), total_rows),
        };
        
        // Dump yapısı oluştur
        let dump = DatabaseDump {
            version: "1.0".to_string(),
            timestamp,
            tables: self.tables.clone(),
            metadata,
        };
        
        // JSON'a serialize et
        let json_content = serde_json::to_string_pretty(&dump)
            .map_err(|e| DbError::SerializationError(format!("Dump serialize edilemedi: {}", e)))?;
        
        // Dosyaya yaz
        fs::write(dump_path, json_content)
            .map_err(|e| DbError::FileSystemError(format!("Dump dosyası yazılamadı: {}", e)))?;
        
        Ok(dump_path.to_string())
    }
    
    /// .dbdump.json dosyasından verileri import eder
    pub fn import_dump(&mut self, file_path: &str, clear_existing: bool) -> Result<DatabaseMetadata, DbError> {
        // Dosya var mı kontrol et
        if !Path::new(file_path).exists() {
            return Err(DbError::FileSystemError(format!("Dump dosyası bulunamadı: {}", file_path)));
        }
        
        // JSON dosyasını oku
        let json_content = fs::read_to_string(file_path)
            .map_err(|e| DbError::FileSystemError(format!("Dump dosyası okunamadı: {}", e)))?;
        
        // JSON'dan deserialize et
        let dump: DatabaseDump = serde_json::from_str(&json_content)
            .map_err(|e| DbError::SerializationError(format!("Dump parse edilemedi: {}", e)))?;
        
        // Mevcut tabloları temizle (isteğe bağlı)
        if clear_existing {
            self.tables.clear();
            println!("🗑️  Mevcut tablolar temizlendi");
        }
        
        // Dump'daki tabloları yükle
        let mut imported_tables = 0;
        let mut imported_rows = 0;
        
        for (table_name, table) in dump.tables {
            let row_count = table.get_all_rows().len();
            
            if self.tables.contains_key(&table_name) && !clear_existing {
                println!("⚠️  Tablo zaten mevcut, atlanıyor: {}", table_name);
                continue;
            }
            
            self.tables.insert(table_name.clone(), table);
            imported_tables += 1;
            imported_rows += row_count;
            
            println!("📥 Tablo import edildi: {} ({} satır)", table_name, row_count);
        }
        
        // Değişiklikleri diske yaz
        if let Err(e) = self.save_tables() {
            eprintln!("⚠️  Import sonrası tablolar kaydedilirken hata: {}", e);
        }
        
        println!("✅ Import tamamlandı: {} tablo, {} satır", imported_tables, imported_rows);
        
        Ok(dump.metadata)
    }
    
    /// Veritabanı istatistiklerini döndürür
    pub fn get_stats(&self) -> HashMap<String, serde_json::Value> {
        let mut stats = HashMap::new();
        
        stats.insert("table_count".to_string(), serde_json::Value::Number(self.tables.len().into()));
        
        let total_rows: usize = self.tables.values().map(|t| t.get_all_rows().len()).sum();
        stats.insert("total_rows".to_string(), serde_json::Value::Number(total_rows.into()));
        
        let mut table_stats = HashMap::new();
        for (name, table) in &self.tables {
            let mut table_info = HashMap::new();
            table_info.insert("columns".to_string(), serde_json::Value::Number(table.get_columns().len().into()));
            table_info.insert("rows".to_string(), serde_json::Value::Number(table.get_all_rows().len().into()));
            
            let column_types: Vec<String> = table.get_columns().iter()
                .map(|c| format!("{}: {}", c.name, c.data_type.to_string()))
                .collect();
            table_info.insert("schema".to_string(), serde_json::Value::Array(
                column_types.into_iter().map(serde_json::Value::String).collect()
            ));
            
            table_stats.insert(name.clone(), serde_json::Value::Object(table_info.into_iter().collect()));
        }
        
        stats.insert("tables".to_string(), serde_json::Value::Object(table_stats.into_iter().collect()));
        
        stats
    }

    pub fn load_tables(&mut self) -> Result<(), DbError> {
        let data_path = Path::new(&self.data_directory);
        
        if !data_path.exists() {
            return Ok(()); // Veri dizini yoksa sorun yok
        }

        let entries = fs::read_dir(data_path)
            .map_err(|e| DbError::FileSystemError(format!("Veri dizini okunamadı: {}", e)))?;

        for entry in entries {
            let entry = entry.map_err(|e| DbError::FileSystemError(format!("Dosya listesi okunamadı: {}", e)))?;
            let path = entry.path();
            
            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                let file_name = path.file_stem()
                    .and_then(|s| s.to_str())
                    .ok_or_else(|| DbError::FileSystemError("Geçersiz dosya adı".to_string()))?;
                
                let json_content = fs::read_to_string(&path)
                    .map_err(|e| DbError::FileSystemError(format!("Dosya okunamadı {}: {}", file_name, e)))?;
                
                let table: Table = serde_json::from_str(&json_content)
                    .map_err(|e| DbError::SerializationError(format!("JSON parse edilemedi {}: {}", file_name, e)))?;
                
                self.tables.insert(file_name.to_string(), table);
                println!("📁 Tablo yüklendi: {}", file_name);
            }
        }
        
        Ok(())
    }

    pub fn save_tables(&self) -> Result<(), DbError> {
        self.ensure_data_directory();
        
        for (table_name, table) in &self.tables {
            let file_path = format!("{}/{}.json", self.data_directory, table_name);
            
            let json_content = serde_json::to_string_pretty(table)
                .map_err(|e| DbError::SerializationError(format!("JSON serialize edilemedi {}: {}", table_name, e)))?;
            
            fs::write(&file_path, json_content)
                .map_err(|e| DbError::FileSystemError(format!("Dosya yazılamadı {}: {}", table_name, e)))?;
            
            println!("💾 Tablo kaydedildi: {}", table_name);
        }
        
        Ok(())
    }

    pub fn save_table(&self, table_name: &str) -> Result<(), DbError> {
        let table = self.tables.get(table_name)
            .ok_or_else(|| DbError::table_not_found(table_name))?;
        
        self.ensure_data_directory();
        
        let file_path = format!("{}/{}.json", self.data_directory, table_name);
        
        let json_content = serde_json::to_string_pretty(table)
                .map_err(|e| DbError::SerializationError(format!("JSON serialize edilemedi {}: {}", table_name, e)))?;
        
        fs::write(&file_path, json_content)
            .map_err(|e| DbError::FileSystemError(format!("Dosya yazılamadı {}: {}", table_name, e)))?;
        
        Ok(())
    }

    /// SQL komutunu çalıştırır - QueryExecutor'ı kullanır
    pub fn execute_sql(&mut self, sql: &str) -> Result<QueryResult, DbError> {
        // Normalize SQL for cache key
        let normalized_sql = sql.trim().to_lowercase();
        
        // Check cache for read-only queries (SELECT, EXPLAIN, SHOW STATS)
        if normalized_sql.starts_with("select") || normalized_sql.starts_with("explain") || normalized_sql.starts_with("show stats") {
            let cache_key = Self::create_cache_key(&normalized_sql);
            
            if let Some(cached_result) = self.query_cache.get(&cache_key) {
                // Parse cached result back
                if let Ok(result) = serde_json::from_str::<QueryResult>(&cached_result) {
                    return Ok(result);
                }
            }
        }
        
        // Execute query normally
        let result = self.executor.execute_sql(sql, &mut self.tables)?;
        
        // Cache SELECT results
        if normalized_sql.starts_with("select") || normalized_sql.starts_with("explain") || normalized_sql.starts_with("show stats") {
            let cache_key = Self::create_cache_key(&normalized_sql);
            if let Ok(json_result) = serde_json::to_string(&result) {
                self.query_cache.put(cache_key, json_result);
            }
        }
        
        // Invalidate cache for write operations
        if normalized_sql.starts_with("insert") || normalized_sql.starts_with("update") || 
           normalized_sql.starts_with("delete") || normalized_sql.starts_with("create") || 
           normalized_sql.starts_with("drop") {
            self.query_cache.clear();
        }
        
        // Başarılı işlemlerden sonra tabloları kaydet
        match &result {
            QueryResult::Success { message, .. } => {
                // Sadece değişiklik yapan işlemlerde kaydet
                if message.contains("created") || message.contains("inserted") || 
                   message.contains("updated") || message.contains("deleted") || 
                   message.contains("dropped") {
                    if let Err(e) = self.save_tables() {
                        eprintln!("⚠️  Tablolar kaydedilirken hata: {}", e);
                    }
                }
            }
            QueryResult::Select { .. } => {
                // SELECT işlemleri için kaydetme gerekli değil
            }
        }
        
        Ok(result)
    }
    
    /// Creates a cache key from normalized SQL
    fn create_cache_key(sql: &str) -> String {
        // Simple pattern matching for cache key generation
        let mut key = sql.to_string();
        
        // Replace specific values with placeholders for better cache hit rates
        key = key.replace("'", "");
        key = key.replace("\"", "");
        
        // Replace numbers with placeholders
        key = regex::Regex::new(r"\b\d+\b").unwrap().replace_all(&key, "?").to_string();
        
        key
    }

    /// Parse edilmiş AST'yi çalıştırır - QueryExecutor'ı kullanır
    pub fn execute_statement(&mut self, statement: SqlStatement) -> Result<QueryResult, DbError> {
        // Transaction komutlarını önce kontrol et
        match &statement {
            SqlStatement::BeginTransaction { isolation_level } => {
                return self.execute_begin_transaction(isolation_level);
            }
            SqlStatement::CommitTransaction => {
                return self.execute_commit_transaction();
            }
            SqlStatement::RollbackTransaction => {
                return self.execute_rollback_transaction();
            }
            SqlStatement::ShowTransactions => {
                return self.execute_show_transactions();
            }
            _ => {}
        }
        
        let result = self.executor.execute_statement(statement, &mut self.tables)?;
        
        // Başarılı işlemlerden sonra tabloları kaydet
        match &result {
            QueryResult::Success { message, .. } => {
                // Sadece değişiklik yapan işlemlerde kaydet
                if message.contains("created") || message.contains("inserted") || 
                   message.contains("updated") || message.contains("deleted") || 
                   message.contains("dropped") {
                    if let Err(e) = self.save_tables() {
                        eprintln!("⚠️  Tablolar kaydedilirken hata: {}", e);
                    }
                }
            }
            QueryResult::Select { .. } => {
                // SELECT işlemleri için kaydetme gerekli değil
            }
        }
        
        Ok(result)
    }

    // Geriye uyumluluk için eski metotlar (deprecated)
    #[deprecated(note = "Use execute_sql instead")]
    pub fn execute_create_table(&mut self, sql: &str) -> Result<QueryResult, String> {
        self.execute_sql(sql).map_err(|e| e.to_string())
    }

    #[deprecated(note = "Use execute_sql instead")]
    pub fn execute_insert_into(&mut self, sql: &str) -> Result<QueryResult, String> {
        self.execute_sql(sql).map_err(|e| e.to_string())
    }

    #[deprecated(note = "Use execute_sql instead")]
    pub fn execute_select_all(&self, sql: &str) -> Result<QueryResult, String> {
        // Bu metot const olduğu için özel bir çözüm gerekiyor
        let executor = QueryExecutor::new();
        let mut tables_copy = self.tables.clone();
        executor.execute_sql(sql, &mut tables_copy).map_err(|e| e.to_string())
    }

    // Önceki adımlardaki fonksiyonlar - özelleştirilmiş hata yönetimi ile güncellendi
    fn execute_create_table_ast(&mut self, table_name: String, columns: Vec<ColumnDefinition>) -> Result<QueryResult, String> {
        self.execute_sql(&format!("CREATE TABLE {} ({:?})", table_name, columns))
            .map_err(|e| e.to_string())
    }

    fn execute_insert_ast(&mut self, table_name: String, values: Vec<SqlValue>) -> Result<QueryResult, String> {
        self.execute_sql(&format!("INSERT INTO {} VALUES ({:?})", table_name, values))
            .map_err(|e| e.to_string())
    }

    fn execute_select_ast(&self, table_name: String, columns: Vec<String>) -> Result<QueryResult, String> {
        let cols = if columns.is_empty() { "*".to_string() } else { columns.join(", ") };
        self.execute_select_all(&format!("SELECT {} FROM {}", cols, table_name))
    }

    fn execute_update_ast(&mut self, table_name: String, assignments: Vec<Assignment>, where_clause: Option<WhereClause>) -> Result<QueryResult, String> {
        let set_clause = assignments.iter()
            .map(|a| format!("{} = {:?}", a.column, a.value))
            .collect::<Vec<_>>()
            .join(", ");
        
        let where_part = if let Some(where_clause) = where_clause {
            format!(" WHERE {:?}", where_clause)
        } else {
            String::new()
        };
        
        self.execute_sql(&format!("UPDATE {} SET {}{}", table_name, set_clause, where_part))
            .map_err(|e| e.to_string())
    }

    fn execute_delete_ast(&mut self, table_name: String, where_clause: Option<WhereClause>) -> Result<QueryResult, String> {
        let where_part = if let Some(where_clause) = where_clause {
            format!(" WHERE {:?}", where_clause)
        } else {
            String::new()
        };
        
        self.execute_sql(&format!("DELETE FROM {}{}", table_name, where_part))
            .map_err(|e| e.to_string())
    }

    fn execute_drop_table_ast(&mut self, table_name: String) -> Result<QueryResult, String> {
        self.execute_sql(&format!("DROP TABLE {}", table_name))
            .map_err(|e| e.to_string())
    }

    fn convert_sql_value_to_typed_value(sql_value: &SqlValue, data_type: &DataType) -> Result<TypedValue, String> {
        match (sql_value, data_type) {
            (SqlValue::Integer(i), DataType::INT) => Ok(TypedValue::Integer(*i)),
            (SqlValue::Text(s), DataType::TEXT) => Ok(TypedValue::Text(s.clone())),
            (SqlValue::Boolean(b), DataType::BOOL) => Ok(TypedValue::Boolean(*b)),
            (SqlValue::Null, _) => Ok(TypedValue::Null),
            _ => Err(format!("Type mismatch: {:?} cannot be converted to {:?}", sql_value, data_type)),
        }
    }

    fn evaluate_condition(condition: &Condition, row: &Row) -> Result<bool, String> {
        match condition {
            Condition::Equal(column, value) => {
                let row_value = row.get(column)
                    .ok_or_else(|| format!("Column '{}' not found", column))?;
                Ok(Self::compare_values(row_value, value) == std::cmp::Ordering::Equal)
            }
            _ => Err("Complex conditions not yet implemented".to_string()),
        }
    }

    fn compare_values(typed_value: &TypedValue, sql_value: &SqlValue) -> std::cmp::Ordering {
        match (typed_value, sql_value) {
            (TypedValue::Integer(a), SqlValue::Integer(b)) => a.cmp(b),
            (TypedValue::Text(a), SqlValue::Text(b)) => a.cmp(b),
            (TypedValue::Boolean(a), SqlValue::Boolean(b)) => a.cmp(b),
            _ => std::cmp::Ordering::Equal,
        }
    }

    // Transaction execution functions
    fn execute_begin_transaction(&mut self, isolation_level: &Option<String>) -> Result<QueryResult, DbError> {
        let isolation = match isolation_level {
            Some(level) => match level.as_str() {
                "READ_COMMITTED" => IsolationLevel::ReadCommitted,
                "REPEATABLE_READ" => IsolationLevel::RepeatableRead,
                "SERIALIZABLE" => IsolationLevel::Serializable,
                _ => IsolationLevel::ReadCommitted, // Default
            },
            None => IsolationLevel::ReadCommitted, // Default
        };

        let mut tx_manager = self.transaction_manager.write().unwrap();
        let transaction_id = tx_manager.begin_transaction(isolation.clone());
        self.current_transaction_id = Some(transaction_id);

        Ok(QueryResult::Success {
            message: format!("Transaction {} started with isolation level {:?}", transaction_id, isolation),
            execution_time_ms: 0,
        })
    }

    fn execute_commit_transaction(&mut self) -> Result<QueryResult, DbError> {
        let transaction_id = self.current_transaction_id
            .ok_or_else(|| DbError::ExecutionError("No active transaction to commit".to_string()))?;

        let mut tx_manager = self.transaction_manager.write().unwrap();
        
        // Check for deadlocks before committing
        let deadlocks = tx_manager.detect_deadlocks();
        if !deadlocks.is_empty() {
            let aborted = tx_manager.resolve_deadlocks();
            if aborted.contains(&transaction_id) {
                self.current_transaction_id = None;
                return Err(DbError::ExecutionError("Transaction aborted due to deadlock".to_string()));
            }
        }

        tx_manager.commit_transaction(transaction_id)
            .map_err(|e| DbError::ExecutionError(format!("Failed to commit transaction: {}", e)))?;

        self.current_transaction_id = None;

        Ok(QueryResult::Success {
            message: format!("Transaction {} committed successfully", transaction_id),
            execution_time_ms: 0,
        })
    }

    fn execute_rollback_transaction(&mut self) -> Result<QueryResult, DbError> {
        let transaction_id = self.current_transaction_id
            .ok_or_else(|| DbError::ExecutionError("No active transaction to rollback".to_string()))?;

        let mut tx_manager = self.transaction_manager.write().unwrap();
        tx_manager.rollback_transaction(transaction_id)
            .map_err(|e| DbError::ExecutionError(format!("Failed to rollback transaction: {}", e)))?;

        self.current_transaction_id = None;

        Ok(QueryResult::Success {
            message: format!("Transaction {} rolled back successfully", transaction_id),
            execution_time_ms: 0,
        })
    }

    fn execute_show_transactions(&self) -> Result<QueryResult, DbError> {
        let tx_manager = self.transaction_manager.read().unwrap();
        
        let columns = vec!["Transaction ID".to_string(), "State".to_string(), "Isolation Level".to_string(), "Start Time".to_string()];
        let mut rows = Vec::new();

        for (tx_id, transaction) in tx_manager.get_active_transactions() {
            let row = vec![
                tx_id.to_string(),
                format!("{:?}", transaction.state),
                format!("{:?}", transaction.isolation_level),
                transaction.start_timestamp.to_string(),
            ];
            rows.push(row);
        }

        Ok(QueryResult::Select { 
            columns, 
            rows,
            execution_time_ms: 0,
        })
    }

    pub fn get_current_transaction_id(&self) -> Option<TransactionId> {
        self.current_transaction_id
    }

    pub fn is_in_transaction(&self) -> bool {
        self.current_transaction_id.is_some()
    }
}

// Backward compatibility - eski QueryResult enum'ı korundu
#[derive(Debug, Clone, PartialEq)]
pub enum OldQueryResult {
    Success(String),
    Select { columns: Vec<String>, rows: Vec<Vec<String>> },
} 