use crate::row::Row;
use crate::table::Table;
use crate::types::{DataType, TypedValue};
use crate::parser::{SqlStatement, SqlValue, ColumnDefinition, WhereClause, Condition, Assignment};
use crate::executor::{QueryExecutor, QueryResult};
use crate::errors::DbError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Database {
    pub tables: HashMap<String, Table>,
    pub data_directory: String,
    #[serde(skip)]
    executor: QueryExecutor,
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
        let result = self.executor.execute_sql(sql, &mut self.tables)?;
        
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

    /// Parse edilmiş AST'yi çalıştırır - QueryExecutor'ı kullanır
    pub fn execute_statement(&mut self, statement: SqlStatement) -> Result<QueryResult, DbError> {
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
}

// Backward compatibility - eski QueryResult enum'ı korundu
#[derive(Debug, Clone, PartialEq)]
pub enum OldQueryResult {
    Success(String),
    Select { columns: Vec<String>, rows: Vec<Vec<String>> },
} 