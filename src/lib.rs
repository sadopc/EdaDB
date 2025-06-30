// lib.rs - Tam ve eksiksiz NoSQL in-memory database with query engine

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use thiserror::Error;
use uuid::Uuid;

// Query modülünü declare edin
pub mod query;

// AST modülünü declare edin - Abstract Syntax Tree for SQL-like queries
pub mod ast;

// Query parser modülünü declare edin - SQL-like query language parser
pub mod query_parser;

// Query engine modülünü declare edin - SQL-like query execution engine  
pub mod query_engine;

// Index modülünü declare edin - gelişmiş indexing sistemi
pub mod index;

// Persistence modülünü declare edin - WAL ve recovery sistemi
pub mod persistence;

// Transaction modülünü declare edin - ACID transaction sistemi
pub mod transaction;

// Protocol modülünü declare edin - network communication protocols
pub mod protocol;

// Network modülünü declare edin - TCP server implementation
pub mod network;

// Client modülünü declare edin - database client API
pub mod client;

// Schema validation modülünü declare edin - JSON Schema validation sistemi
pub mod schema;

// Query modülünden public export'lar
pub use query::{
    QueryBuilder, QueryableDatabase, Query, JsonPath,
    ComparisonOperator, SortDirection, ProjectionType,
    WhereClause, SortClause
};

// AST modülünden public export'lar
pub use ast::{
    Query as SqlQuery, SelectQuery, InsertQuery, UpdateQuery, DeleteQuery, CreateQuery,
    Field, Assignment, Condition, OrderBy, Expression,
    ComparisonOperator as SqlComparisonOperator, SortDirection as SqlSortDirection,
    BinaryOperator
};

// Query parser modülünden public export'lar  
pub use query_parser::{
    parse, ParserError, TokenType, Token, Lexer, Parser
};

// Query engine modülünden public export'lar
pub use query_engine::{
    QueryEngine, QueryResult
};

// Index modülünden public export'lar
pub use index::{
    IndexManager, IndexConfig, IndexType, IndexStats,
    IndexValue, CompositeKey
};

// Persistence modülünden public export'lar
pub use persistence::{
    WalManager, WalConfig, WalFormat, WalEntry, WalEntryType,
    RecoveryManager, RecoveryInfo, SnapshotData,
    PersistentMemoryStorage
};

// Transaction modülünden public export'lar
pub use transaction::{
    TransactionManager, TransactionalStorage,
    IsolationLevel, TransactionStatus, TransactionId,
    LockType, LockManager, VersionManager,
    ResourceId, TransactionContext
};

// Network modülünden public export'lar
pub use network::{
    DatabaseServer, ServerConfig, ConnectionPool, ClientConnection
};

// Client modülünden public export'lar
pub use client::{
    DatabaseClient, ClientConfig
};

// Schema validation modülünden public export'lar
pub use schema::{
    SchemaDefinition, FieldSchema, FieldType, SchemaBuilder, FieldSchemaBuilder,
    ValidationError, ValidationResult, ErrorContext, SchemaRegistry, SchemaConfig,
    ValidationEngine, ValidationOptions, Format, FormatValidator
};

/// Test için kullanılacak örnek kullanıcı verisi
/// Bu struct'ı public yapıyoruz ki example'da kullanabilelim
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TestUser {
    pub name: String,
    pub email: String,
    pub age: u32,
}

impl TestUser {
    pub fn new(name: &str, email: &str, age: u32) -> Self {
        Self {
            name: name.to_string(),
            email: email.to_string(),
            age,
        }
    }
}

/// Veritabanı işlemlerinde karşılaşılabilecek hata türleri
/// Her hata tipi farklı senaryoları temsil eder ve farklı handling stratejileri gerektirir
#[derive(Error, Debug, PartialEq, Serialize, Deserialize)]
pub enum DatabaseError {
    /// Aranan döküman bulunamadığında döndürülür
    /// Bu genellikle 404 Not Found response'una karşılık gelir
    #[error("Document with ID {id} not found")]
    DocumentNotFound { id: String },

    /// JSON serialize/deserialize işlemlerinde hata oluştuğunda
    /// Genellikle malformed data ya da type mismatch durumlarında
    #[error("Serialization error: {message}")]
    SerializationError { message: String },

    /// Thread synchronization sırasında lock alınamadığında
    /// Bu genellikle deadlock ya da poison lock durumlarında oluşur
    #[error("Lock acquisition failed: {reason}")]
    LockError { reason: String },

    /// Geçersiz query parametreleri için
    #[error("Invalid query: {message}")]
    InvalidQuery { message: String },

    /// Döküman zaten var ise (unique constraint ihlali)
    #[error("Document with ID {id} already exists")]
    DocumentAlreadyExists { id: String },

    /// Versiyonlar uyuşmazsa (optimistic locking için)
    #[error("Version mismatch: expected {expected}, got {actual}")]
    VersionMismatch { expected: u64, actual: u64 },

    /// Genel validation hataları için
    #[error("Validation error: {field} - {message}")]
    ValidationError { field: String, message: String },

    /// Storage kapasitesi dolduğunda
    #[error("Storage capacity exceeded: max {max_capacity} documents")]
    CapacityExceeded { max_capacity: usize },

    /// Döküman içeriği çok büyük olduğunda
    #[error("Document too large: {size} bytes, max allowed: {max_size} bytes")]
    DocumentTooLarge { size: usize, max_size: usize },

    /// Transaction hatalarý için yeni error type
    #[error("Transaction error: {message}")]
    TransactionError { message: String },

    /// Collection bulunamadığında döndürülür
    #[error("Collection '{collection}' not found")]
    CollectionNotFound { collection: String },

    /// Schema validation hataları için
    #[error("Schema validation error: {message}")]
    SchemaValidationError { 
        message: String,
        field_path: Option<String>,
        validation_errors: Vec<ValidationError>,
    },
}

// Serde JSON hatalarını kendi hata tipimize dönüştürmek için From implementation
impl From<serde_json::Error> for DatabaseError {
    fn from(error: serde_json::Error) -> Self {
        DatabaseError::SerializationError {
            message: error.to_string(),
        }
    }
}

// Schema validation hatalarını kendi hata tipimize dönüştürmek için From implementation
impl From<ValidationError> for DatabaseError {
    fn from(error: ValidationError) -> Self {
        DatabaseError::SchemaValidationError {
            message: error.to_string(),
            field_path: error.get_path().map(|p| p.to_string()),
            validation_errors: vec![error],
        }
    }
}

/// Her dökümanın sahip olması gereken temel bilgiler
/// Bu struct her veritabanı kaydının meta verilerini tutar
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentMetadata {
    /// Dökümanın benzersiz kimliği - UUID kullanarak çakışma riskini minimize ederiz
    pub id: Uuid,

    /// Dökümanın oluşturulma zamanı - UTC zaman damgası
    pub created_at: DateTime<Utc>,

    /// Son güncellenme zamanı - sorgu optimizasyonu için önemli
    pub updated_at: DateTime<Utc>,

    /// Döküman versiyonu - optimistic locking için kullanılabilir
    pub version: u64,
}

/// Veritabanında saklanan her dökümanı temsil eden ana yapı
/// T: dökümanın içeriği için generic tip parametresi
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document<T> {
    /// Döküman meta verileri
    pub metadata: DocumentMetadata,

    /// Dökümanın asıl içeriği - JSON benzeri yapı
    pub data: T,
}

impl<T> Document<T> {
    /// Yeni bir döküman oluşturur
    /// Bu method dökümanın oluşturulma anında gerekli meta verileri otomatik olarak ayarlar
    pub fn new(data: T) -> Self {
        let now = Utc::now();
        Self {
            metadata: DocumentMetadata {
                id: Uuid::new_v4(), // V4 UUID rastgele ve benzersiz
                created_at: now,
                updated_at: now,
                version: 1,
            },
            data,
        }
    }

    /// Dökümanın içeriğini günceller ve metadata'sını yeniler
    pub fn update(&mut self, new_data: T) {
        self.data = new_data;
        self.metadata.updated_at = Utc::now();
        self.metadata.version += 1;
    }
}

/// CRUD operasyonları için gelişmiş trait tanımı
/// Bu trait, veritabanı operasyonlarının tam kapsamını kapsayacak şekilde tasarlanmıştır
#[async_trait::async_trait]
pub trait CrudDatabase<T>: Send + Sync
where
    T: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync,
{
    // ================================
    // CREATE OPERATIONS (Oluşturma)
    // ================================

    /// Yeni bir döküman oluşturur ve benzersiz ID ile kaydeder
    /// Bu işlem atomik'tir - ya tamamen başarılı olur ya da hiç gerçekleşmez
    async fn create(&self, data: T) -> Result<Document<T>, DatabaseError>;

    /// Belirli bir ID ile döküman oluşturur (eğer mevcut değilse)
    /// Bu method, ID kontrolü gerektiğinde kullanılır
    async fn create_with_id(&self, id: Uuid, data: T) -> Result<Document<T>, DatabaseError>;

    /// Bir batch içerisinde birden fazla döküman oluşturur
    /// Bu işlem atomik'tir - hepsi başarılı olur ya da hiçbiri oluşturulmaz
    async fn create_batch(&self, documents: Vec<T>) -> Result<Vec<Document<T>>, DatabaseError>;

    // ================================
    // READ OPERATIONS (Okuma)
    // ================================

    /// ID ile tekil döküman okur - en temel read operasyonu
    async fn read_by_id(&self, id: &Uuid) -> Result<Option<Document<T>>, DatabaseError>;

    /// Birden fazla ID ile dökümanları okur - batch read operasyonu
    async fn read_by_ids(&self, ids: &[Uuid]) -> Result<Vec<Document<T>>, DatabaseError>;

    /// Tüm dökümanları sayfalama ile okur
    /// offset: kaçıncı kayıttan başlayacağı, limit: kaç kayıt getireceği
    async fn read_all(&self, offset: Option<usize>, limit: Option<usize>) -> Result<Vec<Document<T>>, DatabaseError>;

    /// Belirli bir tarih aralığında oluşturulan dökümanları okur
    async fn read_by_date_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>
    ) -> Result<Vec<Document<T>>, DatabaseError>;

    /// Döküman sayısını döndürür - count operasyonu
    async fn count(&self) -> Result<usize, DatabaseError>;

    // ================================
    // UPDATE OPERATIONS (Güncelleme)
    // ================================

    /// Mevcut dökümanın içeriğini tamamen değiştirir
    /// Bu işlem optimistic locking kullanır - version check yapar
    async fn update(&self, id: &Uuid, data: T) -> Result<Document<T>, DatabaseError>;

    /// Belirli version numarası ile güncelleme yapar (optimistic locking)
    /// Bu, concurrent update'leri önlemek için kullanılır
    async fn update_with_version(&self, id: &Uuid, data: T, expected_version: u64) -> Result<Document<T>, DatabaseError>;

    /// Döküman varsa günceller, yoksa oluşturur (upsert operasyonu)
    async fn upsert(&self, id: &Uuid, data: T) -> Result<(Document<T>, bool), DatabaseError>; // bool: created or updated

    /// Birden fazla dökümanı aynı anda günceller
    async fn update_batch(&self, updates: Vec<(Uuid, T)>) -> Result<Vec<Document<T>>, DatabaseError>;

    // ================================
    // DELETE OPERATIONS (Silme)
    // ================================

    /// Tek dökümanı ID ile siler
    async fn delete(&self, id: &Uuid) -> Result<bool, DatabaseError>; // bool: silindi mi?

    /// Belirli version ile döküman siler (optimistic locking)
    async fn delete_with_version(&self, id: &Uuid, expected_version: u64) -> Result<bool, DatabaseError>;

    /// Birden fazla dökümanı aynı anda siler
    async fn delete_batch(&self, ids: &[Uuid]) -> Result<usize, DatabaseError>; // silinen sayısı

    /// Belirli tarih aralığındaki dökümanları siler
    async fn delete_by_date_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>
    ) -> Result<usize, DatabaseError>;

    /// Tüm dökümanları siler (tehlikeli operasyon!)
    async fn delete_all(&self) -> Result<usize, DatabaseError>;

    // ================================
    // UTILITY OPERATIONS (Yardımcı)
    // ================================

    /// Dökümanın mevcut olup olmadığını kontrol eder
    async fn exists(&self, id: &Uuid) -> Result<bool, DatabaseError>;

    /// Storage'ın mevcut durumu hakkında bilgi döndürür
    async fn stats(&self) -> Result<StorageStats, DatabaseError>;
}

/// Storage durumu hakkında bilgi veren struct
/// Bu bilgiler monitoring ve performance analizi için kullanılır
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStats {
    /// Toplam döküman sayısı
    pub total_documents: usize,
    /// Storage'da kullanılan yaklaşık memory miktarı (bytes)
    pub estimated_memory_usage: usize,
    /// En eski dökümanın oluşturulma zamanı
    pub oldest_document: Option<DateTime<Utc>>,
    /// En yeni dökümanın oluşturulma zamanı
    pub newest_document: Option<DateTime<Utc>>,
    /// Ortalama döküman versiyonu
    pub average_version: f64,
}

/// HashMap tabanlı in-memory storage implementasyonu
/// Bu implementasyon tam CRUD operasyonlarını thread-safe şekilde destekler
/// VE gelişmiş indexing sistemi ile optimize edilmiş query performansı sağlar
#[derive(Debug)]
pub struct MemoryStorage<T> {
    /// Ana veri depolama alanı - UUID'den Document'a mapping
    /// Arc<RwLock<>> kullanarak thread-safety sağlıyoruz
    /// RwLock: Birden fazla okuyucu ama tek yazıcı prensibini uygular
    storage: Arc<RwLock<HashMap<Uuid, Document<T>>>>,

    /// Index Manager - tüm secondary index'leri yönetir
    /// Bu sistem sayesinde query'ler O(n) yerine O(1) veya O(log n) complexity'de çalışabilir
    /// Arc ile wrapped çünkü query engine ile paylaşılabilir olması gerekiyor
    index_manager: Arc<crate::index::IndexManager>,

    /// Schema registry for validation
    /// Thread-safe schema management system
    schema_registry: Arc<SchemaRegistry>,

    /// Collection name for this storage instance
    /// Used for schema validation
    collection_name: Option<String>,

    /// Maximum storage kapasitesi (opsiyonel limit)
    max_capacity: Option<usize>,

    /// Maximum döküman boyutu (bytes) (opsiyonel limit)
    max_document_size: Option<usize>,
}

/// Bu implementasyon tam CRUD operasyonlarını thread-safe şekilde destekler
/// VE gelişmiş indexing sistemi ile optimize edilmiş query performansı sağlar
///
/// LIFETIME CONSTRAINT AÇIKLAMASI:
/// T: 'static bound eklemek zorundayız çünkü:
/// 1. std::any::TypeId::of::<T>() kullanıyoruz (runtime type checking için)
/// 2. document as &dyn std::any::Any casting yapıyoruz
/// 3. Any trait 'static lifetime gerektirir (Rust'ın güvenlik garantisi)
///
/// 'static constraint'i neden güvenlidir?
/// - Sadece compile-time type checking için kullanıyoruz
/// - Runtime'da hiçbir data 'static olmak zorunda değil
/// - Bu sadece type metadata'sının program boyunca available olmasını garantiler
impl<T> MemoryStorage<T>
where
    T: 'static, {
    /// Yeni bir boş in-memory storage oluşturur
    /// Herhangi bir kapasite ya da boyut sınırı olmadan oluşturulur
    /// Yeni: Her instance kendi IndexManager'ı ile birlikte oluşturulur
    /// Bu tasarım sayesinde farklı storage instance'ları farklı index stratejileri kullanabilir
    pub fn new() -> Self {
        Self {
            storage: Arc::new(RwLock::new(HashMap::new())),
            index_manager: Arc::new(crate::index::IndexManager::new()),
            schema_registry: Arc::new(SchemaRegistry::new()),
            collection_name: None,
            max_capacity: None,
            max_document_size: None,
        }
    }

    /// Storage'ı önceden belirlenmiş kapasiteyle oluşturur
    /// Bu method, HashMap'in başlangıçta memory allocation yapmasını sağlar
    /// Çok sayıda döküman bekleniyorsa performance optimizasyonu sağlar
    /// Index Manager: Her instance'ın bağımsız index yönetimi için ayrı IndexManager
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            storage: Arc::new(RwLock::new(HashMap::with_capacity(capacity))),
            index_manager: Arc::new(crate::index::IndexManager::new()),
            schema_registry: Arc::new(SchemaRegistry::new()),
            collection_name: None,
            max_capacity: None,
            max_document_size: None,
        }
    }

    /// Maximum kapasite limiti ile storage oluşturur
    /// Bu limit aşıldığında CapacityExceeded error'u döndürülür
    /// Index Manager: Kapasite sınırlı sistemlerde de index performansından faydalanmak için
    pub fn with_max_capacity(max_capacity: usize) -> Self {
        Self {
            storage: Arc::new(RwLock::new(HashMap::with_capacity(max_capacity.min(1000)))),
            index_manager: Arc::new(crate::index::IndexManager::new()),
            schema_registry: Arc::new(SchemaRegistry::new()),
            collection_name: None,
            max_capacity: Some(max_capacity),
            max_document_size: None,
        }
    }

    /// Hem kapasite hem de döküman boyutu limiti ile storage oluşturur
    /// Production ortamında resource management için kullanılır
    /// Index Manager: Production sistemlerinde performans kritik olduğu için index desteği şart
    pub fn with_limits(max_capacity: usize, max_document_size: usize) -> Self {
        Self {
            storage: Arc::new(RwLock::new(HashMap::with_capacity(max_capacity.min(1000)))),
            index_manager: Arc::new(crate::index::IndexManager::new()),
            schema_registry: Arc::new(SchemaRegistry::new()),
            collection_name: None,
            max_capacity: Some(max_capacity),
            max_document_size: Some(max_document_size),
        }
    }

    /// Create storage with collection name for schema validation
    pub fn with_collection(collection_name: &str) -> Self {
        Self {
            storage: Arc::new(RwLock::new(HashMap::new())),
            index_manager: Arc::new(crate::index::IndexManager::new()),
            schema_registry: Arc::new(SchemaRegistry::new()),
            collection_name: Some(collection_name.to_string()),
            max_capacity: None,
            max_document_size: None,
        }
    }

    /// Create storage with collection name and shared schema registry
    pub fn with_collection_and_registry(collection_name: &str, schema_registry: Arc<SchemaRegistry>) -> Self {
        Self {
            storage: Arc::new(RwLock::new(HashMap::new())),
            index_manager: Arc::new(crate::index::IndexManager::new()),
            schema_registry,
            collection_name: Some(collection_name.to_string()),
            max_capacity: None,
            max_document_size: None,
        }
    }

    // ================================
    // INDEX MANAGEMENT HELPER METHODS
    // Bu method'lar MemoryStorage<T>'nin bir parçasıdır ve index sistemi ile
    // CRUD operasyonları arasında köprü görevini görür
    // ================================

    /// Type-safe index güncelleme helper method'u - CREATE operasyonları için
    ///
    /// Bu method neden MemoryStorage'da tanımlı?
    /// Çünkü CRUD operasyonları (create, update, delete) sırasında index'lerin
    /// güncellenmesi MemoryStorage'ın sorumluluğundadır. Bu method, generic
    /// type system ile JSON Value tabanlı index system arasında güvenli köprü kurar.
    ///
    /// Type Safety Yaklaşımı:
    /// 1. Runtime'da T'nin gerçekte Value olup olmadığını kontrol eder
    /// 2. Eğer öyleyse, güvenli type casting yapar
    /// 3. Index manager'a güncelleme talimatı gönderir
    /// 4. Hata durumunda session crash etmez, sadece warning log'lar
    fn try_update_indexes_on_create(&self, document: &Document<T>) {
        // TypeId ile runtime type checking - bu Rust'ın güçlü type safety özelliği
        if std::any::TypeId::of::<T>() == std::any::TypeId::of::<serde_json::Value>() {
            // Any trait ile safe downcasting - unsafe kod kullanmadan type conversion
            let any_document = document as &dyn std::any::Any;
            if let Some(value_document) = any_document.downcast_ref::<Document<serde_json::Value>>() {
                // Index güncelleme - error handling ile sistem stabilizasyonu
                if let Err(index_error) = self.index_manager.index_document(value_document) {
                    log::warn!("Failed to update indexes after document creation: {:?}", index_error);
                }
            }
        }
    }

    /// Type-safe index güncelleme helper method'u - DELETE operasyonları için
    ///
    /// Delete İşlemlerinin Özel Durumu:
    /// 1. Index'ten kaldırma işlemi field değerlerine ihtiyaç duyar
    /// 2. Döküman silindikten sonra bu değerlere erişim mümkün olmaz
    /// 3. Bu yüzden silmeden ÖNCE index'leri temizlememiz gerekir
    ///
    /// Bu sequencing pattern, ACID transactions'ın temel prensiplerindendir.
    /// Production veritabanlarında bu işlem write-ahead logging ile korunur.
    fn try_update_indexes_on_delete(&self, document: &Document<T>) {
        if std::any::TypeId::of::<T>() == std::any::TypeId::of::<serde_json::Value>() {
            let any_document = document as &dyn std::any::Any;
            if let Some(value_document) = any_document.downcast_ref::<Document<serde_json::Value>>() {
                // Index'ten döküman referansını kaldır
                if let Err(index_error) = self.index_manager.remove_document(value_document) {
                    log::warn!("Failed to update indexes after document deletion: {:?}", index_error);
                }
            }
        }
    }

    /// Index Manager'a erişim sağlar - query optimization için kritik
    ///
    /// Bu method'un Query Engine İçin Önemi:
    /// Query engine, optimal performans için en uygun index'i seçmek zorundadır.
    /// Bu method sayesinde query planner, mevcut index'leri analiz edebilir ve
    /// query execution strategy'sini optimize edebilir.
    ///
    /// Arc cloning: Reference counting ile memory safe sharing sağlar
    pub fn get_index_manager(&self) -> Arc<crate::index::IndexManager> {
        Arc::clone(&self.index_manager)
    }

    /// Schema Registry'ye erişim sağlar - validation operations için kritik
    pub fn get_schema_registry(&self) -> Arc<SchemaRegistry> {
        Arc::clone(&self.schema_registry)
    }

    /// Register a schema for this storage's collection
    pub fn register_schema(&self, schema: SchemaDefinition, config: Option<SchemaConfig>) -> Result<(), DatabaseError> {
        if let Some(collection_name) = &self.collection_name {
            self.schema_registry.register_schema(collection_name, schema, config)?;
            Ok(())
        } else {
            Err(DatabaseError::SchemaValidationError {
                message: "Cannot register schema: collection name not set".to_string(),
                field_path: None,
                validation_errors: vec![],
            })
        }
    }

    /// Validate document data against schema before operations
    fn validate_document_data(&self, data: &T) -> Result<(), DatabaseError> 
    where
        T: Serialize
    {
        // Only validate if we have a collection name and T is serde_json::Value
        if let Some(collection_name) = &self.collection_name {
            if std::any::TypeId::of::<T>() == std::any::TypeId::of::<serde_json::Value>() {
                let any_data = data as &dyn std::any::Any;
                if let Some(value_data) = any_data.downcast_ref::<serde_json::Value>() {
                    self.schema_registry.validate_document(collection_name, value_data, None)?;
                }
            }
        }
        Ok(())
    }

    /// Enable or disable schema validation for this collection
    pub fn set_validation_enabled(&self, enabled: bool) -> Result<(), DatabaseError> {
        if let Some(collection_name) = &self.collection_name {
            self.schema_registry.set_validation_enabled(collection_name, enabled)?;
            Ok(())
        } else {
            Err(DatabaseError::SchemaValidationError {
                message: "Cannot configure validation: collection name not set".to_string(),
                field_path: None,
                validation_errors: vec![],
            })
        }
    }

    /// Index yönetimi için public interface - manual index oluşturma
    ///
    /// Bu method kullanıcılara index yaratma yeteneği sağlar
    /// Örnek kullanım:
    /// ```
    /// storage.create_index("age_index", vec!["age"], IndexType::BTree)?;
    /// storage.create_index("name_email_idx", vec!["name", "email"], IndexType::Hash)?;
    /// ```
    ///
    /// Index Yaratma Süreci:
    /// 1. Konfigürasyon validasyonu
    /// 2. Index structure oluşturma
    /// 3. Mevcut dökümanları yeni index'e ekleme (backfill)
    pub fn create_index(&self, index_name: &str, fields: Vec<&str>, index_type: crate::index::IndexType) -> Result<(), DatabaseError> {
        let config = crate::index::IndexConfig::new(
            index_name.to_string(),
            fields.into_iter().map(|s| s.to_string()).collect(),
            index_type,
        );

        self.index_manager.create_index(config)?;

        // Mevcut dökümanları yeni index'e ekle - bu işlem O(n) complexity'de
        // Büyük veri setlerinde zaman alabilir
        if std::any::TypeId::of::<T>() == std::any::TypeId::of::<serde_json::Value>() {
            self.rebuild_index_for_existing_documents(index_name)?;
        }

        Ok(())
    }

    /// Index silme operasyonu - geri alınamaz işlem
    ///
    /// Dikkat Edilmesi Gerekenler:
    /// - Bu işlem geri alınamaz
    /// - Index rebuild etmek O(n) complexity gerektirir
    /// - Production'da bu işlem backup stratejisi gerektirir
    pub fn drop_index(&self, index_name: &str) -> Result<(), DatabaseError> {
        self.index_manager.drop_index(index_name)
    }

    /// Mevcut index'lerin listesini döndürür
    ///
    /// Bu method monitoring ve debug için kullanılır:
    /// - Database health check'ler için
    /// - Query optimization analysis için
    /// - Performance monitoring için
    pub fn list_indexes(&self) -> Result<Vec<crate::index::IndexConfig>, DatabaseError> {
        self.index_manager.list_indexes()
    }

    /// Index statistics'lerini döndürür
    ///
    /// Performance Monitoring İçin Kritik Bilgiler:
    /// - Memory kullanımı
    /// - Entry count (cardinality)
    /// - Index efficiency metrics
    /// - Capacity planning dataları
    pub fn get_index_stats(&self, index_name: &str) -> Result<crate::index::IndexStats, DatabaseError> {
        self.index_manager.get_index_stats(index_name)
    }

    /// Mevcut dökümanları yeni index'e ekler - backfill operation
    ///
    /// Bu İşlem Neden Gerekli?
    /// Index yaratıldığında sadece structure oluşturulur, mevcut data indexlenmez.
    /// Bu method, veritabanındaki tüm mevcut dökümanları yeni index'e ekler.
    ///
    /// PERFORMANS NOTU: Bu işlem O(n) complexity'de çalışır ve büyük veri setlerinde
    /// uzun sürebilir. Production'da bu işlem background'da yapılmalı.
    fn rebuild_index_for_existing_documents(&self, index_name: &str) -> Result<(), DatabaseError> {
        let storage = self.storage.read()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire read lock for index rebuild: {}", e)
            })?;

        // Tüm dökümanları index'e ekle
        let mut indexed_count = 0;
        for document in storage.values() {
            let any_document = document as &dyn std::any::Any;
            if let Some(value_document) = any_document.downcast_ref::<Document<serde_json::Value>>() {
                if let Err(index_error) = self.index_manager.index_document(value_document) {
                    log::warn!("Failed to index existing document during rebuild: {:?}", index_error);
                } else {
                    indexed_count += 1;
                }
            }
        }

        log::info!("Rebuilt index '{}' with {} documents", index_name, indexed_count);
        Ok(())
    }

    /// Döküman boyutunu kontrol eden yardımcı method
    /// Serialization maliyeti olduğu için sadece gerektiğinde çağrılır
    fn validate_document_size(&self, document: &Document<T>) -> Result<(), DatabaseError>
    where
        T: Serialize
    {
        if let Some(max_size) = self.max_document_size {
            let serialized = serde_json::to_vec(document)?;

            if serialized.len() > max_size {
                return Err(DatabaseError::DocumentTooLarge {
                    size: serialized.len(),
                    max_size,
                });
            }
        }
        Ok(())
    }

    /// Kapasite limitini kontrol eden yardımcı method
    /// Bu kontrolü her create operasyonunda yapıyoruz
    fn validate_capacity(&self, current_size: usize) -> Result<(), DatabaseError> {
        if let Some(max_cap) = self.max_capacity {
            if current_size >= max_cap {
                return Err(DatabaseError::CapacityExceeded {
                    max_capacity: max_cap
                });
            }
        }
        Ok(())
    }
}

/// Default trait implementation - kolaylık için
/// Varsayılan olarak herhangi bir limit olmadan storage oluşturur
///
/// LIFETIME CONSTRAINT: Bu impl de T: 'static gerektirir çünkü
/// Self::new() çağrısı yapıyor ve new() method'u artık 'static bound gerektiriyor.
/// Rust'ın consistency principle'ına göre, eğer bir type belirli constraint'lere sahipse,
/// o type'ın tüm associated implementations da aynı constraint'lere sahip olmalıdır.
impl<T: 'static> Default for MemoryStorage<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Clone trait implementation - storage referansını klonlar (veriyi değil)
/// Bu sayede aynı storage'a birden fazla handle üzerinden erişebiliriz
/// Arc sayesinde reference counting ile memory güvenliğini koruyoruz
///
/// ÖNEMLİ: Index Manager da Arc ile wrapped olduğu için klonlandığında
/// aynı index state'i paylaşılır. Bu kritik çünkü farklı handle'lar arasında
/// index consistency'si sağlanması gerekir. Aksi takdirde bir handle'dan
/// eklenen index başka handle'larda görünmez ve veri tutarsızlığı oluşur.
///
/// LIFETIME CONSTRAINT: T: 'static bound consistency için eklendi.
/// Tüm MemoryStorage trait implementations aynı constraint'lere sahip olmalıdır.
impl<T: 'static> Clone for MemoryStorage<T> {
    fn clone(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
            index_manager: Arc::clone(&self.index_manager), // Index manager'ı da clone et
            schema_registry: Arc::clone(&self.schema_registry),
            collection_name: self.collection_name.clone(),
            max_capacity: self.max_capacity,
            max_document_size: self.max_document_size,
        }
    }
}

/// MemoryStorage için tam CRUD implementasyonu
/// Bu implementasyon thread-safe, performant ve error-safe olacak şekilde tasarlanmıştır
///
/// LIFETIME CONSTRAINT: T: 'static bound ekledik çünkü index operations'da
/// std::any::Any trait kullanıyoruz ve bu trait 'static lifetime gerektirir.
/// Bu constraint, type safety'yi artırır ve runtime type checking'i güvenli hale getirir.
#[async_trait::async_trait]
impl<T> CrudDatabase<T> for MemoryStorage<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static,
{
    // ================================
    // CREATE OPERATIONS - Thread-safe dokuman oluşturma
    // ================================

    /// Yeni döküman oluşturur - en temel create operasyonu
    /// Bu method atomik'tir: ya tamamen başarılı olur ya da hiçbir değişiklik yapmaz
    ///
    /// INDEX ENTEGRASYONU: Döküman eklendikten sonra tüm mevcut index'ler güncellenir
    /// Bu işlem şu adımları takip eder:
    /// 1. Dökümanı ana storage'a ekle
    /// 2. Index manager'a bildiriyi gönder (eğer destekleniyorsa)
    /// 3. Index manager otomatik olarak ilgili index'leri günceller
    ///
    /// PERFORMANS TRADE-OFF: Index güncelleme write performansını biraz düşürür ama
    /// read performansını dramatik olarak artırır. Bu trade-off çoğu use case için mantıklı
    async fn create(&self, data: T) -> Result<Document<T>, DatabaseError> {
        // Schema validation first
        self.validate_document_data(&data)?;
        
        let document = Document::new(data);

        // Döküman boyutu kontrolü (eğer limit varsa)
        self.validate_document_size(&document)?;

        // Write lock alıyoruz çünkü HashMap'e yeni eleman ekleyeceğiz
        let mut storage = self.storage.write()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire write lock for create: {}", e)
            })?;

        // Kapasite kontrolü - mevcut boyutu kontrol ediyoruz
        self.validate_capacity(storage.len())?;

        // Dökümanı storage'a ekliyoruz
        storage.insert(document.metadata.id, document.clone());

        // Lock'u serbest bırakıyoruz - index operations için başka lock'lar gerekebilir
        drop(storage);

        // INDEX GÜNCELLEME: Helper method ile type-safe index güncelleme
        // Bu approach unsafe kod kullanmaz ve type safety garantisi sağlar
        self.try_update_indexes_on_create(&document);

        Ok(document)
    }

    /// Belirli ID ile döküman oluşturur
    /// Bu method ID çakışmasını kontrol eder ve mevcut dökümanı korur
    async fn create_with_id(&self, id: Uuid, data: T) -> Result<Document<T>, DatabaseError> {
        // Schema validation first
        self.validate_document_data(&data)?;
        
        let mut document = Document::new(data);
        document.metadata.id = id; // İstenen ID'yi atıyoruz

        self.validate_document_size(&document)?;

        let mut storage = self.storage.write()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire write lock for create_with_id: {}", e)
            })?;

        self.validate_capacity(storage.len())?;

        // ID çakışması kontrolü - bu çok önemli çünkü unique constraint sağlıyor
        if storage.contains_key(&id) {
            return Err(DatabaseError::DocumentAlreadyExists {
                id: id.to_string()
            });
        }

        storage.insert(id, document.clone());
        drop(storage);

        // Index güncelleme
        self.try_update_indexes_on_create(&document);

        Ok(document)
    }

    /// Batch create operasyonu - birden fazla dökümanı atomik olarak oluşturur
    /// Bu method all-or-nothing prensibini uygular
    async fn create_batch(&self, documents: Vec<T>) -> Result<Vec<Document<T>>, DatabaseError> {
        if documents.is_empty() {
            return Ok(Vec::new());
        }

        // Önce tüm dökümanları hazırlıyoruz ve validate ediyoruz
        let mut prepared_docs = Vec::new();
        for data in documents {
            // Schema validation for each document
            self.validate_document_data(&data)?;
            
            let doc = Document::new(data);
            self.validate_document_size(&doc)?;
            prepared_docs.push(doc);
        }

        let mut storage = self.storage.write()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire write lock for create_batch: {}", e)
            })?;

        // Batch işlemi için toplam kapasite kontrolü
        self.validate_capacity(storage.len() + prepared_docs.len())?;

        // Tüm dökümanları tek seferde ekliyoruz
        // Bu sayede atomicity sağlıyoruz
        for doc in &prepared_docs {
            storage.insert(doc.metadata.id, doc.clone());
        }

        drop(storage);

        // Batch index güncelleme
        for doc in &prepared_docs {
            self.try_update_indexes_on_create(doc);
        }

        Ok(prepared_docs)
    }

    // ================================
    // READ OPERATIONS - Thread-safe döküman okuma
    // ================================

    /// Tekil döküman okuma - en sık kullanılan operasyon
    /// Read lock kullanarak concurrent read'lere izin veriyoruz
    async fn read_by_id(&self, id: &Uuid) -> Result<Option<Document<T>>, DatabaseError> {
        // Read lock alıyoruz - bu sayede birden fazla thread aynı anda okuyabilir
        // Sadece write operasyonları sırasında bloklanır
        let storage = self.storage.read()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire read lock for read_by_id: {}", e)
            })?;

        // HashMap.get() O(1) complexity'de çalışır
        Ok(storage.get(id).cloned())
    }

    /// Birden fazla ID ile batch read operasyonu
    /// Bu method tek lock ile birden fazla döküman okuyor - performance optimization
    async fn read_by_ids(&self, ids: &[Uuid]) -> Result<Vec<Document<T>>, DatabaseError> {
        let storage = self.storage.read()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire read lock for read_by_ids: {}", e)
            })?;

        let mut result = Vec::new();
        for id in ids {
            if let Some(doc) = storage.get(id) {
                result.push(doc.clone());
            }
        }

        Ok(result)
    }

    /// Tüm dökümanları sayfalama ile okur
    /// Büyük veri setleri için memory-efficient approach
    async fn read_all(&self, offset: Option<usize>, limit: Option<usize>) -> Result<Vec<Document<T>>, DatabaseError> {
        let storage = self.storage.read()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire read lock for read_all: {}", e)
            })?;

        let mut docs: Vec<Document<T>> = storage.values().cloned().collect();

        // Dökümanları oluşturulma zamanına göre sıralıyoruz
        // Bu consistent ordering sağlar ve sayfalama için gerekli
        docs.sort_by(|a, b| a.metadata.created_at.cmp(&b.metadata.created_at));

        // Sayfalama logic'i - offset ve limit parametrelerine göre
        let start = offset.unwrap_or(0);
        let end = limit.map(|l| start + l).unwrap_or(docs.len());

        Ok(docs.into_iter().skip(start).take(end.saturating_sub(start)).collect())
    }

    /// Tarih aralığına göre döküman okuma
    /// Bu method zaman bazlı filtreleme için kullanılır
    async fn read_by_date_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>
    ) -> Result<Vec<Document<T>>, DatabaseError> {
        let storage = self.storage.read()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire read lock for read_by_date_range: {}", e)
            })?;

        let mut result = Vec::new();
        for doc in storage.values() {
            if doc.metadata.created_at >= start && doc.metadata.created_at <= end {
                result.push(doc.clone());
            }
        }

        // Tarih sırasına göre sıralıyoruz
        result.sort_by(|a, b| a.metadata.created_at.cmp(&b.metadata.created_at));

        Ok(result)
    }

    /// Toplam döküman sayısını döndürür
    /// Bu method çok hızlı çünkü HashMap'in len() method'u O(1) complexity'de
    async fn count(&self) -> Result<usize, DatabaseError> {
        let storage = self.storage.read()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire read lock for count: {}", e)
            })?;

        Ok(storage.len())
    }

    // ================================
    // UPDATE OPERATIONS - Thread-safe döküman güncelleme
    // ================================

    /// Temel update operasyonu
    /// Mevcut dökümanın içeriğini tamamen değiştirir ve version'ını artırır
    ///
    /// INDEX ENTEGRASYONU: Update operasyonları en karmaşık index senaryosudur
    /// Çünkü hem eski hem de yeni değerlerin index'lerde tutulması gerekir:
    ///
    /// İşlem Sırası (Kritik Önem Taşır):
    /// 1. Mevcut dökümanı oku (eski index değerleri için)
    /// 2. Yeni dökümanı hazırla (yeni index değerleri için)
    /// 3. Index'lerden eski değerleri kaldır
    /// 4. Storage'da dökümanı güncelle
    /// 5. Index'lere yeni değerleri ekle
    ///
    /// Bu sequence atomic değil ama eventual consistency sağlar
    /// Production'da transaction system eklenebilir
    async fn update(&self, id: &Uuid, data: T) -> Result<Document<T>, DatabaseError> {
        // Schema validation first
        self.validate_document_data(&data)?;
        
        // Önce mevcut dökümanı oku - eski index değerleri için gerekli
        let old_document = {
            let storage = self.storage.read()
                .map_err(|e| DatabaseError::LockError {
                    reason: format!("Failed to acquire read lock for update preparation: {}", e)
                })?;

            storage.get(id).cloned()
        };

        let old_doc = old_document.ok_or_else(|| DatabaseError::DocumentNotFound {
            id: id.to_string()
        })?;

        // Yeni dökümanı hazırla
        let mut new_document = old_doc.clone();
        new_document.update(data);

        // Boyut kontrolü (yeni döküman için)
        self.validate_document_size(&new_document)?;

        // INDEX GÜNCELLEME 1: Eski değerleri index'lerden kaldır
        self.try_update_indexes_on_delete(&old_doc);

        // STORAGE GÜNCELLEME: Ana veritabanını güncelle
        {
            let mut storage = self.storage.write()
                .map_err(|e| DatabaseError::LockError {
                    reason: format!("Failed to acquire write lock for update: {}", e)
                })?;

            // Double-check: döküman hala var mı?
            if !storage.contains_key(id) {
                return Err(DatabaseError::DocumentNotFound {
                    id: id.to_string()
                });
            }

            // Güncellemeyi uygula
            storage.insert(*id, new_document.clone());
        }

        // INDEX GÜNCELLEME 2: Yeni değerleri index'lere ekle
        self.try_update_indexes_on_create(&new_document);

        Ok(new_document)
    }

    /// Optimistic locking ile güncelleme
    /// Bu method concurrent update'leri önler ve data consistency sağlar
    async fn update_with_version(&self, id: &Uuid, data: T, expected_version: u64) -> Result<Document<T>, DatabaseError> {
        // Schema validation first
        self.validate_document_data(&data)?;
        
        let mut storage = self.storage.write()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire write lock for update_with_version: {}", e)
            })?;

        match storage.get_mut(id) {
            Some(document) => {
                // Version kontrolü - optimistic locking için kritik
                if document.metadata.version != expected_version {
                    return Err(DatabaseError::VersionMismatch {
                        expected: expected_version,
                        actual: document.metadata.version,
                    });
                }

                // Yeni dökümanı oluşturup boyut kontrolü yapıyoruz
                let mut new_doc = document.clone();
                new_doc.update(data);
                self.validate_document_size(&new_doc)?;

                // Güncellemeyi uyguluyoruz
                document.update(new_doc.data);
                Ok(document.clone())
            }
            None => Err(DatabaseError::DocumentNotFound {
                id: id.to_string()
            }),
        }
    }

    /// Upsert operasyonu - döküman varsa günceller, yoksa oluşturur
    /// Bu pattern NoSQL veritabanlarında çok yaygındır
    async fn upsert(&self, id: &Uuid, data: T) -> Result<(Document<T>, bool), DatabaseError> {
        // Schema validation first
        self.validate_document_data(&data)?;
        
        let document = {
            let mut doc = Document::new(data);
            doc.metadata.id = *id; // İstenen ID'yi kullanıyoruz
            doc
        };

        self.validate_document_size(&document)?;

        let mut storage = self.storage.write()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire write lock for upsert: {}", e)
            })?;

        match storage.get_mut(id) {
            Some(existing_doc) => {
                // Mevcut dökümanı güncelliyoruz
                existing_doc.update(document.data);
                Ok((existing_doc.clone(), false)) // false = updated (not created)
            }
            None => {
                // Kapasite kontrolü sadece yeni döküman oluştururken
                self.validate_capacity(storage.len())?;

                // Yeni döküman oluşturuyoruz
                storage.insert(*id, document.clone());
                Ok((document, true)) // true = created (not updated)
            }
        }
    }

    /// Batch update operasyonu - birden fazla dökümanı atomik olarak günceller
    async fn update_batch(&self, updates: Vec<(Uuid, T)>) -> Result<Vec<Document<T>>, DatabaseError> {
        if updates.is_empty() {
            return Ok(Vec::new());
        }

        // Schema validation for all documents first
        for (_, data) in &updates {
            self.validate_document_data(data)?;
        }

        let mut storage = self.storage.write()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire write lock for update_batch: {}", e)
            })?;

        let mut results = Vec::new();

        // Önce tüm dökümanların mevcut olduğunu kontrol ediyoruz
        for (id, _) in &updates {
            if !storage.contains_key(id) {
                return Err(DatabaseError::DocumentNotFound {
                    id: id.to_string()
                });
            }
        }

        // Tüm güncellemeleri uyguluyoruz
        for (id, data) in updates {
            if let Some(document) = storage.get_mut(&id) {
                // Boyut kontrolü
                let mut temp_doc = document.clone();
                temp_doc.update(data.clone());
                self.validate_document_size(&temp_doc)?;

                // Güncellemeyi uyguluyoruz
                document.update(data);
                results.push(document.clone());
            }
        }

        Ok(results)
    }

    // ================================
    // DELETE OPERATIONS - Thread-safe döküman silme
    // ================================

    /// Temel delete operasyonu
    /// HashMap'den dökümanı kaldırır ve memory'yi serbest bırakır
    ///
    /// INDEX ENTEGRASYONU: Döküman silinmeden önce index'lerden kaldırılır
    /// Bu sequence kritik önemde:
    /// 1. Önce dökümanı storage'dan al (index güncelleme için field değerleri gerekir)
    /// 2. Index'lerden dökümanı kaldır
    /// 3. Storage'dan dökümanı sil
    ///
    /// Bu sıra neden önemli? Çünkü dökümanı önce silerseniz, index'leri güncellemek için
    /// gerekli field değerlerine erişemezsiniz. Index inconsistency oluşur.
    async fn delete(&self, id: &Uuid) -> Result<bool, DatabaseError> {
        // Önce dökümanı okumalıyız - index güncelleme için field değerleri gerekir
        let document_to_delete = {
            let storage = self.storage.read()
                .map_err(|e| DatabaseError::LockError {
                    reason: format!("Failed to acquire read lock for delete preparation: {}", e)
                })?;
            storage.get(id).cloned()
        };

        // Eğer döküman yoksa, silme işlemi zaten başarısız
        let document = match document_to_delete {
            Some(doc) => doc,
            None => return Ok(false), // Döküman zaten yok
        };

        // INDEX GÜNCELLEME: Dökümanı index'lerden kaldır (silmeden önce)
        self.try_update_indexes_on_delete(&document);

        // Şimdi gerçek silme işlemini yap
        let mut storage = self.storage.write()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire write lock for delete: {}", e)
            })?;

        // HashMap.remove() method'u dökümanın var olup olmadığını bize söylüyor
        // Double-check: döküman hala var mı? (race condition prevention)
        Ok(storage.remove(id).is_some())
    }

    /// Version kontrolü ile delete operasyonu
    /// Bu method optimistic locking kullanarak concurrent delete'leri önler
    async fn delete_with_version(&self, id: &Uuid, expected_version: u64) -> Result<bool, DatabaseError> {
        let mut storage = self.storage.write()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire write lock for delete_with_version: {}", e)
            })?;

        match storage.get(id) {
            Some(document) => {
                // Version kontrolü
                if document.metadata.version != expected_version {
                    return Err(DatabaseError::VersionMismatch {
                        expected: expected_version,
                        actual: document.metadata.version,
                    });
                }

                // Version doğru ise dökümanı siliyoruz
                storage.remove(id);
                Ok(true)
            }
            None => Ok(false), // Döküman zaten yok
        }
    }

    /// Batch delete operasyonu - birden fazla dökümanı atomik olarak siler
    async fn delete_batch(&self, ids: &[Uuid]) -> Result<usize, DatabaseError> {
        let mut storage = self.storage.write()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire write lock for delete_batch: {}", e)
            })?;

        let mut deleted_count = 0;

        for id in ids {
            if storage.remove(id).is_some() {
                deleted_count += 1;
            }
        }

        Ok(deleted_count)
    }

    /// Tarih aralığına göre döküman silme
    /// Bu method zaman bazlı cleanup operasyonları için kullanılır
    async fn delete_by_date_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>
    ) -> Result<usize, DatabaseError> {
        let mut storage = self.storage.write()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire write lock for delete_by_date_range: {}", e)
            })?;

        // Önce silinecek dökümanların ID'lerini topluyoruz
        let mut ids_to_delete = Vec::new();
        for (id, doc) in storage.iter() {
            if doc.metadata.created_at >= start && doc.metadata.created_at <= end {
                ids_to_delete.push(*id);
            }
        }

        // Dökümanları siliyoruz
        let deleted_count = ids_to_delete.len();
        for id in ids_to_delete {
            storage.remove(&id);
        }

        Ok(deleted_count)
    }

    /// Tüm dökümanları siler - tehlikeli operasyon!
    /// Production'da bu method'un kullanımı log'lanmalı ve authorize edilmeli
    async fn delete_all(&self) -> Result<usize, DatabaseError> {
        let mut storage = self.storage.write()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire write lock for delete_all: {}", e)
            })?;

        let count = storage.len();
        storage.clear(); // HashMap'i tamamen temizliyoruz

        Ok(count)
    }

    // ================================
    // UTILITY OPERATIONS - Yardımcı operasyonlar
    // ================================

    /// Dökümanın mevcut olup olmadığını kontrol eder
    /// Bu method read_by_id'ye göre daha hafif çünkü dökümanı klonlamıyor
    async fn exists(&self, id: &Uuid) -> Result<bool, DatabaseError> {
        let storage = self.storage.read()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire read lock for exists: {}", e)
            })?;

        Ok(storage.contains_key(id))
    }

    /// Storage istatistiklerini döndürür
    /// Bu method monitoring ve performance analysis için kullanılır
    async fn stats(&self) -> Result<StorageStats, DatabaseError> {
        let storage = self.storage.read()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire read lock for stats: {}", e)
            })?;

        if storage.is_empty() {
            return Ok(StorageStats {
                total_documents: 0,
                estimated_memory_usage: 0,
                oldest_document: None,
                newest_document: None,
                average_version: 0.0,
            });
        }

        let mut oldest = None;
        let mut newest = None;
        let mut total_version = 0u64;
        let mut estimated_size = 0usize;

        for doc in storage.values() {
            // En eski ve en yeni dökümanları buluyoruz
            match oldest {
                None => oldest = Some(doc.metadata.created_at),
                Some(current_oldest) => {
                    if doc.metadata.created_at < current_oldest {
                        oldest = Some(doc.metadata.created_at);
                    }
                }
            }

            match newest {
                None => newest = Some(doc.metadata.created_at),
                Some(current_newest) => {
                    if doc.metadata.created_at > current_newest {
                        newest = Some(doc.metadata.created_at);
                    }
                }
            }

            total_version += doc.metadata.version;

            // Yaklaşık memory kullanımını hesaplıyoruz
            // Bu estimation çünkü exact calculation çok pahalı olurdu
            if let Ok(serialized) = serde_json::to_vec(doc) {
                estimated_size += serialized.len();
            }
        }

        let average_version = total_version as f64 / storage.len() as f64;

        Ok(StorageStats {
            total_documents: storage.len(),
            estimated_memory_usage: estimated_size,
            oldest_document: oldest,
            newest_document: newest,
            average_version,
        })
    }
}

// ================================
// TESTLER
// ================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use tokio::time::{sleep, Duration};

    // Bu testler, CRUD operasyonlarının ve query engine'in doğru çalıştığını doğrular
    // Production sistemlerinde critical importance taşır

    #[tokio::test]
    async fn test_create_basic() {
        let db = MemoryStorage::<TestUser>::new();
        let user = TestUser::new("Ahmet Yılmaz", "ahmet@test.com", 25);

        let doc = db.create(user.clone()).await.expect("Create should succeed");

        assert_eq!(doc.data, user);
        assert_eq!(doc.metadata.version, 1);
        assert!(doc.metadata.created_at <= Utc::now());
        assert_eq!(doc.metadata.created_at, doc.metadata.updated_at);
    }

    #[tokio::test]
    async fn test_read_by_id_found() {
        let db = MemoryStorage::<TestUser>::new();
        let user = TestUser::new("Mehmet Demir", "mehmet@test.com", 28);

        let created_doc = db.create(user.clone()).await.expect("Create should succeed");
        let found_doc = db.read_by_id(&created_doc.metadata.id).await.expect("Read should succeed");

        assert!(found_doc.is_some());
        assert_eq!(found_doc.unwrap().data, user);
    }

    #[tokio::test]
    async fn test_update_success() {
        let db = MemoryStorage::<TestUser>::new();
        let original_user = TestUser::new("Orijinal İsim", "original@test.com", 25);

        let doc = db.create(original_user).await.expect("Create should succeed");
        let original_version = doc.metadata.version;

        let updated_user = TestUser::new("Güncellenmiş İsim", "updated@test.com", 26);
        let updated_doc = db.update(&doc.metadata.id, updated_user.clone()).await.expect("Update should succeed");

        assert_eq!(updated_doc.data, updated_user);
        assert_eq!(updated_doc.metadata.version, original_version + 1);
    }

    #[tokio::test]
    async fn test_delete_success() {
        let db = MemoryStorage::<TestUser>::new();
        let user = TestUser::new("Test User", "test@test.com", 25);

        let doc = db.create(user).await.expect("Create should succeed");
        let deleted = db.delete(&doc.metadata.id).await.expect("Delete should succeed");

        assert!(deleted);

        let found = db.read_by_id(&doc.metadata.id).await.expect("Read should succeed");
        assert!(found.is_none());
    }

    #[tokio::test]
    async fn test_concurrent_operations() {
        let db = Arc::new(MemoryStorage::<TestUser>::new());
        let num_threads = 5;
        let ops_per_thread = 10;

        let mut handles = Vec::new();

        for thread_id in 0..num_threads {
            let db_clone = Arc::clone(&db);
            let handle = tokio::spawn(async move {
                for i in 0..ops_per_thread {
                    let user = TestUser::new(
                        &format!("User-{}-{}", thread_id, i),
                        &format!("user{}{}@test.com", thread_id, i),
                        20 + i as u32,
                    );
                    let _doc = db_clone.create(user).await.expect("Create should succeed");
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.expect("Thread should complete successfully");
        }

        let count = db.count().await.expect("Count should succeed");
        assert_eq!(count, num_threads * ops_per_thread);
    }
}
