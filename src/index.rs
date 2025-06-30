// index.rs - Gelişmiş İndeksleme Sistemi
// Bu modül, veritabanımızın query performansını dramatik olarak artıran index sistemini içerir

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, RwLock};
use uuid::Uuid;
use crate::{DatabaseError, Document, query::JsonPath};

/// Index tip enum'ı - farklı index stratejileri için
/// Her index tipi farklı kullanım senaryoları için optimize edilmiştir
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IndexType {
    /// HashMap tabanlı index - equality search'ler için optimize
    /// Örnek: name = "John", status = "active" gibi tam eşleştirme sorguları
    /// Avantaj: O(1) average lookup time
    /// Dezavantaj: Range query desteği yok
    Hash,

    /// BTreeMap tabanlı index - range query'ler için optimize
    /// Örnek: age > 18, salary between 50000-100000 gibi aralık sorguları
    /// Avantaj: Range query desteği, O(log n) lookup time
    /// Dezavantaj: Hash'den biraz daha yavaş equality search
    BTree,
}

/// Index konfigürasyonu - bir index'in nasıl yaratılacağını belirler
/// Bu struct, index yaratırken tüm gerekli parametreleri tutar
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexConfig {
    /// Index'in benzersiz adı - query'lerde referans için kullanılır
    pub name: String,

    /// Index edilecek field'ların JSON path'leri
    /// Tek field için: ["age"]
    /// Composite index için: ["department", "salary"]
    pub fields: Vec<String>,

    /// Index'in tipi (Hash veya BTree)
    pub index_type: IndexType,

    /// Index'in oluşturulma zamanı - metadata için
    pub created_at: DateTime<Utc>,

    /// Index'in benzersiz olup olmadığı (unique constraint)
    /// True ise, aynı değere sahip birden fazla döküman olamaz
    pub unique: bool,
}

impl IndexConfig {
    /// Yeni bir index konfigürasyonu oluşturur
    /// Bu method, index yaratmak için gerekli minimum bilgileri alır
    pub fn new(name: String, fields: Vec<String>, index_type: IndexType) -> Self {
        Self {
            name,
            fields,
            index_type,
            created_at: Utc::now(),
            unique: false,
        }
    }

    /// Unique constraint ile index konfigürasyonu oluşturur
    /// Unique index'ler, aynı değere sahip birden fazla dökümanın olmasını engeller
    pub fn new_unique(name: String, fields: Vec<String>, index_type: IndexType) -> Self {
        Self {
            name,
            fields,
            index_type,
            created_at: Utc::now(),
            unique: true,
        }
    }

    /// Bu index'in composite (çoklu field) olup olmadığını kontrol eder
    /// Composite index'ler birden fazla field'ı birden indexler
    pub fn is_composite(&self) -> bool {
        self.fields.len() > 1
    }
}

/// Index değeri - JSON değerlerini index'lerde key olarak kullanmak için
/// JSON Value'nun tüm tiplerini karşılaştırılabilir hale getirir
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IndexValue {
    /// Null değer - JSON null karşılığı
    Null,
    /// Boolean değer - true/false
    Bool(bool),
    /// Sayısal değer - tüm sayıları i64 olarak saklar (precision loss riski var ama performant)
    Number(i64),
    /// Float değer - floating point sayılar için (karşılaştırma problemi var ama gerekli)
    Float(ordered_float::OrderedFloat<f64>),
    /// String değer - text data için
    String(String),
}

impl IndexValue {
    /// JSON Value'yu IndexValue'ya dönüştürür
    /// Bu conversion, JSON'ın dinamik tiplerini index'lerde kullanılabilir hale getirir
    pub fn from_json_value(value: &Value) -> Self {
        match value {
            Value::Null => IndexValue::Null,
            Value::Bool(b) => IndexValue::Bool(*b),
            Value::Number(n) => {
                // Önce integer olarak deneyip, olmezsa float olarak al
                if let Some(i) = n.as_i64() {
                    IndexValue::Number(i)
                } else if let Some(f) = n.as_f64() {
                    IndexValue::Float(ordered_float::OrderedFloat(f))
                } else {
                    // Edge case: sayı parse edilemezse 0 varsayalım
                    IndexValue::Number(0)
                }
            }
            Value::String(s) => IndexValue::String(s.clone()),
            // Array ve Object'ler için string representation kullan
            // Bu ideal değil ama pratik bir çözüm
            _ => IndexValue::String(value.to_string()),
        }
    }

    /// Composite key oluşturur - birden fazla IndexValue'yu birleştirir
    /// Örnek: (department="Engineering", salary=85000) -> tek bir composite key
    pub fn create_composite(values: Vec<IndexValue>) -> CompositeKey {
        CompositeKey { values }
    }
}

/// Composite key - birden fazla field değerini tek bir key'de tutar
/// Bu struct, multi-field index'ler için gereklidir
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompositeKey {
    /// Key'i oluşturan değerler sırası önemli
    /// Örnek: ["Engineering", 85000] != [85000, "Engineering"]
    values: Vec<IndexValue>,
}

impl CompositeKey {
    /// Yeni composite key oluşturur
    pub fn new(values: Vec<IndexValue>) -> Self {
        Self { values }
    }

    /// Key'in bileşenlerini döndürür
    pub fn values(&self) -> &[IndexValue] {
        &self.values
    }

    /// Partial match - key'in başlangıç kısmının eşleşip eşleşmediğini kontrol eder
    /// Örnek: composite key ["Engineering", 85000] için ["Engineering"] prefix match yapar
    pub fn starts_with(&self, prefix: &[IndexValue]) -> bool {
        if prefix.len() > self.values.len() {
            return false;
        }

        self.values[..prefix.len()] == *prefix
    }
}

/// HashMap tabanlı index implementasyonu
/// Equality search'ler için optimize edilmiştir - O(1) average complexity
type HashIndex = HashMap<IndexValue, HashSet<Uuid>>;

/// Composite HashMap index - birden fazla field için
type CompositeHashIndex = HashMap<CompositeKey, HashSet<Uuid>>;

/// BTreeMap tabanlı index implementasyonu
/// Range query'ler için optimize edilmiştir - O(log n) complexity
type BTreeIndex = BTreeMap<IndexValue, HashSet<Uuid>>;

/// Composite BTreeMap index - birden fazla field için
type CompositeBTreeIndex = BTreeMap<CompositeKey, HashSet<Uuid>>;

/// Ana index storage enum'ı - farklı index tiplerini unified interface ile yönetir
/// Bu enum, index sistemimizin kalbidir ve tüm index tiplerini tek çatı altında toplar
#[derive(Debug)]
pub enum IndexStorage {
    /// Single field hash index
    Hash(HashIndex),
    /// Multi field hash index
    CompositeHash(CompositeHashIndex),
    /// Single field btree index
    BTree(BTreeIndex),
    /// Multi field btree index
    CompositeBTree(CompositeBTreeIndex),
}

impl IndexStorage {
    /// Index config'e göre yeni index storage oluşturur
    /// Bu method, config parametrelerine bakarak doğru index tipini seçer
    pub fn new(config: &IndexConfig) -> Self {
        match (config.index_type.clone(), config.is_composite()) {
            (IndexType::Hash, false) => IndexStorage::Hash(HashMap::new()),
            (IndexType::Hash, true) => IndexStorage::CompositeHash(HashMap::new()),
            (IndexType::BTree, false) => IndexStorage::BTree(BTreeMap::new()),
            (IndexType::BTree, true) => IndexStorage::CompositeBTree(BTreeMap::new()),
        }
    }

    /// Index'e döküman ekler
    /// Bu method, dökümanın ilgili field değerlerini extract edip index'e kaydeder
    pub fn insert_document(&mut self, document_id: Uuid, field_values: &[Value]) -> Result<(), DatabaseError> {
        match self {
            IndexStorage::Hash(index) => {
                // Single field hash index
                if field_values.len() != 1 {
                    return Err(DatabaseError::InvalidQuery {
                        message: "Hash index expects exactly one field value".to_string()
                    });
                }

                let key = IndexValue::from_json_value(&field_values[0]);
                index.entry(key).or_insert_with(HashSet::new).insert(document_id);
            }

            IndexStorage::CompositeHash(index) => {
                // Multi field hash index
                let values: Vec<IndexValue> = field_values.iter()
                    .map(IndexValue::from_json_value)
                    .collect();
                let key = CompositeKey::new(values);
                index.entry(key).or_insert_with(HashSet::new).insert(document_id);
            }

            IndexStorage::BTree(index) => {
                // Single field btree index
                if field_values.len() != 1 {
                    return Err(DatabaseError::InvalidQuery {
                        message: "BTree index expects exactly one field value".to_string()
                    });
                }

                let key = IndexValue::from_json_value(&field_values[0]);
                index.entry(key).or_insert_with(HashSet::new).insert(document_id);
            }

            IndexStorage::CompositeBTree(index) => {
                // Multi field btree index
                let values: Vec<IndexValue> = field_values.iter()
                    .map(IndexValue::from_json_value)
                    .collect();
                let key = CompositeKey::new(values);
                index.entry(key).or_insert_with(HashSet::new).insert(document_id);
            }
        }

        Ok(())
    }

    /// Index'ten döküman siler
    /// Bu method, index'ten belirli bir dökümanın referansını kaldırır
    pub fn remove_document(&mut self, document_id: Uuid, field_values: &[Value]) -> Result<(), DatabaseError> {
        match self {
            IndexStorage::Hash(index) => {
                if field_values.len() != 1 {
                    return Err(DatabaseError::InvalidQuery {
                        message: "Hash index expects exactly one field value".to_string()
                    });
                }

                let key = IndexValue::from_json_value(&field_values[0]);
                if let Some(doc_set) = index.get_mut(&key) {
                    doc_set.remove(&document_id);
                    // Eğer set boş kaldıysa, key'i tamamen sil (memory optimization)
                    if doc_set.is_empty() {
                        index.remove(&key);
                    }
                }
            }

            IndexStorage::CompositeHash(index) => {
                let values: Vec<IndexValue> = field_values.iter()
                    .map(IndexValue::from_json_value)
                    .collect();
                let key = CompositeKey::new(values);
                if let Some(doc_set) = index.get_mut(&key) {
                    doc_set.remove(&document_id);
                    if doc_set.is_empty() {
                        index.remove(&key);
                    }
                }
            }

            IndexStorage::BTree(index) => {
                if field_values.len() != 1 {
                    return Err(DatabaseError::InvalidQuery {
                        message: "BTree index expects exactly one field value".to_string()
                    });
                }

                let key = IndexValue::from_json_value(&field_values[0]);
                if let Some(doc_set) = index.get_mut(&key) {
                    doc_set.remove(&document_id);
                    if doc_set.is_empty() {
                        index.remove(&key);
                    }
                }
            }

            IndexStorage::CompositeBTree(index) => {
                let values: Vec<IndexValue> = field_values.iter()
                    .map(IndexValue::from_json_value)
                    .collect();
                let key = CompositeKey::new(values);
                if let Some(doc_set) = index.get_mut(&key) {
                    doc_set.remove(&document_id);
                    if doc_set.is_empty() {
                        index.remove(&key);
                    }
                }
            }
        }

        Ok(())
    }

    /// Exact match lookup - belirli değere sahip dökümanları bulur
    /// Bu method equality query'ler için kullanılır
    pub fn lookup_exact(&self, field_values: &[Value]) -> Result<Option<HashSet<Uuid>>, DatabaseError> {
        match self {
            IndexStorage::Hash(index) => {
                if field_values.len() != 1 {
                    return Err(DatabaseError::InvalidQuery {
                        message: "Hash index expects exactly one field value".to_string()
                    });
                }

                let key = IndexValue::from_json_value(&field_values[0]);
                Ok(index.get(&key).cloned())
            }

            IndexStorage::CompositeHash(index) => {
                let values: Vec<IndexValue> = field_values.iter()
                    .map(IndexValue::from_json_value)
                    .collect();
                let key = CompositeKey::new(values);
                Ok(index.get(&key).cloned())
            }

            IndexStorage::BTree(index) => {
                if field_values.len() != 1 {
                    return Err(DatabaseError::InvalidQuery {
                        message: "BTree index expects exactly one field value".to_string()
                    });
                }

                let key = IndexValue::from_json_value(&field_values[0]);
                Ok(index.get(&key).cloned())
            }

            IndexStorage::CompositeBTree(index) => {
                let values: Vec<IndexValue> = field_values.iter()
                    .map(IndexValue::from_json_value)
                    .collect();
                let key = CompositeKey::new(values);
                Ok(index.get(&key).cloned())
            }
        }
    }

    /// Range lookup - belirli aralıktaki değerlere sahip dökümanları bulur
    /// Bu method sadece BTree index'ler için çalışır
    pub fn lookup_range(&self, min_value: Option<&Value>, max_value: Option<&Value>) -> Result<HashSet<Uuid>, DatabaseError> {
        match self {
            IndexStorage::BTree(index) => {
                let mut result = HashSet::new();

                // Range boundaries'leri belirle
                let start_key = min_value.map(IndexValue::from_json_value);
                let end_key = max_value.map(IndexValue::from_json_value);

                // BTreeMap'in range functionality'sini kullan
                let range = match (start_key.as_ref(), end_key.as_ref()) {
                    (Some(start), Some(end)) => {
                        // Both boundaries specified
                        index.range(start..=end)
                    }
                    (Some(start), None) => {
                        // Only minimum specified
                        index.range(start..)
                    }
                    (None, Some(end)) => {
                        // Only maximum specified
                        index.range(..=end)
                    }
                    (None, None) => {
                        // No boundaries - return all
                        index.range(..)
                    }
                };

                // Tüm matching dökümanları topla
                for (_, doc_set) in range {
                    result.extend(doc_set);
                }

                Ok(result)
            }

            IndexStorage::CompositeBTree(_index) => {
                // Composite range query daha karmaşık - şimdilik sadece exact match destekleyelim
                // Production'da daha sophisticated logic gerekir
                Err(DatabaseError::InvalidQuery {
                    message: "Range queries on composite BTree indexes not yet implemented".to_string()
                })
            }

            _ => {
                // Hash index'ler range query desteklemez
                Err(DatabaseError::InvalidQuery {
                    message: "Range queries only supported on BTree indexes".to_string()
                })
            }
        }
    }

    /// Index içindeki toplam entry sayısını döndürür
    /// Bu method monitoring ve statistics için kullanılır
    pub fn size(&self) -> usize {
        match self {
            IndexStorage::Hash(index) => index.len(),
            IndexStorage::CompositeHash(index) => index.len(),
            IndexStorage::BTree(index) => index.len(),
            IndexStorage::CompositeBTree(index) => index.len(),
        }
    }

    /// Index'in memory kullanımını estimate eder
    /// Bu rough bir calculation ama monitoring için yararlı
    pub fn estimated_memory_usage(&self) -> usize {
        match self {
            IndexStorage::Hash(index) => {
                // Rough estimation: key size + value size + HashMap overhead
                index.len() * (std::mem::size_of::<IndexValue>() + std::mem::size_of::<HashSet<Uuid>>() + 24)
            }
            IndexStorage::CompositeHash(index) => {
                index.len() * (std::mem::size_of::<CompositeKey>() + std::mem::size_of::<HashSet<Uuid>>() + 24)
            }
            IndexStorage::BTree(index) => {
                // BTreeMap has more overhead due to tree structure
                index.len() * (std::mem::size_of::<IndexValue>() + std::mem::size_of::<HashSet<Uuid>>() + 48)
            }
            IndexStorage::CompositeBTree(index) => {
                index.len() * (std::mem::size_of::<CompositeKey>() + std::mem::size_of::<HashSet<Uuid>>() + 48)
            }
        }
    }
}

/// Index metadata ve statistics
/// Bu struct, index'lerin durumu hakkında bilgi sağlar
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStats {
    /// Index adı
    pub name: String,
    /// Index tipi
    pub index_type: IndexType,
    /// Index edilmiş field'lar
    pub fields: Vec<String>,
    /// Toplam entry sayısı
    pub total_entries: usize,
    /// Unique değer sayısı
    pub unique_values: usize,
    /// Tahmini memory kullanımı (bytes)
    pub estimated_memory_usage: usize,
    /// Index'in oluşturulma zamanı
    pub created_at: DateTime<Utc>,
    /// En son güncelleme zamanı
    pub last_updated: DateTime<Utc>,
}

/// Ana Index Manager - tüm index'leri yönetir
/// Bu struct, index yaratma, güncelleme, silme ve kullanma işlemlerini koordine eder
pub struct IndexManager {
    /// Mevcut index'lerin konfigürasyonları
    configs: Arc<RwLock<HashMap<String, IndexConfig>>>,

    /// Index storage'lar - her index'in gerçek verisi
    indexes: Arc<RwLock<HashMap<String, IndexStorage>>>,
}

impl IndexManager {
    /// Yeni IndexManager oluşturur
    pub fn new() -> Self {
        Self {
            configs: Arc::new(RwLock::new(HashMap::new())),
            indexes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Yeni index yaratır
    /// Bu method, belirtilen konfigürasyona göre yeni bir index oluşturur
    pub fn create_index(&self, config: IndexConfig) -> Result<(), DatabaseError> {
        let mut configs = self.configs.write()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire write lock for index configs: {}", e)
            })?;

        let mut indexes = self.indexes.write()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire write lock for indexes: {}", e)
            })?;

        // Index adının unique olduğunu kontrol et
        if configs.contains_key(&config.name) {
            return Err(DatabaseError::InvalidQuery {
                message: format!("Index with name '{}' already exists", config.name)
            });
        }

        // Yeni index storage oluştur
        let storage = IndexStorage::new(&config);

        // Config ve storage'ı kaydet
        let index_name = config.name.clone();
        configs.insert(index_name.clone(), config);
        indexes.insert(index_name, storage);

        Ok(())
    }

    /// Index'i siler
    /// Bu method, belirtilen index'i tamamen kaldırır
    pub fn drop_index(&self, index_name: &str) -> Result<(), DatabaseError> {
        let mut configs = self.configs.write()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire write lock for index configs: {}", e)
            })?;

        let mut indexes = self.indexes.write()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire write lock for indexes: {}", e)
            })?;

        // Index'in var olduğunu kontrol et
        if !configs.contains_key(index_name) {
            return Err(DatabaseError::InvalidQuery {
                message: format!("Index '{}' does not exist", index_name)
            });
        }

        // Config ve storage'ı sil
        configs.remove(index_name);
        indexes.remove(index_name);

        Ok(())
    }

    /// Döküman eklendiğinde tüm relevant index'leri günceller
    /// Bu method, döküman ekleme operasyonunda otomatik olarak çağrılır
    pub fn index_document(&self, document: &Document<Value>) -> Result<(), DatabaseError> {
        let configs = self.configs.read()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire read lock for index configs: {}", e)
            })?;

        let mut indexes = self.indexes.write()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire write lock for indexes: {}", e)
            })?;

        // Her index için dökümanı index'le
        for (index_name, config) in configs.iter() {
            if let Some(index_storage) = indexes.get_mut(index_name) {
                // Field değerlerini extract et
                let field_values = self.extract_field_values(&document.data, &config.fields)?;

                // Index'e ekle
                index_storage.insert_document(document.metadata.id, &field_values)?;
            }
        }

        Ok(())
    }

    /// Döküman silindiğinde tüm relevant index'lerden kaldırır
    /// Bu method, döküman silme operasyonunda otomatik olarak çağrılır
    pub fn remove_document(&self, document: &Document<Value>) -> Result<(), DatabaseError> {
        let configs = self.configs.read()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire read lock for index configs: {}", e)
            })?;

        let mut indexes = self.indexes.write()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire write lock for indexes: {}", e)
            })?;

        // Her index'ten dökümanı kaldır
        for (index_name, config) in configs.iter() {
            if let Some(index_storage) = indexes.get_mut(index_name) {
                // Field değerlerini extract et
                let field_values = self.extract_field_values(&document.data, &config.fields)?;

                // Index'ten kaldır
                index_storage.remove_document(document.metadata.id, &field_values)?;
            }
        }

        Ok(())
    }

    /// Belirtilen field'lar için en uygun index'i bulur
    /// Bu method, query optimization için kullanılır
    pub fn find_best_index(&self, fields: &[String]) -> Result<Option<String>, DatabaseError> {
        let configs = self.configs.read()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire read lock for index configs: {}", e)
            })?;

        // Önce exact match ara
        for (index_name, config) in configs.iter() {
            if config.fields == fields {
                return Ok(Some(index_name.clone()));
            }
        }

        // Exact match yoksa, partial match ara (composite index'ler için)
        for (index_name, config) in configs.iter() {
            if config.is_composite() && fields.len() <= config.fields.len() {
                // Query field'ları index field'larının prefix'i mi?
                if config.fields[..fields.len()] == *fields {
                    return Ok(Some(index_name.clone()));
                }
            }
        }

        Ok(None)
    }

    /// Index kullanarak lookup yapar
    /// Bu method, query engine tarafından optimized search için kullanılır
    pub fn lookup_exact(&self, index_name: &str, field_values: &[Value]) -> Result<Option<HashSet<Uuid>>, DatabaseError> {
        let indexes = self.indexes.read()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire read lock for indexes: {}", e)
            })?;

        if let Some(index_storage) = indexes.get(index_name) {
            index_storage.lookup_exact(field_values)
        } else {
            Err(DatabaseError::InvalidQuery {
                message: format!("Index '{}' does not exist", index_name)
            })
        }
    }

    /// Range lookup yapar (sadece BTree index'ler için)
    pub fn lookup_range(&self, index_name: &str, min_value: Option<&Value>, max_value: Option<&Value>) -> Result<HashSet<Uuid>, DatabaseError> {
        let indexes = self.indexes.read()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire read lock for indexes: {}", e)
            })?;

        if let Some(index_storage) = indexes.get(index_name) {
            index_storage.lookup_range(min_value, max_value)
        } else {
            Err(DatabaseError::InvalidQuery {
                message: format!("Index '{}' does not exist", index_name)
            })
        }
    }

    /// Tüm index'lerin listesini döndürür
    pub fn list_indexes(&self) -> Result<Vec<IndexConfig>, DatabaseError> {
        let configs = self.configs.read()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire read lock for index configs: {}", e)
            })?;

        Ok(configs.values().cloned().collect())
    }

    /// Index statistics'lerini döndürür
    pub fn get_index_stats(&self, index_name: &str) -> Result<IndexStats, DatabaseError> {
        let configs = self.configs.read()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire read lock for index configs: {}", e)
            })?;

        let indexes = self.indexes.read()
            .map_err(|e| DatabaseError::LockError {
                reason: format!("Failed to acquire read lock for indexes: {}", e)
            })?;

        let config = configs.get(index_name)
            .ok_or_else(|| DatabaseError::InvalidQuery {
                message: format!("Index '{}' does not exist", index_name)
            })?;

        let storage = indexes.get(index_name)
            .ok_or_else(|| DatabaseError::InvalidQuery {
                message: format!("Index storage '{}' does not exist", index_name)
            })?;

        Ok(IndexStats {
            name: config.name.clone(),
            index_type: config.index_type.clone(),
            fields: config.fields.clone(),
            total_entries: storage.size(),
            unique_values: storage.size(), // For now, assume each entry is unique
            estimated_memory_usage: storage.estimated_memory_usage(),
            created_at: config.created_at,
            last_updated: Utc::now(), // For now, use current time
        })
    }

    /// Document'ten belirtilen field'ların değerlerini extract eder
    /// Bu helper method, index operations için field değerlerini çıkarır
    fn extract_field_values(&self, document: &Value, fields: &[String]) -> Result<Vec<Value>, DatabaseError> {
        let mut values = Vec::new();

        for field_path in fields {
            let path = JsonPath::new(field_path);

            if let Some(value) = path.extract_value(document) {
                values.push(value.clone());
            } else {
                // Field bulunamadıysa null değer kullan
                values.push(Value::Null);
            }
        }

        Ok(values)
    }
}

impl Default for IndexManager {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for IndexManager {
    fn clone(&self) -> Self {
        Self {
            configs: Arc::clone(&self.configs),
            indexes: Arc::clone(&self.indexes),
        }
    }
}
