// query.rs - Ayrı bir modül olarak ekleyelim
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use crate::{Document, DatabaseError, CrudDatabase};
use uuid::Uuid;

/// JSON path'leri parse etmek için basit bir sistem
/// Örnek: "profile.bio" → ["profile", "bio"]
/// Örnek: "interests[0]" → ["interests", "0"]
#[derive(Debug, Clone, PartialEq)]
pub struct JsonPath {
    segments: Vec<String>,
}

impl JsonPath {
    /// String'den JsonPath oluşturur
    /// Basit implementasyon - production'da daha sophisticated parser gerekir
    pub fn new(path: &str) -> Self {
        let segments = path
            .split('.')
            .flat_map(|segment| {
                // Array syntax'ını handle et: "interests[0]" → ["interests", "0"]
                if segment.contains('[') && segment.contains(']') {
                    let parts: Vec<&str> = segment.split('[').collect();
                    let main_part = parts[0];
                    let index_part = parts[1].trim_end_matches(']');
                    vec![main_part.to_string(), index_part.to_string()]
                } else {
                    vec![segment.to_string()]
                }
            })
            .filter(|s| !s.is_empty())
            .collect();

        Self { segments }
    }

    /// JSON Value'dan path'e göre değer çıkarır
    /// Bu method JSON'ın derinliğinde gezinir
    /// Lifetime parametresi ile input ve output'un lifetime'ını bağlıyoruz
    pub fn extract_value<'a>(&self, value: &'a Value) -> Option<&'a Value> {
        let mut current = value;

        for segment in &self.segments {
            match current {
                Value::Object(obj) => {
                    // Object field'larında ara
                    current = obj.get(segment)?;
                }
                Value::Array(arr) => {
                    // Array index'lerinde ara
                    if let Ok(index) = segment.parse::<usize>() {
                        current = arr.get(index)?;
                    } else {
                        return None;
                    }
                }
                _ => return None, // Primitive değerlerde daha derine gidemeyiz
            }
        }

        Some(current)
    }

    /// Path'in string representation'ı
    pub fn as_string(&self) -> String {
        self.segments.join(".")
    }
}

/// Query için kullanılacak karşılaştırma operatörleri
/// Her operatör farklı bir filtreleme türünü temsil eder
#[derive(Debug, Clone, PartialEq)]
pub enum ComparisonOperator {
    Equal,              // ==
    NotEqual,           // !=
    GreaterThan,        // >
    GreaterThanOrEqual, // >=
    LessThan,           // <
    LessThanOrEqual,    // <=
    Contains,           // String içinde arama (case-insensitive)
    StartsWith,         // String ile başlama
    EndsWith,           // String ile bitme
    In,                 // Array içinde bulunma
    NotIn,              // Array içinde bulunmama
    Exists,             // Field'ın var olup olmadığı
    NotExists,          // Field'ın var olmadığı
}

/// Tek bir filtreleme koşulunu temsil eder
/// Örnek: "age > 25" → WhereClause { path: "age", operator: GreaterThan, value: 25 }
#[derive(Debug, Clone)]
pub struct WhereClause {
    pub path: JsonPath,
    pub operator: ComparisonOperator,
    pub value: Value,
}

impl WhereClause {
    /// Bir dökümanın bu koşulu sağlayıp sağlamadığını kontrol eder
    /// Bu method'un iç mekanizması query engine'in kalbidir
    pub fn matches(&self, document: &Value) -> bool {
        match self.operator {
            ComparisonOperator::Exists => {
                self.path.extract_value(document).is_some()
            }
            ComparisonOperator::NotExists => {
                self.path.extract_value(document).is_none()
            }
            _ => {
                // Diğer operatörler için field'ın mevcut olması gerekir
                if let Some(field_value) = self.path.extract_value(document) {
                    self.compare_values(field_value, &self.value)
                } else {
                    false // Field yoksa koşul sağlanmaz
                }
            }
        }
    }

    /// İki JSON değeri arasında karşılaştırma yapar
    /// JSON'ın dinamik tipi nedeniyle oldukça karmaşık bir işlemdir
    fn compare_values(&self, left: &Value, right: &Value) -> bool {
        match self.operator {
            ComparisonOperator::Equal => left == right,
            ComparisonOperator::NotEqual => left != right,

            // Sayısal karşılaştırmalar
            ComparisonOperator::GreaterThan => {
                self.compare_numbers(left, right, |a, b| a > b)
            }
            ComparisonOperator::GreaterThanOrEqual => {
                self.compare_numbers(left, right, |a, b| a >= b)
            }
            ComparisonOperator::LessThan => {
                self.compare_numbers(left, right, |a, b| a < b)
            }
            ComparisonOperator::LessThanOrEqual => {
                self.compare_numbers(left, right, |a, b| a <= b)
            }

            // String işlemleri
            ComparisonOperator::Contains => {
                if let (Some(left_str), Some(right_str)) = (left.as_str(), right.as_str()) {
                    left_str.to_lowercase().contains(&right_str.to_lowercase())
                } else {
                    false
                }
            }
            ComparisonOperator::StartsWith => {
                if let (Some(left_str), Some(right_str)) = (left.as_str(), right.as_str()) {
                    left_str.to_lowercase().starts_with(&right_str.to_lowercase())
                } else {
                    false
                }
            }
            ComparisonOperator::EndsWith => {
                if let (Some(left_str), Some(right_str)) = (left.as_str(), right.as_str()) {
                    left_str.to_lowercase().ends_with(&right_str.to_lowercase())
                } else {
                    false
                }
            }

            // Array işlemleri
            ComparisonOperator::In => {
                if let Value::Array(arr) = right {
                    arr.contains(left)
                } else {
                    false
                }
            }
            ComparisonOperator::NotIn => {
                if let Value::Array(arr) = right {
                    !arr.contains(left)
                } else {
                    true
                }
            }

            // Bu operatörler yukarıda handle edildi
            ComparisonOperator::Exists | ComparisonOperator::NotExists => false,
        }
    }

    /// Sayısal değerleri karşılaştırır - JSON'da hem integer hem float olabilir
    fn compare_numbers<F>(&self, left: &Value, right: &Value, compare_fn: F) -> bool
    where
        F: Fn(f64, f64) -> bool,
    {
        match (left, right) {
            // Her iki taraf da sayı ise
            (Value::Number(l), Value::Number(r)) => {
                if let (Some(l_float), Some(r_float)) = (l.as_f64(), r.as_f64()) {
                    compare_fn(l_float, r_float)
                } else {
                    false
                }
            }
            _ => false, // Sayısal karşılaştırma sadece sayılar arasında yapılabilir
        }
    }
}

/// Sıralama direction'ı
#[derive(Debug, Clone, PartialEq)]
pub enum SortDirection {
    Ascending,  // Küçükten büyüğe (A-Z, 1-9)
    Descending, // Büyükten küçüğe (Z-A, 9-1)
}

/// Sıralama kriteri
/// Birden fazla field'a göre sıralama yapabilmek için
#[derive(Debug, Clone)]
pub struct SortClause {
    pub path: JsonPath,
    pub direction: SortDirection,
}

/// Projection - hangi field'ların döndürüleceğini belirler
/// SQL'deki SELECT field1, field2 FROM ... gibi
#[derive(Debug, Clone)]
pub enum ProjectionType {
    /// Tüm field'ları döndür
    All,
    /// Sadece belirtilen field'ları döndür
    Include(Vec<JsonPath>),
    /// Belirtilen field'lar hariç hepsini döndür
    Exclude(Vec<JsonPath>),
}

/// Ana Query struct'ı - tüm query bilgilerini tutar
/// Bu struct immutable'dır, her değişiklik yeni bir instance oluşturur
#[derive(Debug, Clone)]
pub struct Query {
    where_clauses: Vec<WhereClause>,
    sort_clauses: Vec<SortClause>,
    projection: ProjectionType,
    limit: Option<usize>,
    offset: Option<usize>,
}

impl Query {
    /// Boş bir query oluşturur
    pub fn new() -> Self {
        Self {
            where_clauses: Vec::new(),
            sort_clauses: Vec::new(),
            projection: ProjectionType::All,
            limit: None,
            offset: None,
        }
    }

    /// Filtreleme koşulu ekler
    pub fn where_clause(mut self, path: &str, operator: ComparisonOperator, value: Value) -> Self {
        self.where_clauses.push(WhereClause {
            path: JsonPath::new(path),
            operator,
            value,
        });
        self
    }

    /// Convenience method'lar - daha kolay kullanım için
    pub fn where_eq(self, path: &str, value: Value) -> Self {
        self.where_clause(path, ComparisonOperator::Equal, value)
    }

    pub fn where_gt(self, path: &str, value: Value) -> Self {
        self.where_clause(path, ComparisonOperator::GreaterThan, value)
    }

    pub fn where_lt(self, path: &str, value: Value) -> Self {
        self.where_clause(path, ComparisonOperator::LessThan, value)
    }

    pub fn where_contains(self, path: &str, value: &str) -> Self {
        self.where_clause(path, ComparisonOperator::Contains, Value::String(value.to_string()))
    }

    pub fn where_exists(self, path: &str) -> Self {
        self.where_clause(path, ComparisonOperator::Exists, Value::Null)
    }

    /// Sıralama ekler
    pub fn sort_by(mut self, path: &str, direction: SortDirection) -> Self {
        self.sort_clauses.push(SortClause {
            path: JsonPath::new(path),
            direction,
        });
        self
    }

    /// Convenience method'lar
    pub fn sort_asc(self, path: &str) -> Self {
        self.sort_by(path, SortDirection::Ascending)
    }

    pub fn sort_desc(self, path: &str) -> Self {
        self.sort_by(path, SortDirection::Descending)
    }

    /// Projection ayarlar
    pub fn select(mut self, fields: Vec<&str>) -> Self {
        let paths = fields.into_iter().map(|f| JsonPath::new(f)).collect();
        self.projection = ProjectionType::Include(paths);
        self
    }

    pub fn exclude(mut self, fields: Vec<&str>) -> Self {
        let paths = fields.into_iter().map(|f| JsonPath::new(f)).collect();
        self.projection = ProjectionType::Exclude(paths);
        self
    }

    /// Limit ayarlar
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Offset ayarlar
    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Bir dökümanın tüm where koşullarını sağlayıp sağlamadığını kontrol eder
    /// Tüm koşulların AND ile bağlandığını varsayar
    pub fn matches_document(&self, document: &Value) -> bool {
        self.where_clauses.iter().all(|clause| clause.matches(document))
    }

    /// Döküman listesini bu query'ye göre filtreler
    pub fn apply_filters(&self, documents: Vec<Value>) -> Vec<Value> {
        documents
            .into_iter()
            .filter(|doc| self.matches_document(doc))
            .collect()
    }

    /// Döküman listesini sıralar
    pub fn apply_sorting(&self, mut documents: Vec<Value>) -> Vec<Value> {
        if self.sort_clauses.is_empty() {
            return documents;
        }

        // Multi-field sorting - önce son kriterden başlayarak sıralar
        // Böylece en önemli kriter en son uygulanır ve öncelikli olur
        documents.sort_by(|a, b| {
            for sort_clause in &self.sort_clauses {
                let a_value = sort_clause.path.extract_value(a);
                let b_value = sort_clause.path.extract_value(b);

                let comparison = match (a_value, b_value) {
                    (Some(a_val), Some(b_val)) => self.compare_json_values(a_val, b_val),
                    (Some(_), None) => Ordering::Less,    // Değer var vs yok
                    (None, Some(_)) => Ordering::Greater, // Yok vs değer var
                    (None, None) => Ordering::Equal,      // İkisi de yok
                };

                // Direction'a göre sıralamayı ters çevir
                let final_comparison = match sort_clause.direction {
                    SortDirection::Ascending => comparison,
                    SortDirection::Descending => comparison.reverse(),
                };

                // Eğer bu kriterde fark varsa, sonucu döndür
                if final_comparison != Ordering::Equal {
                    return final_comparison;
                }
                // Eşitse bir sonraki kritere geç
            }

            Ordering::Equal // Tüm kriterlerde eşit
        });

        documents
    }

    /// İki JSON değerini karşılaştırır - sıralama için
    fn compare_json_values(&self, a: &Value, b: &Value) -> Ordering {
        match (a, b) {
            // Sayılar
            (Value::Number(a_num), Value::Number(b_num)) => {
                let a_float = a_num.as_f64().unwrap_or(0.0);
                let b_float = b_num.as_f64().unwrap_or(0.0);
                a_float.partial_cmp(&b_float).unwrap_or(Ordering::Equal)
            }
            // String'ler
            (Value::String(a_str), Value::String(b_str)) => a_str.cmp(b_str),
            // Boolean'lar
            (Value::Bool(a_bool), Value::Bool(b_bool)) => a_bool.cmp(b_bool),
            // Tip farklılıkları - consistent ordering için
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            // Diğer tip kombinasyonları için string'e çevirip karşılaştır
            _ => a.to_string().cmp(&b.to_string()),
        }
    }

    /// Projection uygular - hangi field'ların döndürüleceğini belirler
    pub fn apply_projection(&self, documents: Vec<Value>) -> Vec<Value> {
        match &self.projection {
            ProjectionType::All => documents,
            ProjectionType::Include(paths) => {
                documents
                    .into_iter()
                    .map(|doc| self.project_include(&doc, paths))
                    .collect()
            }
            ProjectionType::Exclude(paths) => {
                documents
                    .into_iter()
                    .map(|doc| self.project_exclude(&doc, paths))
                    .collect()
            }
        }
    }

    /// Sadece belirtilen field'ları içeren yeni JSON oluşturur
    fn project_include(&self, document: &Value, paths: &[JsonPath]) -> Value {
        let mut result = serde_json::Map::new();

        for path in paths {
            if let Some(value) = path.extract_value(document) {
                // Basit implementasyon - sadece top-level field'ları destekler
                // Production'da nested path'leri de desteklemek gerekir
                if path.segments.len() == 1 {
                    result.insert(path.segments[0].clone(), value.clone());
                }
            }
        }

        Value::Object(result)
    }

    /// Belirtilen field'lar hariç tüm field'ları içeren yeni JSON oluşturur
    fn project_exclude(&self, document: &Value, paths: &[JsonPath]) -> Value {
        if let Value::Object(obj) = document {
            let exclude_keys: HashSet<String> = paths
                .iter()
                .filter(|path| path.segments.len() == 1) // Sadece top-level
                .map(|path| path.segments[0].clone())
                .collect();

            let filtered: serde_json::Map<String, Value> = obj
                .iter()
                .filter(|(key, _)| !exclude_keys.contains(*key))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();

            Value::Object(filtered)
        } else {
            document.clone() // Object değilse olduğu gibi döndür
        }
    }

    /// Sayfalama uygular
    pub fn apply_pagination(&self, documents: Vec<Value>) -> Vec<Value> {
        let start = self.offset.unwrap_or(0);
        let end = match self.limit {
            Some(limit) => start + limit,
            None => documents.len(),
        };

        documents
            .into_iter()
            .skip(start)
            .take(end.saturating_sub(start))
            .collect()
    }
}

/// QueryBuilder - fluent API için ana interface
/// Bu struct database instance'ını tutar ve query'yi execute etme yeteneği sağlar
///
/// LIFETIME CONSTRAINT: T: 'static bound eklendi çünkü index optimization sırasında
/// runtime type checking yapıyoruz. Bu constraint, Any trait'inin gerektirdiği
/// 'static lifetime guarantee'sini sağlar.
pub struct QueryBuilder<T> {
    database: std::sync::Arc<T>,
    query: Query,
}

impl<T> QueryBuilder<T>
where
    T: CrudDatabase<Value> + Send + Sync + 'static,
{
    /// Yeni QueryBuilder oluşturur
    pub fn new(database: std::sync::Arc<T>) -> Self {
        Self {
            database,
            query: Query::new(),
        }
    }

    /// WHERE koşulu ekler - fluent interface
    pub fn where_field(mut self, path: &str, operator: ComparisonOperator, value: Value) -> Self {
        self.query = self.query.where_clause(path, operator, value);
        self
    }

    /// Convenience method'lar - daha kolay kullanım
    pub fn where_eq(mut self, path: &str, value: Value) -> Self {
        self.query = self.query.where_eq(path, value);
        self
    }

    pub fn where_gt(mut self, path: &str, value: Value) -> Self {
        self.query = self.query.where_gt(path, value);
        self
    }

    pub fn where_lt(mut self, path: &str, value: Value) -> Self {
        self.query = self.query.where_lt(path, value);
        self
    }

    pub fn where_contains(mut self, path: &str, value: &str) -> Self {
        self.query = self.query.where_contains(path, value);
        self
    }

    pub fn where_exists(mut self, path: &str) -> Self {
        self.query = self.query.where_exists(path);
        self
    }

    /// Sıralama ekler
    pub fn sort_by(mut self, path: &str, direction: SortDirection) -> Self {
        self.query = self.query.sort_by(path, direction);
        self
    }

    pub fn sort_asc(mut self, path: &str) -> Self {
        self.query = self.query.sort_asc(path);
        self
    }

    pub fn sort_desc(mut self, path: &str) -> Self {
        self.query = self.query.sort_desc(path);
        self
    }

    /// Projection
    pub fn select(mut self, fields: Vec<&str>) -> Self {
        self.query = self.query.select(fields);
        self
    }

    pub fn exclude(mut self, fields: Vec<&str>) -> Self {
        self.query = self.query.exclude(fields);
        self
    }

    /// Sayfalama
    pub fn limit(mut self, limit: usize) -> Self {
        self.query = self.query.limit(limit);
        self
    }

    pub fn offset(mut self, offset: usize) -> Self {
        self.query = self.query.offset(offset);
        self
    }

    /// Query'yi execute eder - asenkron işlem
    /// Bu method tüm query pipeline'ını çalıştırır
    ///
    /// INDEX OPTIMIZATION: Bu method artık mümkün olduğunda index kullanır
    /// Query Planner Logic'i:
    /// 1. Query'yi analiz et (hangi field'lar filter ediliyor?)
    /// 2. En uygun index'i ara (exact match > partial match > no index)
    /// 3. Index varsa kullán (O(1) veya O(log n)), yoksa full scan yap (O(n))
    /// 4. Sonuçları post-process et (sorting, projection, pagination)
    ///
    /// Bu optimization, büyük veri setlerinde dramatic performance improvement sağlar
    pub async fn execute(self) -> Result<Vec<Value>, DatabaseError> {
        // INDEX OPTIMIZATION: Query'yi index kullanarak optimize etmeye çalış
        if let Some(optimized_results) = self.try_execute_with_index().await? {
            log::info!("Query executed using index optimization");
            return Ok(optimized_results);
        }

        // Index kullanılamadıysa, fallback: traditional full scan
        log::info!("Query executed using full table scan (no suitable index found)");

        // 1. Tüm dökümanları veritabanından al
        let all_documents = self.database.read_all(None, None).await?;

        // 2. Document wrapper'ları Value'lara çevir
        let values: Vec<Value> = all_documents
            .into_iter()
            .map(|doc| doc.data)
            .collect();

        // 3. Query pipeline'ını uygula
        let filtered = self.query.apply_filters(values);
        let sorted = self.query.apply_sorting(filtered);
        let projected = self.query.apply_projection(sorted);
        let paginated = self.query.apply_pagination(projected);

        Ok(paginated)
    }

    /// Query'yi execute eder ve sonuçları Document wrapper'ı ile döndürür
    /// Metadata bilgisi gerekliyse bu method kullanılır
    pub async fn execute_with_metadata(self) -> Result<Vec<Document<Value>>, DatabaseError> {
        // Önce ID'leri bul
        let filtered_ids = self.get_matching_document_ids().await?;

        // Sonra bu ID'lere göre tam dökümanları al
        let documents = self.database.read_by_ids(&filtered_ids).await?;

        // Sıralama ve projection'ı metadata ile birlikte uygula
        // Bu daha karmaşık ama metadata'yı korur
        Ok(documents)
    }

    /// Filtreleme koşullarını sağlayan döküman ID'lerini bulur
    async fn get_matching_document_ids(&self) -> Result<Vec<Uuid>, DatabaseError> {
        let all_documents = self.database.read_all(None, None).await?;

        let matching_ids: Vec<Uuid> = all_documents
            .into_iter()
            .filter(|doc| self.query.matches_document(&doc.data))
            .map(|doc| doc.metadata.id)
            .collect();

        Ok(matching_ids)
    }

    /// Count query - sadece eşleşen döküman sayısını döndürür
    /// Büyük veri setlerinde performans için önemli
    pub async fn count(self) -> Result<usize, DatabaseError> {
        let all_documents = self.database.read_all(None, None).await?;

        let count = all_documents
            .iter()
            .filter(|doc| self.query.matches_document(&doc.data))
            .count();

        Ok(count)
    }

    /// Query'nin var olan bir dökümanı sağlayıp sağlamadığını kontrol eder
    pub async fn exists(self) -> Result<bool, DatabaseError> {
        let count = self.count().await?;
        Ok(count > 0)
    }

    // ================================
    // INDEX OPTIMIZATION METHODS
    // ================================

    /// Index kullanarak query execute etmeye çalışır
    ///
    /// Bu method query planner'ın kalbidir. Şu logic'i takip eder:
    /// 1. Query'deki WHERE clause'ları analiz et
    /// 2. Bu clause'ları destekleyebilecek index'leri ara
    /// 3. En uygun index'i seç (exact match > partial match > range support)
    /// 4. Index'i kullanarak candidate dökümanları bul
    /// 5. Remaining filters'ı uygula
    /// 6. Sort, project ve paginate et
    ///
    /// Return: Some(results) if index kullanılabildi, None if full scan gerekli
    async fn try_execute_with_index(&self) -> Result<Option<Vec<Value>>, DatabaseError> {
        // Query analysis: hangi field'lar üzerinde equality filter var?
        let equality_filters = self.extract_equality_filters();

        if equality_filters.is_empty() {
            // Equality filter yoksa, range query olabilir mi kontrol et
            return self.try_execute_with_range_index().await;
        }

        // Index manager'dan en uygun index'i ara
        let index_manager = self.get_index_manager_if_available()?;
        let field_names: Vec<String> = equality_filters.keys().cloned().collect();

        if let Some(index_name) = index_manager.find_best_index(&field_names)? {
            log::info!("Found suitable index '{}' for fields: {:?}", index_name, field_names);

            // Index kullanarak candidate dökümanları bul
            let field_values: Vec<Value> = field_names.iter()
                .map(|field| equality_filters[field].clone())
                .collect();

            if let Some(candidate_ids) = index_manager.lookup_exact(&index_name, &field_values)? {
                log::info!("Index lookup returned {} candidate documents", candidate_ids.len());

                // Candidate dökümanları al
                let candidate_docs = self.database.read_by_ids(&candidate_ids.into_iter().collect::<Vec<_>>()).await?;

                // Document'ları Value'lara çevir
                let values: Vec<Value> = candidate_docs.into_iter().map(|doc| doc.data).collect();

                // Remaining filters'ı uygula (index'te olmayan diğer WHERE clause'lar)
                let filtered = self.apply_remaining_filters(values, &equality_filters);
                let sorted = self.query.apply_sorting(filtered);
                let projected = self.query.apply_projection(sorted);
                let paginated = self.query.apply_pagination(projected);

                return Ok(Some(paginated));
            }
        }

        Ok(None) // Uygun index bulunamadı
    }

    /// Range query'ler için index kullanmaya çalışır
    /// Bu method BTree index'leri kullanarak range operasyonları optimize eder
    /// Örnek: age > 25, salary BETWEEN 50000 AND 100000
    async fn try_execute_with_range_index(&self) -> Result<Option<Vec<Value>>, DatabaseError> {
        // Range operasyonları (>, <, >=, <=) ara
        let range_filters = self.extract_range_filters();

        if range_filters.is_empty() {
            return Ok(None);
        }

        let index_manager = self.get_index_manager_if_available()?;

        // Her range filter için uygun index ara
        for (field_name, (min_val, max_val)) in range_filters {
            if let Some(index_name) = index_manager.find_best_index(&[field_name.clone()])? {
                log::info!("Found range index '{}' for field: {}", index_name, field_name);

                // Range lookup yap
                if let Ok(candidate_ids) = index_manager.lookup_range(&index_name, min_val.as_ref(), max_val.as_ref()) {
                    log::info!("Range index lookup returned {} candidate documents", candidate_ids.len());

                    // Candidate dökümanları al ve process et
                    let candidate_docs = self.database.read_by_ids(&candidate_ids.into_iter().collect::<Vec<_>>()).await?;
                    let values: Vec<Value> = candidate_docs.into_iter().map(|doc| doc.data).collect();

                    // Full filtering uygula (range index sadece bir field'ı optimize eder)
                    let filtered = self.query.apply_filters(values);
                    let sorted = self.query.apply_sorting(filtered);
                    let projected = self.query.apply_projection(sorted);
                    let paginated = self.query.apply_pagination(projected);

                    return Ok(Some(paginated));
                }
            }
        }

        Ok(None) // Uygun range index bulunamadı
    }

    /// Query'deki equality filters'ı extract eder
    /// Örnek: name = "John", age = 25 -> {"name": "John", "age": 25}
    fn extract_equality_filters(&self) -> HashMap<String, Value> {
        let mut filters = HashMap::new();

        for clause in &self.query.where_clauses {
            if clause.operator == ComparisonOperator::Equal {
                // Sadece single-field path'leri destekliyoruz şimdilik
                if clause.path.segments.len() == 1 {
                    filters.insert(clause.path.segments[0].clone(), clause.value.clone());
                }
            }
        }

        filters
    }

    /// Query'deki range filters'ı extract eder
    /// Return: HashMap<field_name, (min_value, max_value)>
    /// min_value veya max_value None olabilir (unbounded range için)
    fn extract_range_filters(&self) -> HashMap<String, (Option<Value>, Option<Value>)> {
        let mut ranges: HashMap<String, (Option<Value>, Option<Value>)> = HashMap::new();

        for clause in &self.query.where_clauses {
            if clause.path.segments.len() == 1 {
                let field_name = &clause.path.segments[0];
                let entry = ranges.entry(field_name.clone()).or_insert((None, None));

                match clause.operator {
                    ComparisonOperator::GreaterThan | ComparisonOperator::GreaterThanOrEqual => {
                        // Minimum değer set et
                        entry.0 = Some(clause.value.clone());
                    }
                    ComparisonOperator::LessThan | ComparisonOperator::LessThanOrEqual => {
                        // Maximum değer set et
                        entry.1 = Some(clause.value.clone());
                    }
                    _ => {} // Diğer operatörler range değil
                }
            }
        }

        // Sadece en az bir boundary'si olan field'ları döndür
        ranges.into_iter()
            .filter(|(_, (min, max))| min.is_some() || max.is_some())
            .collect()
    }

    /// Equality filter'lardan sonra kalan filter'ları uygular
    /// Bu method, index ile pre-filter edilmiş dökümanlar üzerinde
    /// remaining WHERE clause'ları execute eder
    fn apply_remaining_filters(&self, documents: Vec<Value>, applied_filters: &HashMap<String, Value>) -> Vec<Value> {
        documents.into_iter().filter(|doc| {
            // Her WHERE clause'ı kontrol et
            for clause in &self.query.where_clauses {
                // Bu clause zaten index'te uygulandı mı?
                if clause.operator == ComparisonOperator::Equal
                   && clause.path.segments.len() == 1
                   && applied_filters.contains_key(&clause.path.segments[0]) {
                    continue; // Bu filter zaten uygulandı, skip et
                }

                // Diğer filter'ları uygula
                if !clause.matches(doc) {
                    return false;
                }
            }
            true
        }).collect()
    }

    /// Index manager'a erişim sağlar (sadece MemoryStorage<Value> için)
    /// Bu method, type safety sağlayarak sadece JSON Value tiplerinde index kullanımına izin verir
    ///
    /// **ANA DÜZELTME:** Arc içindeki actual object'e erişmek için as_ref() kullanıyoruz
    /// Bu çok kritik! Çünkü:
    /// - self.database tipi: Arc<MemoryStorage<Value>>
    /// - &self.database as &dyn Any: &Arc<MemoryStorage<Value>> tipine cast ediyor
    /// - Ama biz &MemoryStorage<Value> tipini arıyoruz
    /// - self.database.as_ref(): Arc içindeki actual object'e referans verir
    fn get_index_manager_if_available(&self) -> Result<std::sync::Arc<crate::index::IndexManager>, DatabaseError> {
        // CRITIAL FIX: Arc wrapper'ın içindekilere erişim için as_ref() kullanın
        // Bu satır Arc<MemoryStorage<Value>>'dan MemoryStorage<Value>'ya reference sağlar
        let any_db = self.database.as_ref() as &dyn std::any::Any;

        if let Some(memory_storage) = any_db.downcast_ref::<crate::MemoryStorage<Value>>() {
            Ok(memory_storage.get_index_manager())
        } else {
            Err(DatabaseError::InvalidQuery {
                message: "Index operations only available for MemoryStorage<Value>".to_string()
            })
        }
    }
}

/// Database trait'ine query method'u eklemek için extension
/// Bu trait'e gerekli tüm bound'ları ekliyoruz
pub trait QueryableDatabase<T>: CrudDatabase<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync,
{
    fn query(self: std::sync::Arc<Self>) -> QueryBuilder<Self>
    where
        Self: Sized + Send + Sync;
}

/// MemoryStorage için Value tipine özel implementation
/// Bu approach generic lifetime ve trait bound problemlerini çözer
/// QueryBuilder zaten CrudDatabase<Value> bekliyor, bu yüzden Value için spesifik impl mantıklı
impl QueryableDatabase<serde_json::Value> for crate::MemoryStorage<serde_json::Value> {
    fn query(self: std::sync::Arc<Self>) -> QueryBuilder<Self> {
        QueryBuilder::new(self)
    }
}
