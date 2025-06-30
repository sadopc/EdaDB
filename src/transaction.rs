// transaction.rs - ACID Transaction System with MVCC
// Bu modül modern veritabanı sistemlerinin transaction management katmanını implement eder
// ACID properties, MVCC, isolation levels ve deadlock detection içerir

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock, Mutex};
use std::time::{Duration, Instant};
use uuid::Uuid;
use crate::{Document, DatabaseError, MemoryStorage};

/// Transaction ID - her transaction'ın benzersiz kimliği
pub type TransactionId = Uuid;

/// Transaction Version - MVCC için version tracking
pub type Version = u64;

/// Lock Type - farklı lock türleri
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LockType {
    /// Shared lock - Read operasyonları için
    /// Birden fazla transaction aynı resource'u shared mode'da lock'layabilir
    Shared,

    /// Exclusive lock - Write operasyonları için
    /// Sadece tek transaction exclusive lock alabilir
    Exclusive,

    /// Intent Shared - Hierarchical locking için
    /// Bir parent resource'un child'larında shared lock alınacağını belirtir
    IntentShared,

    /// Intent Exclusive - Hierarchical locking için
    /// Bir parent resource'un child'larında exclusive lock alınacağını belirtir
    IntentExclusive,
}

/// Lock Mode Compatibility Matrix
/// Bu matrix hangi lock'ların birlikte kullanılabileceğini belirler
impl LockType {
    /// İki lock type'ının compatible olup olmadığını kontrol eder
    pub fn is_compatible_with(&self, other: &LockType) -> bool {
        use LockType::*;
        match (self, other) {
            // Shared locks birbirleriyle compatible
            (Shared, Shared) => true,
            (Shared, IntentShared) => true,
            (IntentShared, Shared) => true,
            (IntentShared, IntentShared) => true,

            // Exclusive locks hiçbir şeyle compatible değil
            (Exclusive, _) => false,
            (_, Exclusive) => false,

            // Intent Exclusive sadece Intent Shared ile compatible
            (IntentExclusive, IntentShared) => true,
            (IntentShared, IntentExclusive) => true,
            (IntentExclusive, IntentExclusive) => false,

            // Eksik kombinasyonlar - Shared ve IntentExclusive
            (Shared, IntentExclusive) => false,
            (IntentExclusive, Shared) => false,
        }
    }

    /// Lock'ın upgrade edilip edilemeyeceğini kontrol eder
    /// CRITICAL FIX: Aynı lock tipini tekrar almaya da izin ver (re-entrant locking)
    /// MODERN FIX: Lock subsumption desteği - güçlü lock zayıf lock'ı kapsar
    pub fn can_upgrade_to(&self, target: &LockType) -> bool {
        use LockType::*;
        match (self, target) {
            // Re-entrant locking: Aynı lock tipini tekrar almaya izin ver
            (Shared, Shared) => true,
            (Exclusive, Exclusive) => true,
            (IntentShared, IntentShared) => true,
            (IntentExclusive, IntentExclusive) => true,

            // Lock Upgrades: Zayıf → Güçlü
            (Shared, Exclusive) => true,
            (IntentShared, IntentExclusive) => true,
            (IntentShared, Exclusive) => true,

            // CRITICAL FIX: Lock Subsumption - Güçlü lock zayıf lock'ı kapsar
            // Bu modern database sistemlerinin standard davranışıdır
            (Exclusive, Shared) => true,        // Exclusive zaten read yapabilir
            (Exclusive, IntentShared) => true,  // Exclusive > IntentShared
            (IntentExclusive, Shared) => true,  // IntentExclusive > Shared
            (IntentExclusive, IntentShared) => true, // IntentExclusive > IntentShared

            _ => false,
        }
    }

    /// Bir lock'ın başka bir lock'ı kapsayıp kapsamadığını kontrol eder
    /// Bu method lock subsumption logic'ini centralize eder
    pub fn subsumes(&self, other: &LockType) -> bool {
        use LockType::*;
        match (self, other) {
            // Her lock kendini kapsar
            (Shared, Shared) => true,
            (Exclusive, Exclusive) => true,
            (IntentShared, IntentShared) => true,
            (IntentExclusive, IntentExclusive) => true,

            // Exclusive her şeyi kapsar
            (Exclusive, Shared) => true,
            (Exclusive, IntentShared) => true,
            (Exclusive, IntentExclusive) => true,

            // IntentExclusive bazı lock'ları kapsar
            (IntentExclusive, Shared) => true,
            (IntentExclusive, IntentShared) => true,

            // Diğer durumlar
            _ => false,
        }
    }
}

/// Resource Identifier - lock'lanacak resource'ları tanımlar
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ResourceId {
    /// Döküman level lock
    Document(Uuid),
    /// Index level lock
    Index(String),
    /// Table level lock (tüm storage)
    Table,
    /// Row range lock (gelecek özellik için)
    Range(String, Value, Value),
}

/// Lock Request - lock talebi
#[derive(Debug, Clone)]
pub struct LockRequest {
    pub transaction_id: TransactionId,
    pub resource_id: ResourceId,
    pub lock_type: LockType,
    pub requested_at: DateTime<Utc>,
    pub timeout: Option<Duration>,
}

/// Granted Lock - verilmiş lock
#[derive(Debug, Clone)]
pub struct GrantedLock {
    pub transaction_id: TransactionId,
    pub resource_id: ResourceId,
    pub lock_type: LockType,
    pub granted_at: DateTime<Utc>,
}

/// Transaction Status
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionStatus {
    /// Transaction başlatıldı ama henüz commit/rollback yapılmadı
    Active,
    /// Transaction commit edildi
    Committed,
    /// Transaction rollback edildi
    Aborted,
    /// Transaction prepare phase'inde (2PC için)
    Prepared,
}

/// Transaction Isolation Levels
/// SQL standard'ına uygun isolation levels
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IsolationLevel {
    /// En düşük isolation - dirty reads, non-repeatable reads, phantom reads mümkün
    ReadUncommitted,
    /// Dirty reads engellenir ama non-repeatable reads ve phantom reads mümkün
    ReadCommitted,
    /// Dirty reads ve non-repeatable reads engellenir ama phantom reads mümkün
    RepeatableRead,
    /// En yüksek isolation - tüm anomaliler engellenir
    Serializable,
}

/// MVCC Version Entry - her döküman versiyonu için
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VersionEntry {
    /// Version number - artan sequence
    pub version: Version,
    /// Bu versiyonu yaratan transaction
    pub created_by: TransactionId,
    /// Bu versiyonu silen transaction (eğer silinmişse)
    pub deleted_by: Option<TransactionId>,
    /// Version'ın oluşturulma zamanı
    pub created_at: DateTime<Utc>,
    /// Döküman verisi
    pub data: Value,
    /// Bu version committed mi?
    pub is_committed: bool,
}

/// Transaction Context - her transaction'ın durumu
#[derive(Debug, Clone)]
pub struct TransactionContext {
    /// Transaction ID
    pub id: TransactionId,
    /// Transaction başlangıç zamanı
    pub start_time: DateTime<Utc>,
    /// Transaction status
    pub status: TransactionStatus,
    /// Isolation level
    pub isolation_level: IsolationLevel,
    /// Transaction timeout
    pub timeout: Option<Duration>,
    /// Read timestamp (MVCC için)
    pub read_timestamp: Version,
    /// Write timestamp (MVCC için)
    pub write_timestamp: Version,
    /// Acquired locks
    pub acquired_locks: HashSet<ResourceId>,
    /// Modified documents (rollback için)
    pub modified_documents: HashMap<Uuid, Value>,
    /// Created documents (rollback için)
    pub created_documents: HashSet<Uuid>,
    /// Deleted documents (rollback için)
    pub deleted_documents: HashMap<Uuid, Value>,
}

impl TransactionContext {
    pub fn new(isolation_level: IsolationLevel, timeout: Option<Duration>) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            start_time: now,
            status: TransactionStatus::Active,
            isolation_level,
            timeout,
            read_timestamp: 0,
            write_timestamp: 0,
            acquired_locks: HashSet::new(),
            modified_documents: HashMap::new(),
            created_documents: HashSet::new(),
            deleted_documents: HashMap::new(),
        }
    }

    /// Transaction'ın timeout olup olmadığını kontrol eder
    pub fn is_timed_out(&self) -> bool {
        if let Some(timeout) = self.timeout {
            Utc::now().signed_duration_since(self.start_time).to_std().unwrap_or(Duration::ZERO) > timeout
        } else {
            false
        }
    }
}

/// Lock Manager - lock'ları yönetir ve deadlock detection yapar
pub struct LockManager {
    /// Granted locks - resource'a göre organize
    granted_locks: Arc<RwLock<HashMap<ResourceId, Vec<GrantedLock>>>>,

    /// Waiting queue - her resource için bekleyen lock talepleri
    waiting_queue: Arc<RwLock<HashMap<ResourceId, VecDeque<LockRequest>>>>,

    /// Wait-for graph - deadlock detection için
    /// Key: waiting transaction, Value: transactions it's waiting for
    wait_for_graph: Arc<RwLock<HashMap<TransactionId, HashSet<TransactionId>>>>,

    /// Lock timeout duration
    default_lock_timeout: Duration,
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            granted_locks: Arc::new(RwLock::new(HashMap::new())),
            waiting_queue: Arc::new(RwLock::new(HashMap::new())),
            wait_for_graph: Arc::new(RwLock::new(HashMap::new())),
            default_lock_timeout: Duration::from_secs(60), // 60 saniye timeout
        }
    }

    /// Lock alma talebi
    /// Bu method lock compatibility check'i yapar ve deadlock detection çalıştırır
    pub async fn acquire_lock(
        &self,
        transaction_id: TransactionId,
        resource_id: ResourceId,
        lock_type: LockType,
        timeout: Option<Duration>,
    ) -> Result<(), DatabaseError> {
        let start_time = Instant::now();
        let timeout_duration = timeout.unwrap_or(self.default_lock_timeout);

        loop {
            // Timeout kontrolü
            if start_time.elapsed() > timeout_duration {
                return Err(DatabaseError::TransactionError {
                    message: format!("Lock acquisition timeout for transaction {}", transaction_id)
                });
            }

            // Lock uyumluluğunu kontrol et
            if self.can_grant_lock(&transaction_id, &resource_id, &lock_type)? {
                // Lock'ı ver
                self.grant_lock(transaction_id, resource_id, lock_type).await?;
                return Ok(());
            } else {
                // Wait queue'ya ekle
                self.add_to_wait_queue(transaction_id, resource_id.clone(), lock_type.clone(), timeout).await?;

                // Deadlock detection
                if self.detect_deadlock(&transaction_id)? {
                    // Deadlock tespit edildi - transaction'ı abort et
                    self.remove_from_wait_queue(&transaction_id, &resource_id).await?;
                    return Err(DatabaseError::TransactionError {
                        message: format!("Deadlock detected for transaction {}", transaction_id)
                    });
                }

                // Kısa süre bekle ve tekrar dene
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }

    /// Lock'ın verilebilip verilemeyeceğini kontrol eder
    /// CRITICAL FIX: Re-entrant locking desteği eklendi
    /// MODERN FIX: Lock subsumption desteği - production database behavior
    fn can_grant_lock(
        &self,
        transaction_id: &TransactionId,
        resource_id: &ResourceId,
        lock_type: &LockType,
    ) -> Result<bool, DatabaseError> {
        let granted_locks = self.granted_locks.read().map_err(|e| {
            DatabaseError::LockError {
                reason: format!("Failed to read granted locks: {}", e)
            }
        })?;

        if let Some(existing_locks) = granted_locks.get(resource_id) {
            for existing_lock in existing_locks {
                // CRITICAL FIX: Aynı transaction'ın lock'ı ise
                if existing_lock.transaction_id == *transaction_id {
                    // 1. Aynı lock tipini tekrar istiyorsa doğrudan izin ver (re-entrant)
                    if existing_lock.lock_type == *lock_type {
                        log::debug!("Re-entrant lock granted for transaction {} on resource {:?}",
                                   transaction_id, resource_id);
                        return Ok(true);
                    }

                    // 2. MODERN FIX: Lock subsumption kontrolü
                    // Eğer mevcut lock istenen lock'ı kapsıyorsa (örneğin Exclusive varken Shared isteniyorsa)
                    // bu isteği redundant kabul et ve grant et
                    if existing_lock.lock_type.subsumes(lock_type) {
                        log::debug!("Lock subsumption: {:?} subsumes {:?} for transaction {} on resource {:?}",
                                   existing_lock.lock_type, lock_type, transaction_id, resource_id);
                        return Ok(true);
                    }

                    // 3. Gerçek upgrade durumu (zayıf lock'tan güçlü lock'a geçiş)
                    return Ok(existing_lock.lock_type.can_upgrade_to(lock_type));
                }

                // Farklı transaction'ın lock'ı ise compatibility kontrolü
                if !lock_type.is_compatible_with(&existing_lock.lock_type) {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }

    /// Lock'ı ver
    /// OPTIMIZATION FIX: Lock subsumption durumunda duplicate lock ekleme
    /// RUST FIX: Ownership-aware logging strategy
    async fn grant_lock(
        &self,
        transaction_id: TransactionId,
        resource_id: ResourceId,
        lock_type: LockType,
    ) -> Result<(), DatabaseError> {
        // RUST LESSON: Logging'i move operations'tan ÖNCE yap
        // Bu sayede ownership problemi olmaz ve debugging daha kolay olur
        log::debug!("Attempting to grant lock: {:?} for transaction {} on resource {:?}",
                   lock_type, transaction_id, resource_id);

        let mut granted_locks = self.granted_locks.write().map_err(|e| {
            DatabaseError::LockError {
                reason: format!("Failed to write granted locks: {}", e)
            }
        })?;

        // CRITICAL FIX: Lock subsumption ve re-entrant lock kontrolü
        // Eğer bu transaction için bu resource'ta zaten bir lock varsa:
        if let Some(existing_locks) = granted_locks.get(&resource_id) {
            for existing_lock in existing_locks {
                if existing_lock.transaction_id == transaction_id {
                    // 1. Aynı lock zaten var - duplicate ekleme
                    if existing_lock.lock_type == lock_type {
                        log::debug!("Lock already exists, skipping duplicate grant");
                        return Ok(());
                    }

                    // 2. Mevcut lock istenen lock'ı kapsıyor - subsumption case
                    if existing_lock.lock_type.subsumes(&lock_type) {
                        log::debug!("Lock subsumption in effect: {:?} subsumes {:?}",
                                   existing_lock.lock_type, lock_type);
                        return Ok(());
                    }

                    // 3. Bu gerçek bir upgrade case - mevcut lock'ı güncelle
                    // Production database'lerde bu genellikle in-place upgrade yapılır
                    // Şimdilik basit approach: yeni lock ekle, eski lock kalsın
                    // (Production'da bu daha sophisticated optimize edilir)
                    log::debug!("Lock upgrade detected: {:?} -> {:?}",
                               existing_lock.lock_type, lock_type);
                    break;
                }
            }
        }

        // RUST LESSON: Clone'u sadece gereken yerde kullan
        // resource_id'yi clone'luyoruz çünkü hem key olarak hem value'da kullanılacak
        let resource_id_for_entry = resource_id.clone();

        // Yeni lock ekle (gerçek grant case)
        let granted_lock = GrantedLock {
            transaction_id,
            resource_id,  // Burada resource_id move ediliyor
            lock_type,    // Burada lock_type move ediliyor
            granted_at: Utc::now(),
        };

        granted_locks.entry(resource_id_for_entry).or_insert_with(Vec::new).push(granted_lock);

        // Success logging - artık move edilmiş değerleri kullanamayız
        log::debug!("New lock granted successfully for transaction {}", transaction_id);

        Ok(())
    }

    /// Wait queue'ya ekle
    async fn add_to_wait_queue(
        &self,
        transaction_id: TransactionId,
        resource_id: ResourceId,
        lock_type: LockType,
        timeout: Option<Duration>,
    ) -> Result<(), DatabaseError> {
        let mut waiting_queue = self.waiting_queue.write().map_err(|e| {
            DatabaseError::LockError {
                reason: format!("Failed to write waiting queue: {}", e)
            }
        })?;

        let lock_request = LockRequest {
            transaction_id,
            resource_id: resource_id.clone(),
            lock_type,
            requested_at: Utc::now(),
            timeout,
        };

        waiting_queue.entry(resource_id.clone()).or_insert_with(VecDeque::new).push_back(lock_request);

        drop(waiting_queue); // Erken drop

        // Wait-for graph'ı güncelle
        self.update_wait_for_graph(&transaction_id, &resource_id)?;

        Ok(())
    }

    /// Wait-for graph'ı güncelle - deadlock detection için
    fn update_wait_for_graph(
        &self,
        waiting_transaction: &TransactionId,
        resource_id: &ResourceId,
    ) -> Result<(), DatabaseError> {
        let granted_locks = self.granted_locks.read().map_err(|e| {
            DatabaseError::LockError {
                reason: format!("Failed to read granted locks for wait-for graph: {}", e)
            }
        })?;

        let mut wait_for_graph = self.wait_for_graph.write().map_err(|e| {
            DatabaseError::LockError {
                reason: format!("Failed to write wait-for graph: {}", e)
            }
        })?;

        // Bu resource'u hold eden transaction'ları bul
        if let Some(existing_locks) = granted_locks.get(resource_id) {
            let waiting_for = wait_for_graph.entry(*waiting_transaction).or_insert_with(HashSet::new);

            for lock in existing_locks {
                if lock.transaction_id != *waiting_transaction {
                    waiting_for.insert(lock.transaction_id);
                }
            }
        }

        Ok(())
    }

    /// Deadlock detection - DFS algoritması kullanarak cycle detection
    fn detect_deadlock(&self, transaction_id: &TransactionId) -> Result<bool, DatabaseError> {
        let wait_for_graph = self.wait_for_graph.read().map_err(|e| {
            DatabaseError::LockError {
                reason: format!("Failed to read wait-for graph for deadlock detection: {}", e)
            }
        })?;

        let mut visited = HashSet::new();
        let mut recursion_stack = HashSet::new();

        self.has_cycle_dfs(&wait_for_graph, transaction_id, &mut visited, &mut recursion_stack)
    }

    /// DFS ile cycle detection
    fn has_cycle_dfs(
        &self,
        graph: &HashMap<TransactionId, HashSet<TransactionId>>,
        node: &TransactionId,
        visited: &mut HashSet<TransactionId>,
        recursion_stack: &mut HashSet<TransactionId>,
    ) -> Result<bool, DatabaseError> {
        visited.insert(*node);
        recursion_stack.insert(*node);

        if let Some(neighbors) = graph.get(node) {
            for neighbor in neighbors {
                if !visited.contains(neighbor) {
                    if self.has_cycle_dfs(graph, neighbor, visited, recursion_stack)? {
                        return Ok(true);
                    }
                } else if recursion_stack.contains(neighbor) {
                    // Back edge found - cycle detected
                    return Ok(true);
                }
            }
        }

        recursion_stack.remove(node);
        Ok(false)
    }

    /// Wait queue'dan kaldır
    async fn remove_from_wait_queue(
        &self,
        transaction_id: &TransactionId,
        resource_id: &ResourceId,
    ) -> Result<(), DatabaseError> {
        let mut waiting_queue = self.waiting_queue.write().map_err(|e| {
            DatabaseError::LockError {
                reason: format!("Failed to write waiting queue for removal: {}", e)
            }
        })?;

        if let Some(queue) = waiting_queue.get_mut(resource_id) {
            queue.retain(|req| req.transaction_id != *transaction_id);
            if queue.is_empty() {
                waiting_queue.remove(resource_id);
            }
        }

        drop(waiting_queue); // Erken drop

        // Wait-for graph'tan da kaldır
        let mut wait_for_graph = self.wait_for_graph.write().map_err(|e| {
            DatabaseError::LockError {
                reason: format!("Failed to write wait-for graph for removal: {}", e)
            }
        })?;

        wait_for_graph.remove(transaction_id);

        // Diğer transaction'ların wait-for listelerinden de kaldır
        for waiting_for_set in wait_for_graph.values_mut() {
            waiting_for_set.remove(transaction_id);
        }

        Ok(())
    }

    /// Transaction'ın tüm lock'larını serbest bırak
    pub async fn release_all_locks(&self, transaction_id: &TransactionId) -> Result<(), DatabaseError> {
        // Granted locks'tan kaldır
        let mut granted_locks = self.granted_locks.write().map_err(|e| {
            DatabaseError::LockError {
                reason: format!("Failed to write granted locks for release: {}", e)
            }
        })?;

        let mut resources_to_check = Vec::new();

        for (resource_id, locks) in granted_locks.iter_mut() {
            locks.retain(|lock| {
                if lock.transaction_id == *transaction_id {
                    resources_to_check.push(resource_id.clone());
                    false
                } else {
                    true
                }
            });
        }

        // Boş kalan resource entry'lerini temizle
        granted_locks.retain(|_, locks| !locks.is_empty());

        drop(granted_locks); // Lock'ı serbest bırak

        // Wait queue'dan kaldır
        for resource_id in &resources_to_check {
            self.remove_from_wait_queue(transaction_id, resource_id).await?;
        }

        // Waiting transaction'ları kontrol et ve grant et
        self.process_waiting_queue(&resources_to_check).await?;

        Ok(())
    }

    /// Waiting queue'daki transaction'ları işle
    async fn process_waiting_queue(&self, released_resources: &[ResourceId]) -> Result<(), DatabaseError> {
        for resource_id in released_resources {
            // İlk lock'ı al
            let request_to_grant = {
                let mut waiting_queue = self.waiting_queue.write().map_err(|e| {
                    DatabaseError::LockError {
                        reason: format!("Failed to write waiting queue for processing: {}", e)
                    }
                })?;

                if let Some(queue) = waiting_queue.get_mut(resource_id) {
                    if let Some(request) = queue.front() {
                        if self.can_grant_lock(&request.transaction_id, &request.resource_id, &request.lock_type)? {
                            queue.pop_front()
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            };

            // Grant işlemi (lock dışında)
            if let Some(request) = request_to_grant {
                self.grant_lock(request.transaction_id, request.resource_id, request.lock_type).await?;
            }
        }

        Ok(())
    }
}

/// Version Manager - MVCC version tracking
pub struct VersionManager {
    /// Global version counter
    global_version: Arc<Mutex<Version>>,

    /// Document versions - her döküman için version history
    document_versions: Arc<RwLock<HashMap<Uuid, Vec<VersionEntry>>>>,
}

impl VersionManager {
    pub fn new() -> Self {
        Self {
            global_version: Arc::new(Mutex::new(0)),
            document_versions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Yeni version al
    pub fn next_version(&self) -> Result<Version, DatabaseError> {
        let mut global_version = self.global_version.lock().map_err(|e| {
            DatabaseError::LockError {
                reason: format!("Failed to acquire version lock: {}", e)
            }
        })?;

        *global_version += 1;
        Ok(*global_version)
    }

    /// Döküman version'ını ekle
    pub fn add_version(
        &self,
        document_id: Uuid,
        version: Version,
        transaction_id: TransactionId,
        data: Value,
        is_delete: bool,
    ) -> Result<(), DatabaseError> {
        let mut document_versions = self.document_versions.write().map_err(|e| {
            DatabaseError::LockError {
                reason: format!("Failed to write document versions: {}", e)
            }
        })?;

        let version_entry = VersionEntry {
            version,
            created_by: transaction_id,
            deleted_by: if is_delete { Some(transaction_id) } else { None },
            created_at: Utc::now(),
            data,
            is_committed: false, // Transaction commit olunca true yapılacak
        };

        document_versions.entry(document_id).or_insert_with(Vec::new).push(version_entry);

        Ok(())
    }

    /// MVCC read - belirli timestamp'te dökümanın görünür versiyonunu bul
    pub fn read_version(
        &self,
        document_id: &Uuid,
        read_timestamp: Version,
        transaction_id: &TransactionId,
    ) -> Result<Option<Value>, DatabaseError> {
        let document_versions = self.document_versions.read().map_err(|e| {
            DatabaseError::LockError {
                reason: format!("Failed to read document versions: {}", e)
            }
        })?;

        if let Some(versions) = document_versions.get(document_id) {
            // En uygun version'ı bul (read_timestamp'ten küçük en büyük committed version)
            let mut best_version: Option<&VersionEntry> = None;

            for version_entry in versions {
                // Bu transaction'ın kendi uncommitted write'ları görünür
                if version_entry.created_by == *transaction_id {
                    if version_entry.deleted_by.is_some() {
                        return Ok(None); // Bu transaction dökümanı silmiş
                    } else {
                        best_version = Some(version_entry);
                        continue;
                    }
                }

                // Committed version'lar ve timestamp kontrolü
                if version_entry.is_committed && version_entry.version <= read_timestamp {
                    if version_entry.deleted_by.is_some() {
                        return Ok(None); // Döküman silinmiş
                    }

                    match best_version {
                        None => best_version = Some(version_entry),
                        Some(current_best) => {
                            if version_entry.version > current_best.version {
                                best_version = Some(version_entry);
                            }
                        }
                    }
                }
            }

            Ok(best_version.map(|v| v.data.clone()))
        } else {
            Ok(None)
        }
    }

    /// Transaction commit - version'ları committed olarak işaretle
    pub fn commit_transaction(&self, transaction_id: &TransactionId) -> Result<(), DatabaseError> {
        let mut document_versions = self.document_versions.write().map_err(|e| {
            DatabaseError::LockError {
                reason: format!("Failed to write document versions for commit: {}", e)
            }
        })?;

        for versions in document_versions.values_mut() {
            for version_entry in versions {
                if version_entry.created_by == *transaction_id {
                    version_entry.is_committed = true;
                }
            }
        }

        Ok(())
    }

    /// Transaction rollback - uncommitted version'ları kaldır
    pub fn rollback_transaction(&self, transaction_id: &TransactionId) -> Result<(), DatabaseError> {
        let mut document_versions = self.document_versions.write().map_err(|e| {
            DatabaseError::LockError {
                reason: format!("Failed to write document versions for rollback: {}", e)
            }
        })?;

        for versions in document_versions.values_mut() {
            versions.retain(|version_entry| version_entry.created_by != *transaction_id);
        }

        // Boş kalan document entry'lerini temizle
        document_versions.retain(|_, versions| !versions.is_empty());

        Ok(())
    }

    /// Version cleanup - eski version'ları temizle
    pub fn cleanup_old_versions(&self, cutoff_version: Version) -> Result<usize, DatabaseError> {
        let mut document_versions = self.document_versions.write().map_err(|e| {
            DatabaseError::LockError {
                reason: format!("Failed to write document versions for cleanup: {}", e)
            }
        })?;

        let mut cleaned_count = 0;

        for versions in document_versions.values_mut() {
            let original_len = versions.len();

            // En son version'ı her zaman sakla, diğerleri için cutoff kontrolü
            if versions.len() > 1 {
                let last_version = versions.last().cloned();
                versions.retain(|v| v.version > cutoff_version || last_version.as_ref().map(|lv| lv == v).unwrap_or(false));
                cleaned_count += original_len - versions.len();
            }
        }

        Ok(cleaned_count)
    }
}

/// Transaction Manager - Ana transaction coordinator
pub struct TransactionManager {
    /// Active transactions
    active_transactions: Arc<RwLock<HashMap<TransactionId, TransactionContext>>>,

    /// Lock manager
    lock_manager: Arc<LockManager>,

    /// Version manager - MVCC için
    version_manager: Arc<VersionManager>,

    /// Default timeout
    default_timeout: Duration,

    /// WAL manager reference (persistence için)
    wal_manager: Option<Arc<crate::persistence::WalManager>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
            lock_manager: Arc::new(LockManager::new()),
            version_manager: Arc::new(VersionManager::new()),
            default_timeout: Duration::from_secs(600), // 10 dakika default
            wal_manager: None,
        }
    }

    /// WAL manager'ı set et
    pub fn set_wal_manager(&mut self, wal_manager: Arc<crate::persistence::WalManager>) {
        self.wal_manager = Some(wal_manager);
    }

    /// Transaction başlat
    pub async fn begin_transaction(
        &self,
        isolation_level: IsolationLevel,
        timeout: Option<Duration>,
    ) -> Result<TransactionId, DatabaseError> {
        let mut context = TransactionContext::new(
            isolation_level.clone(),
            timeout.or(Some(self.default_timeout))
        );

        // Read timestamp ata (MVCC için)
        context.read_timestamp = self.version_manager.next_version()?;

        let transaction_id = context.id;

        // WAL'a transaction begin yaz
        if let Some(ref wal_manager) = self.wal_manager {
            wal_manager.write_entry(crate::persistence::WalEntryType::TransactionBegin {
                transaction_id,
            }).await?;
        }

        // Active transaction'lara ekle
        let mut active_transactions = self.active_transactions.write().map_err(|e| {
            DatabaseError::LockError {
                reason: format!("Failed to write active transactions: {}", e)
            }
        })?;

        active_transactions.insert(transaction_id, context);

        log::info!("Transaction started: {} with isolation level {:?}",
                  transaction_id, isolation_level);

        Ok(transaction_id)
    }

    /// Transaction commit
    pub async fn commit_transaction(&self, transaction_id: &TransactionId) -> Result<(), DatabaseError> {
        // Transaction context'i al
        let mut active_transactions = self.active_transactions.write().map_err(|e| {
            DatabaseError::LockError {
                reason: format!("Failed to write active transactions for commit: {}", e)
            }
        })?;

        let mut context = active_transactions.remove(transaction_id)
            .ok_or_else(|| DatabaseError::TransactionError {
                message: format!("Transaction {} not found or already completed", transaction_id)
            })?;

        // Timeout kontrolü
        if context.is_timed_out() {
            drop(active_transactions); // Lock'ı serbest bırak
            self.abort_transaction_internal(transaction_id, context).await?;
            return Err(DatabaseError::TransactionError {
                message: format!("Transaction {} timed out", transaction_id)
            });
        }

        // Status güncelle
        context.status = TransactionStatus::Committed;

        drop(active_transactions); // Lock'ı serbest bırak

        // Version'ları commit et
        self.version_manager.commit_transaction(transaction_id)?;

        // WAL'a commit yaz
        if let Some(ref wal_manager) = self.wal_manager {
            wal_manager.write_entry(crate::persistence::WalEntryType::TransactionCommit {
                transaction_id: *transaction_id,
            }).await?;
        }

        // Lock'ları serbest bırak
        self.lock_manager.release_all_locks(transaction_id).await?;

        log::info!("Transaction committed: {}", transaction_id);

        Ok(())
    }

    /// Transaction rollback
    pub async fn rollback_transaction(&self, transaction_id: &TransactionId) -> Result<(), DatabaseError> {
        // Transaction context'i al
        let mut active_transactions = self.active_transactions.write().map_err(|e| {
            DatabaseError::LockError {
                reason: format!("Failed to write active transactions for rollback: {}", e)
            }
        })?;

        let context = active_transactions.remove(transaction_id)
            .ok_or_else(|| DatabaseError::TransactionError {
                message: format!("Transaction {} not found or already completed", transaction_id)
            })?;

        drop(active_transactions); // Lock'ı serbest bırak

        self.abort_transaction_internal(transaction_id, context).await?;

        log::info!("Transaction rolled back: {}", transaction_id);

        Ok(())
    }

    /// Internal abort transaction
    async fn abort_transaction_internal(
        &self,
        transaction_id: &TransactionId,
        mut context: TransactionContext,
    ) -> Result<(), DatabaseError> {
        // Status güncelle
        context.status = TransactionStatus::Aborted;

        // Version'ları rollback et
        self.version_manager.rollback_transaction(transaction_id)?;

        // WAL'a rollback yaz
        if let Some(ref wal_manager) = self.wal_manager {
            wal_manager.write_entry(crate::persistence::WalEntryType::TransactionRollback {
                transaction_id: *transaction_id,
            }).await?;
        }

        // Lock'ları serbest bırak
        self.lock_manager.release_all_locks(transaction_id).await?;

        Ok(())
    }

    /// Transaction durumunu al
    pub fn get_transaction_status(&self, transaction_id: &TransactionId) -> Result<Option<TransactionStatus>, DatabaseError> {
        let active_transactions = self.active_transactions.read().map_err(|e| {
            DatabaseError::LockError {
                reason: format!("Failed to read active transactions: {}", e)
            }
        })?;

        Ok(active_transactions.get(transaction_id).map(|ctx| ctx.status.clone()))
    }

    /// Lock alma
    pub async fn acquire_lock(
        &self,
        transaction_id: &TransactionId,
        resource_id: ResourceId,
        lock_type: LockType,
    ) -> Result<(), DatabaseError> {
        // Transaction'ın var olduğunu kontrol et ve timeout değerini al
        let timeout = {
            let active_transactions = self.active_transactions.read().map_err(|e| {
                DatabaseError::LockError {
                    reason: format!("Failed to read active transactions: {}", e)
                }
            })?;

            let context = active_transactions.get(transaction_id)
                .ok_or_else(|| DatabaseError::TransactionError {
                    message: format!("Transaction {} not found", transaction_id)
                })?;

            // Timeout kontrolü
            if context.is_timed_out() {
                return Err(DatabaseError::TransactionError {
                    message: format!("Transaction {} timed out", transaction_id)
                });
            }

            context.timeout
        }; // active_transactions guard scope'u burada bitti

        // Lock manager'dan lock al
        self.lock_manager.acquire_lock(*transaction_id, resource_id.clone(), lock_type, timeout).await?;

        // Transaction context'ine lock'ı ekle
        let mut active_transactions = self.active_transactions.write().map_err(|e| {
            DatabaseError::LockError {
                reason: format!("Failed to write active transactions: {}", e)
            }
        })?;

        if let Some(context) = active_transactions.get_mut(transaction_id) {
            context.acquired_locks.insert(resource_id);
        }

        Ok(())
    }

    /// MVCC read
    pub fn read_document(
        &self,
        transaction_id: &TransactionId,
        document_id: &Uuid,
    ) -> Result<Option<Value>, DatabaseError> {
        let active_transactions = self.active_transactions.read().map_err(|e| {
            DatabaseError::LockError {
                reason: format!("Failed to read active transactions: {}", e)
            }
        })?;

        let context = active_transactions.get(transaction_id)
            .ok_or_else(|| DatabaseError::TransactionError {
                message: format!("Transaction {} not found", transaction_id)
            })?;

        let read_timestamp = context.read_timestamp;

        drop(active_transactions); // Lock'ı serbest bırak

        self.version_manager.read_version(document_id, read_timestamp, transaction_id)
    }

    /// MVCC write
    pub async fn write_document(
        &self,
        transaction_id: &TransactionId,
        document_id: Uuid,
        data: Value,
        is_delete: bool,
    ) -> Result<(), DatabaseError> {
        // Write timestamp al
        let write_version = self.version_manager.next_version()?;

        // Version manager'a yeni version ekle
        self.version_manager.add_version(document_id, write_version, *transaction_id, data, is_delete)?;

        // Transaction context'e write timestamp güncelle
        let mut active_transactions = self.active_transactions.write().map_err(|e| {
            DatabaseError::LockError {
                reason: format!("Failed to write active transactions: {}", e)
            }
        })?;

        if let Some(context) = active_transactions.get_mut(transaction_id) {
            context.write_timestamp = write_version;
        }

        Ok(())
    }

    /// Active transaction'ları temizle (timeout olanları abort et)
    pub async fn cleanup_timed_out_transactions(&self) -> Result<usize, DatabaseError> {
        let mut active_transactions = self.active_transactions.write().map_err(|e| {
            DatabaseError::LockError {
                reason: format!("Failed to write active transactions for cleanup: {}", e)
            }
        })?;

        let mut timed_out_transactions = Vec::new();

        for (transaction_id, context) in active_transactions.iter() {
            if context.is_timed_out() {
                timed_out_transactions.push((*transaction_id, context.clone()));
            }
        }

        let cleanup_count = timed_out_transactions.len();

        // Timed out transaction'ları kaldır
        for (transaction_id, _) in &timed_out_transactions {
            active_transactions.remove(transaction_id);
        }

        drop(active_transactions); // Lock'ı serbest bırak

        // Her birini abort et
        for (transaction_id, context) in timed_out_transactions {
            self.abort_transaction_internal(&transaction_id, context).await?;
            log::warn!("Transaction {} aborted due to timeout", transaction_id);
        }

        Ok(cleanup_count)
    }

    /// Transaction statistics
    pub fn get_statistics(&self) -> Result<TransactionStatistics, DatabaseError> {
        let active_transactions = self.active_transactions.read().map_err(|e| {
            DatabaseError::LockError {
                reason: format!("Failed to read active transactions for statistics: {}", e)
            }
        })?;

        let active_count = active_transactions.len();
        let mut isolation_level_counts = HashMap::new();
        let mut oldest_transaction: Option<DateTime<Utc>> = None;

        for context in active_transactions.values() {
            // Isolation level count
            *isolation_level_counts.entry(context.isolation_level.clone()).or_insert(0) += 1;

            // Oldest transaction
            match oldest_transaction {
                None => oldest_transaction = Some(context.start_time),
                Some(oldest) => {
                    if context.start_time < oldest {
                        oldest_transaction = Some(context.start_time);
                    }
                }
            }
        }

        Ok(TransactionStatistics {
            active_transactions: active_count,
            isolation_level_counts,
            oldest_transaction,
        })
    }
}

/// Transaction Statistics
#[derive(Debug)]
pub struct TransactionStatistics {
    pub active_transactions: usize,
    pub isolation_level_counts: HashMap<IsolationLevel, usize>,
    pub oldest_transaction: Option<DateTime<Utc>>,
}

/// Transactional Database Wrapper
/// Bu wrapper tüm database operations'ları transaction context'i içinde yapar
pub struct TransactionalStorage {
    /// Underlying storage
    storage: Arc<MemoryStorage<Value>>,

    /// Transaction manager
    transaction_manager: Arc<TransactionManager>,
}

impl TransactionalStorage {
    pub fn new(storage: MemoryStorage<Value>) -> Self {
        Self {
            storage: Arc::new(storage),
            transaction_manager: Arc::new(TransactionManager::new()),
        }
    }

    /// WAL manager set et
    pub fn set_wal_manager(&mut self, wal_manager: Arc<crate::persistence::WalManager>) {
        Arc::get_mut(&mut self.transaction_manager)
            .expect("Transaction manager should be uniquely owned")
            .set_wal_manager(wal_manager);
    }

    /// Transaction başlat
    pub async fn begin_transaction(
        &self,
        isolation_level: IsolationLevel,
        timeout: Option<Duration>,
    ) -> Result<TransactionId, DatabaseError> {
        self.transaction_manager.begin_transaction(isolation_level, timeout).await
    }

    /// Transaction commit
    pub async fn commit_transaction(&self, transaction_id: &TransactionId) -> Result<(), DatabaseError> {
        self.transaction_manager.commit_transaction(transaction_id).await
    }

    /// Transaction rollback
    pub async fn rollback_transaction(&self, transaction_id: &TransactionId) -> Result<(), DatabaseError> {
        self.transaction_manager.rollback_transaction(transaction_id).await
    }

    /// Transactional create
    /// CRITICAL FIX: Document ID tutarlılığı için manuel Document yaratımı
    pub async fn transactional_create(
        &self,
        transaction_id: &TransactionId,
        data: Value,
    ) -> Result<Document<Value>, DatabaseError> {
        let document_id = Uuid::new_v4();

        // Exclusive lock al
        self.transaction_manager.acquire_lock(
            transaction_id,
            ResourceId::Document(document_id),
            LockType::Exclusive,
        ).await?;

        // MVCC write
        self.transaction_manager.write_document(transaction_id, document_id, data.clone(), false).await?;

        // CRITICAL FIX: Tutarlı document ID ile Document döndür
        Ok(Document {
            metadata: crate::DocumentMetadata {
                id: document_id,  // Aynı ID'yi kullan
                created_at: Utc::now(),
                updated_at: Utc::now(),
                version: 1,
            },
            data,
        })
    }

    /// Transactional read
    pub async fn transactional_read(
        &self,
        transaction_id: &TransactionId,
        document_id: &Uuid,
    ) -> Result<Option<Value>, DatabaseError> {
        // Shared lock al (isolation level'a göre)
        self.transaction_manager.acquire_lock(
            transaction_id,
            ResourceId::Document(*document_id),
            LockType::Shared,
        ).await?;

        // MVCC read
        self.transaction_manager.read_document(transaction_id, document_id)
    }

    /// Transactional update
    pub async fn transactional_update(
        &self,
        transaction_id: &TransactionId,
        document_id: &Uuid,
        data: Value,
    ) -> Result<Document<Value>, DatabaseError> {
        // Exclusive lock al
        self.transaction_manager.acquire_lock(
            transaction_id,
            ResourceId::Document(*document_id),
            LockType::Exclusive,
        ).await?;

        // MVCC write
        self.transaction_manager.write_document(transaction_id, *document_id, data.clone(), false).await?;

        // Updated document döndür
        Ok(Document {
            metadata: crate::DocumentMetadata {
                id: *document_id,
                created_at: Utc::now(), // Bu normalde eski created_at olmalı ama demo için basitleştirildi
                updated_at: Utc::now(),
                version: 2, // Bu da MVCC'den alınmalı ama demo için basitleştirildi
            },
            data,
        })
    }

    /// Transactional delete
    pub async fn transactional_delete(
        &self,
        transaction_id: &TransactionId,
        document_id: &Uuid,
    ) -> Result<bool, DatabaseError> {
        // Exclusive lock al
        self.transaction_manager.acquire_lock(
            transaction_id,
            ResourceId::Document(*document_id),
            LockType::Exclusive,
        ).await?;

        // MVCC delete (data olarak null yaz)
        self.transaction_manager.write_document(transaction_id, *document_id, Value::Null, true).await?;

        Ok(true)
    }

    /// Transaction manager'a erişim
    pub fn transaction_manager(&self) -> &Arc<TransactionManager> {
        &self.transaction_manager
    }

    /// Underlying storage'a erişim
    pub fn storage(&self) -> &Arc<MemoryStorage<Value>> {
        &self.storage
    }
}

// Default implementations
impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for VersionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}
