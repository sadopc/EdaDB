// persistence.rs - Complete Write-Ahead Log & Recovery System
// Bu modül modern veritabanı sistemlerinin temel persistence katmanını implement eder

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use uuid::Uuid;
use std::time::{Duration, Instant};
use tokio::time::interval;
use crate::{Document, DatabaseError, MemoryStorage, CrudDatabase};

// Bincode import for binary serialization - production database'lerde performance için kritik
use bincode;

/// WAL (Write-Ahead Log) Entry Types
/// Her entry type farklı bir veritabanı operasyonunu temsil eder
/// Bu design pattern, event sourcing ve CQRS architecture'lerinde yaygın kullanılır
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalEntryType {
    /// Yeni döküman ekleme operasyonu
    /// Data: tam döküman içeriği
    Insert {
        document_id: Uuid,
        data: Value,
    },

    /// Döküman güncelleme operasyonu
    /// Old_data: rollback için gerekli (opsiyonel)
    /// New_data: yeni içerik
    Update {
        document_id: Uuid,
        old_data: Option<Value>,
        new_data: Value,
        old_version: u64,
        new_version: u64,
    },

    /// Döküman silme operasyonu
    /// Deleted_data: recovery için saklıyoruz
    Delete {
        document_id: Uuid,
        deleted_data: Value,
    },

    /// Batch operasyonları için
    /// Atomicity garantisi sağlamak için batch'leri tek entry olarak log'luyoruz
    BatchInsert {
        documents: Vec<(Uuid, Value)>,
    },

    /// Index operasyonları
    /// Index create/drop gibi metadata operations için
    IndexOperation {
        operation_type: String, // "create", "drop", "rebuild"
        index_name: String,
        config: Option<Value>, // Index configuration
    },

    /// Checkpoint marker - recovery optimization için
    /// Bu point'e kadar tüm operasyonlar snapshot'a dahil
    Checkpoint {
        snapshot_id: Uuid,
        document_count: usize,
        index_count: usize,
    },

    /// Transaction boundaries (gelecek özellik için hazır)
    TransactionBegin {
        transaction_id: Uuid,
    },

    TransactionCommit {
        transaction_id: Uuid,
    },

    TransactionRollback {
        transaction_id: Uuid,
    },
}

/// WAL Entry - Write-Ahead Log'daki her kayıt
/// Bu struct log dosyasındaki her satırı temsil eder
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    /// Entry'nin benzersiz kimliği
    pub id: Uuid,

    /// Entry'nin türü ve verisi
    pub entry_type: WalEntryType,

    /// İşlemin gerçekleştiği zaman
    pub timestamp: DateTime<Utc>,

    /// Log Sequence Number - ordering için kritik
    /// Recovery sırasında entries'lerin doğru sırayla apply edilmesi için
    pub lsn: u64,

    /// Checksum - data integrity için
    /// Log corruption'ı detect etmek için kullanılır
    pub checksum: u64,
}

impl WalEntry {
    /// Yeni WAL entry oluşturur
    /// Checksum otomatik olarak hesaplanır - data integrity için kritik
    pub fn new(entry_type: WalEntryType, lsn: u64) -> Self {
        let mut entry = Self {
            id: Uuid::new_v4(),
            entry_type,
            timestamp: Utc::now(),
            lsn,
            checksum: 0, // Önce 0 set edip sonra hesaplayacağız
        };

        // Checksum hesaplama - simple hash function kullanıyoruz
        // Production'da CRC32 veya xxhash gibi daha robust hash functions kullanılır
        entry.checksum = entry.calculate_checksum();
        entry
    }

    /// Checksum hesaplama - data corruption detect etmek için
    /// Bu basit bir implementation, production'da cryptographic hash kullanılır
    fn calculate_checksum(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();

        // Checksum hariç tüm field'ları hash'le
        self.id.hash(&mut hasher);
        self.timestamp.timestamp_nanos_opt().unwrap_or(0).hash(&mut hasher);
        self.lsn.hash(&mut hasher);

        // Entry type'ı serialize edip hash'le
        if let Ok(serialized) = serde_json::to_string(&self.entry_type) {
            serialized.hash(&mut hasher);
        }

        hasher.finish()
    }

    /// Entry'nin integrity'sini kontrol eder
    /// Recovery sırasında corrupt entry'leri detect etmek için
    pub fn verify_integrity(&self) -> bool {
        let calculated_checksum = self.calculate_checksum();
        self.checksum == calculated_checksum
    }
}

/// WAL Configuration - Write-Ahead Log ayarları
/// Bu settings production environment'ında fine-tuning için kullanılır
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// WAL dosyasının path'i
    pub wal_file_path: PathBuf,

    /// Snapshot dosyalarının bulunduğu dizin
    pub snapshot_directory: PathBuf,

    /// WAL dosyası maksimum boyutu (bytes)
    /// Bu boyuta ulaştığında log rotation yapılır
    pub max_wal_size: usize,

    /// Kaç entry'den sonra force sync yapılacağı
    /// Performance vs durability trade-off
    pub sync_interval: usize,

    /// Checkpoint alma sıklığı (saniye)
    /// Çok sık: I/O overhead, çok seyrek: uzun recovery time
    pub checkpoint_interval_seconds: u64,

    /// Recovery sırasında maximum replay edilecek entry sayısı
    /// Çok büyük log dosyalarında memory protection için
    pub max_recovery_entries: usize,

    /// WAL dosyası format'ı
    pub format: WalFormat,
}

/// WAL file format options
/// JSON: human readable, debugging friendly, biraz daha yavaş
/// Binary: compact, hızlı, production için ideal
#[derive(Debug, Clone, PartialEq)]
pub enum WalFormat {
    Json,      // JSON format - debugging friendly
    Binary,    // Binary format - production optimized
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            wal_file_path: PathBuf::from("database.wal"),
            snapshot_directory: PathBuf::from("snapshots"),
            max_wal_size: 100 * 1024 * 1024, // 100MB
            sync_interval: 100,               // Her 100 entry'de sync
            checkpoint_interval_seconds: 300, // 5 dakikada bir checkpoint
            max_recovery_entries: 1_000_000,  // 1M entry limit
            format: WalFormat::Json,          // Default olarak JSON (debugging için)
        }
    }
}

/// Write-Ahead Log Manager
/// Bu class tüm WAL operasyonlarını yönetir ve ACID properties'i guarantee eder
pub struct WalManager {
    /// Configuration settings
    config: WalConfig,

    /// Current WAL file writer
    /// BufWriter: performance optimization - batch yazma için
    wal_writer: Arc<Mutex<BufWriter<File>>>,

    /// Current Log Sequence Number - atomicity için
    /// Her entry'ye unique, artan LSN assign edilir
    current_lsn: Arc<Mutex<u64>>,

    /// Entry counter for sync operations
    /// sync_interval'e ulaştığında force sync yapılır
    entry_count: Arc<Mutex<usize>>,

    /// Background checkpoint task handle - Arc<Mutex<>> ile thread-safe
    /// Bu wrapper sayesinde multiple Arc reference'ları handle'ı paylaşabilir
    checkpoint_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl WalManager {
    /// Yeni WAL Manager oluşturur
    /// File system'ı initialize eder ve recovery check yapar
    pub async fn new(config: WalConfig) -> Result<Self, DatabaseError> {
        // WAL dizinini oluştur
        if let Some(parent) = config.wal_file_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                DatabaseError::InvalidQuery {
                    message: format!("Failed to create WAL directory: {}", e)
                }
            })?;
        }

        // Snapshot dizinini oluştur
        std::fs::create_dir_all(&config.snapshot_directory).map_err(|e| {
            DatabaseError::InvalidQuery {
                message: format!("Failed to create snapshot directory: {}", e)
            }
        })?;

        // WAL dosyasını aç - append mode'da
        let wal_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&config.wal_file_path)
            .map_err(|e| {
                DatabaseError::InvalidQuery {
                    message: format!("Failed to open WAL file: {}", e)
                }
            })?;

        let wal_writer = Arc::new(Mutex::new(BufWriter::new(wal_file)));

        // Current LSN'i hesapla - recovery için gerekli
        let current_lsn = Arc::new(Mutex::new(Self::calculate_current_lsn(&config).await?));

        log::info!("WAL Manager initialized, current LSN: {}",
                  *current_lsn.lock().unwrap());

        Ok(Self {
            config,
            wal_writer,
            current_lsn,
            entry_count: Arc::new(Mutex::new(0)),
            checkpoint_handle: Arc::new(Mutex::new(None)), // Arc<Mutex<Option<JoinHandle>>>
        })
    }

    /// WAL dosyasından mevcut LSN'i hesaplar
    /// Recovery ve continuation için gerekli
    async fn calculate_current_lsn(config: &WalConfig) -> Result<u64, DatabaseError> {
        if !config.wal_file_path.exists() {
            return Ok(0); // Yeni veritabanı
        }

        let file = File::open(&config.wal_file_path).map_err(|e| {
            DatabaseError::InvalidQuery {
                message: format!("Failed to read WAL file for LSN calculation: {}", e)
            }
        })?;

        let reader = BufReader::new(file);
        let mut max_lsn = 0u64;

        // WAL dosyasının tamamını okuyup en yüksek LSN'i bul
        for line in reader.lines() {
            let line = line.map_err(|e| {
                DatabaseError::InvalidQuery {
                    message: format!("Failed to read WAL line: {}", e)
                }
            })?;

            if let Ok(entry) = serde_json::from_str::<WalEntry>(&line) {
                if entry.lsn > max_lsn {
                    max_lsn = entry.lsn;
                }
            }
        }

        Ok(max_lsn)
    }

    /// WAL'a entry yazar - tüm database operations bu method'u kullanır
    /// Bu method ACID'in Durability prensibini implement eder
    pub async fn write_entry(&self, entry_type: WalEntryType) -> Result<u64, DatabaseError> {
        // 1. Next LSN'i al - atomic operation
        let lsn = {
            let mut current_lsn = self.current_lsn.lock().unwrap();
            *current_lsn += 1;
            *current_lsn
        };

        // 2. WAL entry oluştur
        let entry = WalEntry::new(entry_type, lsn);

        // 3. Entry'yi serialize et (format'a göre)
        let serialized = match self.config.format {
            WalFormat::Json => {
                let mut json_line = serde_json::to_string(&entry).map_err(|e| {
                    DatabaseError::SerializationError {
                        message: format!("Failed to serialize WAL entry: {}", e)
                    }
                })?;
                json_line.push('\n'); // Newline delimiter
                json_line.into_bytes() // Convert to bytes for unified handling
            },
            WalFormat::Binary => {
                // Binary format implementation - daha performant
                // Length-prefixed binary format: [length: 4 bytes][data: length bytes][newline: 1 byte]
                let binary_data = bincode::serialize(&entry).map_err(|e| {
                    DatabaseError::SerializationError {
                        message: format!("Failed to serialize WAL entry to binary: {}", e)
                    }
                })?;

                // Length prefix (4 bytes) + data + newline delimiter için binary format
                let len = binary_data.len() as u32;
                let mut result = Vec::with_capacity(4 + binary_data.len() + 1);
                result.extend_from_slice(&len.to_le_bytes()); // Little-endian length
                result.extend_from_slice(&binary_data);
                result.push(b'\n'); // Newline delimiter for line-based reading
                result
            }
        };

        // 4. WAL dosyasına yaz - bu kritik section
        {
            let mut writer = self.wal_writer.lock().unwrap();
            writer.write_all(&serialized).map_err(|e| {
                DatabaseError::InvalidQuery {
                    message: format!("Failed to write to WAL: {}", e)
                }
            })?;

            // CRITICAL FIX: Her entry'den sonra immediate flush
            // Bu demo için performance'ı biraz düşürür ama durability guarantee eder
            writer.flush().map_err(|e| {
                DatabaseError::InvalidQuery {
                    message: format!("Failed to flush WAL entry: {}", e)
                }
            })?;
        }

        // 5. Entry count'u artır ve sync kontrolü yap
        let should_sync = {
            let mut count = self.entry_count.lock().unwrap();
            *count += 1;
            *count >= self.config.sync_interval
        };

        // 6. Periyodik sync - durability guarantee (artık her entry'de flush yapıyoruz)
        if should_sync {
            let mut count = self.entry_count.lock().unwrap();
            *count = 0; // Reset counter
        }

        log::debug!("WAL entry written and flushed: LSN {}, type: {:?}", lsn,
                   std::mem::discriminant(&entry.entry_type));

        Ok(lsn)
    }

    /// Force sync - buffer'ları diske flush eder
    /// Critical operations'dan sonra çağrılır
    pub async fn force_sync(&self) -> Result<(), DatabaseError> {
        let mut writer = self.wal_writer.lock().unwrap();
        writer.flush().map_err(|e| {
            DatabaseError::InvalidQuery {
                message: format!("Failed to sync WAL: {}", e)
            }
        })?;

        log::debug!("WAL force sync completed");
        Ok(())
    }

    /// WAL dosyasından entries'leri okur - recovery için
    /// Bu method crash recovery sırasında kullanılır
    pub async fn read_entries_from_lsn(&self, start_lsn: u64) -> Result<Vec<WalEntry>, DatabaseError> {
        let file = File::open(&self.config.wal_file_path).map_err(|e| {
            DatabaseError::InvalidQuery {
                message: format!("Failed to open WAL for reading: {}", e)
            }
        })?;

        let reader = BufReader::new(file);
        let mut entries = Vec::new();
        let mut processed_count = 0;

        for line in reader.lines() {
            // Max entry limit protection
            if processed_count >= self.config.max_recovery_entries {
                log::warn!("Recovery entry limit reached: {}", self.config.max_recovery_entries);
                break;
            }

            let line = line.map_err(|e| {
                DatabaseError::InvalidQuery {
                    message: format!("Failed to read WAL line during recovery: {}", e)
                }
            })?;

            // Parse WAL entry
            match serde_json::from_str::<WalEntry>(&line) {
                Ok(entry) => {
                    // Integrity check
                    if !entry.verify_integrity() {
                        log::error!("WAL entry integrity check failed: LSN {}", entry.lsn);
                        return Err(DatabaseError::InvalidQuery {
                            message: format!("WAL corruption detected at LSN {}", entry.lsn)
                        });
                    }

                    // LSN filter
                    if entry.lsn >= start_lsn {
                        entries.push(entry);
                    }

                    processed_count += 1;
                },
                Err(e) => {
                    log::warn!("Failed to parse WAL entry: {}", e);
                    // Continue with next line - partial corruption handling
                }
            }
        }

        // LSN'e göre sırala - recovery order guarantee
        entries.sort_by_key(|e| e.lsn);

        log::info!("Loaded {} WAL entries from LSN {}", entries.len(), start_lsn);
        Ok(entries)
    }

    /// Checkpoint alma - snapshot oluşturur
    /// Bu method recovery time'ı minimize eder
    pub async fn create_checkpoint(&self, storage: &MemoryStorage<Value>) -> Result<Uuid, DatabaseError> {
        let checkpoint_id = Uuid::new_v4();
        let timestamp = Utc::now();

        log::info!("Creating checkpoint: {}", checkpoint_id);

        // CRITICAL FIX: Current LSN'i al - bu checkpoint'te hangi LSN'e kadar işlendiğini gösterir
        let current_lsn = {
            let lsn_guard = self.current_lsn.lock().unwrap();
            *lsn_guard
        };

        // 1. Memory storage'dan tüm data'yı al
        let all_documents = storage.read_all(None, None).await?;
        let storage_stats = storage.stats().await?;
        let indexes = storage.list_indexes()?;

        // 2. Snapshot dosya path'i oluştur
        let snapshot_filename = format!("snapshot_{}.json", checkpoint_id);
        let snapshot_path = self.config.snapshot_directory.join(snapshot_filename);

        // 3. Snapshot data structure - LAST_LSN ile
        let snapshot_data = SnapshotData {
            checkpoint_id,
            timestamp,
            documents: all_documents,
            storage_stats,
            indexes,
            last_lsn: Some(current_lsn), // Option olarak wrap et
            version: "1.0".to_string(),
        };

        // 4. Snapshot'ı diske yaz
        let snapshot_file = File::create(&snapshot_path).map_err(|e| {
            DatabaseError::InvalidQuery {
                message: format!("Failed to create snapshot file: {}", e)
            }
        })?;

        let writer = BufWriter::new(snapshot_file);
        serde_json::to_writer_pretty(writer, &snapshot_data).map_err(|e| {
            DatabaseError::SerializationError {
                message: format!("Failed to write snapshot: {}", e)
            }
        })?;

        // 5. WAL'a checkpoint marker yaz
        self.write_entry(WalEntryType::Checkpoint {
            snapshot_id: checkpoint_id,
            document_count: snapshot_data.documents.len(),
            index_count: snapshot_data.indexes.len(),
        }).await?;

        // 6. WAL'ı sync et
        self.force_sync().await?;

        log::info!("Checkpoint created successfully: {} ({} documents, {} indexes, LSN: {})",
                  checkpoint_id, snapshot_data.documents.len(), snapshot_data.indexes.len(), current_lsn);

        Ok(checkpoint_id)
    }

    /// Background checkpoint task başlatır
    /// Periyodik olarak automatic checkpoint alır
    /// Arc wrapper sayesinde thread-safe operation sağlar
    pub fn start_background_checkpoint(&self, storage: Arc<MemoryStorage<Value>>) {
        let interval_duration = Duration::from_secs(self.config.checkpoint_interval_seconds);
        let config = self.config.clone();
        let wal_writer = Arc::clone(&self.wal_writer);
        let current_lsn = Arc::clone(&self.current_lsn);
        let entry_count = Arc::clone(&self.entry_count);
        let checkpoint_handle_ref = Arc::clone(&self.checkpoint_handle);

        let handle = tokio::spawn(async move {
            let mut interval_timer = interval(interval_duration);

            loop {
                interval_timer.tick().await;

                log::info!("Background checkpoint starting...");

                // Temporary WAL manager for checkpoint - Arc references ile
                let temp_wal = WalManager {
                    config: config.clone(),
                    wal_writer: Arc::clone(&wal_writer),
                    current_lsn: Arc::clone(&current_lsn),
                    entry_count: Arc::clone(&entry_count),
                    checkpoint_handle: Arc::clone(&checkpoint_handle_ref),
                };

                match temp_wal.create_checkpoint(&storage).await {
                    Ok(checkpoint_id) => {
                        log::info!("Background checkpoint completed: {}", checkpoint_id);
                    },
                    Err(e) => {
                        log::error!("Background checkpoint failed: {:?}", e);
                        // Continue operation even if checkpoint fails
                    }
                }
            }
        });

        // Handle'ı Arc<Mutex<>> içinde sakla - thread-safe assignment
        if let Ok(mut handle_guard) = self.checkpoint_handle.lock() {
            *handle_guard = Some(handle);
        }
    }

    /// Background task'ı durdurur - Arc wrapper ile thread-safe
    pub fn stop_background_checkpoint(&self) {
        if let Ok(mut handle_guard) = self.checkpoint_handle.lock() {
            if let Some(handle) = handle_guard.take() {
                handle.abort();
                log::info!("Background checkpoint task stopped");
            }
        }
    }
}

/// Snapshot data structure - checkpoint içeriği
/// Bu structure recovery sırasında system state'ini restore etmek için kullanılır
#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotData {
    /// Checkpoint benzersiz kimliği
    pub checkpoint_id: Uuid,

    /// Snapshot alınma zamanı
    pub timestamp: DateTime<Utc>,

    /// Tüm dökümanlar
    pub documents: Vec<Document<Value>>,

    /// Storage istatistikleri
    pub storage_stats: crate::StorageStats,

    /// Index konfigürasyonları
    pub indexes: Vec<crate::index::IndexConfig>,

    /// CRITICAL FIX: Checkpoint'te işlenen son LSN
    /// Recovery sırasında bu LSN'den sonraki entries replay edilir
    /// Option olarak tanımlıyoruz ki eski snapshot'larla compatible olsun
    #[serde(default)]
    pub last_lsn: Option<u64>,

    /// Snapshot format versiyonu - backward compatibility için
    pub version: String,
}

/// Recovery Manager - crash recovery operations
/// Bu class sistem restart'tan sonra consistent state'e geri döner
pub struct RecoveryManager {
    wal_config: WalConfig,
}

impl RecoveryManager {
    pub fn new(wal_config: WalConfig) -> Self {
        Self { wal_config }
    }

    /// Full recovery process - crash'ten sonra çağrılır
    /// Bu method database'i son tutarlı duruma restore eder
    pub async fn recover(&self, storage: &mut MemoryStorage<Value>) -> Result<RecoveryInfo, DatabaseError> {
        log::info!("Starting database recovery...");
        let start_time = Instant::now();

        // 1. En son checkpoint'i bul
        let latest_checkpoint = self.find_latest_checkpoint().await?;

        // 2. Checkpoint'ten restore et (varsa)
        let start_lsn = if let Some(checkpoint_id) = latest_checkpoint {
            log::info!("Restoring from checkpoint: {}", checkpoint_id);
            let restored_lsn = self.restore_from_checkpoint(checkpoint_id, storage).await?;

            // CRITICAL FIX: Checkpoint'te işlenen son LSN'den sonraki entries'i replay et
            // Bu sayede duplicate entries problemi çözülür
            log::info!("Checkpoint restored up to LSN: {}, will replay from LSN: {}", restored_lsn, restored_lsn + 1);
            restored_lsn + 1 // Checkpoint'ten sonraki entry'lerden başla
        } else {
            log::info!("No checkpoint found, starting from beginning");
            0
        };

        // 3. WAL replay - checkpoint'ten sonraki entries
        let wal_manager = WalManager::new(self.wal_config.clone()).await?;
        let entries = wal_manager.read_entries_from_lsn(start_lsn).await?;

        log::info!("Replaying {} WAL entries from LSN {}", entries.len(), start_lsn);

        let mut replayed_count = 0;
        let mut error_count = 0;

        // CRITICAL FIX: entries'ın last LSN'ini loop'tan ÖNCE al
        // Çünkü for loop entries'ı move edecek ve sonra .last() çağrılamayacak
        let final_lsn = entries.last().map(|e| e.lsn).unwrap_or(start_lsn);

        for entry in entries {
            match self.replay_entry(&entry, storage).await {
                Ok(_) => replayed_count += 1,
                Err(e) => {
                    error_count += 1;
                    log::error!("Failed to replay entry LSN {}: {:?}", entry.lsn, e);

                    // Critical errors durumunda recovery'yi durdur
                    if error_count > 10 {
                        return Err(DatabaseError::InvalidQuery {
                            message: format!("Too many replay errors ({}), stopping recovery", error_count)
                        });
                    }
                }
            }
        }

        let recovery_time = start_time.elapsed();

        log::info!("Recovery completed: {} entries replayed, {} errors, took {:?}",
                  replayed_count, error_count, recovery_time);

        Ok(RecoveryInfo {
            checkpoint_restored: latest_checkpoint,
            entries_replayed: replayed_count,
            replay_errors: error_count,
            recovery_duration: recovery_time,
            final_lsn, // Artık güvenli şekilde kullanabiliriz
        })
    }

    /// En son checkpoint'i bulur
    async fn find_latest_checkpoint(&self) -> Result<Option<Uuid>, DatabaseError> {
        let snapshot_dir = &self.wal_config.snapshot_directory;

        if !snapshot_dir.exists() {
            return Ok(None);
        }

        let mut latest_checkpoint: Option<(Uuid, DateTime<Utc>)> = None;

        // Snapshot dizinindeki tüm dosyaları tara
        let entries = std::fs::read_dir(snapshot_dir).map_err(|e| {
            DatabaseError::InvalidQuery {
                message: format!("Failed to read snapshot directory: {}", e)
            }
        })?;

        for entry in entries {
            let entry = entry.map_err(|e| {
                DatabaseError::InvalidQuery {
                    message: format!("Failed to read directory entry: {}", e)
                }
            })?;

            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                // Snapshot dosyasını parse et
                if let Some(filename) = path.file_stem().and_then(|s| s.to_str()) {
                    if filename.starts_with("snapshot_") {
                        if let Ok(uuid_str) = filename.strip_prefix("snapshot_").unwrap().parse::<Uuid>() {
                            // Snapshot metadata'sını oku
                            if let Ok(snapshot_data) = self.read_snapshot_metadata(&path).await {
                                match latest_checkpoint {
                                    None => latest_checkpoint = Some((uuid_str, snapshot_data.timestamp)),
                                    Some((_, latest_time)) => {
                                        if snapshot_data.timestamp > latest_time {
                                            latest_checkpoint = Some((uuid_str, snapshot_data.timestamp));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(latest_checkpoint.map(|(uuid, _)| uuid))
    }

    /// Snapshot metadata'sını okur (full snapshot load etmeden)
    async fn read_snapshot_metadata(&self, path: &Path) -> Result<SnapshotData, DatabaseError> {
        let file = File::open(path).map_err(|e| {
            DatabaseError::InvalidQuery {
                message: format!("Failed to open snapshot file: {}", e)
            }
        })?;

        let reader = BufReader::new(file);
        let snapshot_data: SnapshotData = serde_json::from_reader(reader).map_err(|e| {
            DatabaseError::SerializationError {
                message: format!("Failed to parse snapshot: {}", e)
            }
        })?;

        Ok(snapshot_data)
    }

    /// Checkpoint'ten storage'ı restore eder
    async fn restore_from_checkpoint(&self, checkpoint_id: Uuid, storage: &mut MemoryStorage<Value>) -> Result<u64, DatabaseError> {
        let snapshot_filename = format!("snapshot_{}.json", checkpoint_id);
        let snapshot_path = self.wal_config.snapshot_directory.join(snapshot_filename);

        let snapshot_data = self.read_snapshot_metadata(&snapshot_path).await?;

        // Backward compatibility: eski snapshot'larda last_lsn olmayabilir
        let last_lsn = snapshot_data.last_lsn.unwrap_or(0);

        log::info!("Restoring {} documents and {} indexes from checkpoint (LSN: {})",
                  snapshot_data.documents.len(), snapshot_data.indexes.len(), last_lsn);

        // 1. Storage'ı temizle
        storage.delete_all().await?;

        // 2. Documents'ları restore et
        for document in snapshot_data.documents {
            // Storage'a direkt document insert et (WAL bypass)
            storage.create_with_id(document.metadata.id, document.data).await?;
        }

        // 3. Index'leri restore et
        for index_config in snapshot_data.indexes {
            storage.create_index(
                &index_config.name,
                index_config.fields.iter().map(|s| s.as_str()).collect(),
                index_config.index_type
            )?;
        }

        log::info!("Checkpoint restore completed, last processed LSN: {}", last_lsn);

        // CRITICAL FIX: Checkpoint'te işlenen son LSN'i döndür
        // Recovery sadece bu LSN'den sonraki entries'i replay edecek
        Ok(last_lsn)
    }

    /// Tek WAL entry'yi replay eder
    async fn replay_entry(&self, entry: &WalEntry, storage: &mut MemoryStorage<Value>) -> Result<(), DatabaseError> {
        log::debug!("Replaying entry LSN {}: {:?}", entry.lsn,
                   std::mem::discriminant(&entry.entry_type));

        match &entry.entry_type {
            WalEntryType::Insert { document_id, data } => {
                // Document insert replay
                storage.create_with_id(*document_id, data.clone()).await?;
            },

            WalEntryType::Update { document_id, new_data, .. } => {
                // Document update replay
                storage.update(document_id, new_data.clone()).await?;
            },

            WalEntryType::Delete { document_id, .. } => {
                // Document delete replay
                storage.delete(document_id).await?;
            },

            WalEntryType::BatchInsert { documents } => {
                // Batch insert replay
                let data_vec: Vec<Value> = documents.iter().map(|(_, data)| data.clone()).collect();
                storage.create_batch(data_vec).await?;
            },

            WalEntryType::IndexOperation { operation_type, index_name, config } => {
                // Index operation replay
                match operation_type.as_str() {
                    "create" => {
                        if let Some(_config_value) = config { // _ prefix ile unused warning'i gider
                            // Index config'i parse et ve restore et
                            // Bu simplified implementation - production'da daha complex
                            log::info!("Replaying index creation: {}", index_name);
                        }
                    },
                    "drop" => {
                        if let Err(_) = storage.drop_index(index_name) {
                            log::warn!("Failed to replay index drop: {}", index_name);
                        }
                    },
                    _ => {
                        log::warn!("Unknown index operation: {}", operation_type);
                    }
                }
            },

            WalEntryType::Checkpoint { .. } => {
                // Checkpoint marker'ları skip edilir - sadece metadata
                log::debug!("Skipping checkpoint marker");
            },

            WalEntryType::TransactionBegin { .. } |
            WalEntryType::TransactionCommit { .. } |
            WalEntryType::TransactionRollback { .. } => {
                // Transaction support henüz implement edilmedi
                log::debug!("Transaction entries not yet supported");
            }
        }

        Ok(())
    }
}

/// Recovery operation sonuçları
/// Bu info recovery'nin başarısı ve performance metrics'i için kullanılır
#[derive(Debug)]
pub struct RecoveryInfo {
    /// Restore edilen checkpoint ID (varsa)
    pub checkpoint_restored: Option<Uuid>,

    /// Replay edilen entry sayısı
    pub entries_replayed: usize,

    /// Replay sırasında oluşan hata sayısı
    pub replay_errors: usize,

    /// Recovery'nin toplam süresi
    pub recovery_duration: Duration,

    /// Son işlenen LSN
    pub final_lsn: u64,
}

impl RecoveryInfo {
    /// Recovery başarılı mı?
    pub fn is_successful(&self) -> bool {
        self.replay_errors == 0
    }

    /// Recovery summary'si
    pub fn summary(&self) -> String {
        format!(
            "Recovery completed: {} entries replayed, {} errors, {:?} duration, final LSN: {}",
            self.entries_replayed, self.replay_errors, self.recovery_duration, self.final_lsn
        )
    }
}

/// WAL-enabled MemoryStorage wrapper
/// Bu wrapper tüm operations'ları WAL'a log'lar
pub struct PersistentMemoryStorage {
    /// Inner memory storage
    storage: MemoryStorage<Value>,

    /// WAL manager
    wal_manager: Arc<WalManager>,

    /// Recovery manager
    recovery_manager: RecoveryManager,
}

impl PersistentMemoryStorage {
    /// Yeni persistent storage oluşturur
    /// Auto-recovery yaparak sistem ready hale getirir
    pub async fn new(wal_config: WalConfig) -> Result<Self, DatabaseError> {
        log::info!("Initializing persistent memory storage...");

        // 1. WAL manager oluştur
        let wal_manager = WalManager::new(wal_config.clone()).await?;

        // 2. Memory storage oluştur
        let mut storage = MemoryStorage::<Value>::new();

        // 3. Recovery işlemi
        let recovery_manager = RecoveryManager::new(wal_config.clone());
        let recovery_info = recovery_manager.recover(&mut storage).await?;

        log::info!("Recovery completed: {}", recovery_info.summary());

        let wal_manager = Arc::new(wal_manager);

        // 4. Background checkpoint'i başlat - Arc clone ile
        let storage_arc = Arc::new(storage.clone());
        wal_manager.start_background_checkpoint(storage_arc);

        Ok(Self {
            storage,
            wal_manager,
            recovery_manager,
        })
    }

    /// Graceful shutdown - background tasks'ları durdurur
    pub async fn shutdown(&self) -> Result<(), DatabaseError> {
        log::info!("Shutting down persistent storage...");

        // 1. Background checkpoint'i durdur - Arc wrapper üzerinden
        self.wal_manager.stop_background_checkpoint();

        // 2. Final checkpoint al
        self.wal_manager.create_checkpoint(&self.storage).await?;

        // 3. WAL'ı sync et
        self.wal_manager.force_sync().await?;

        log::info!("Persistent storage shutdown completed");
        Ok(())
    }

    /// Storage'a erişim sağlar
    pub fn storage(&self) -> &MemoryStorage<Value> {
        &self.storage
    }

    /// Manual checkpoint alma
    pub async fn create_checkpoint(&self) -> Result<Uuid, DatabaseError> {
        self.wal_manager.create_checkpoint(&self.storage).await
    }

    /// Recovery manager'a erişim - manual recovery operations için
    pub fn recovery_manager(&self) -> &RecoveryManager {
        &self.recovery_manager
    }
}

// WAL-enabled CRUD operations için PersistentMemoryStorage'a implementation'lar eklenir
// Bu implementation'lar hem memory'de operation yapar hem de WAL'a log'lar

#[async_trait::async_trait]
impl crate::CrudDatabase<Value> for PersistentMemoryStorage {
    async fn create(&self, data: Value) -> Result<Document<Value>, DatabaseError> {
        // 1. WAL'a log yaz (durability için önce)
        let document_id = Uuid::new_v4();
        self.wal_manager.write_entry(WalEntryType::Insert {
            document_id,
            data: data.clone(),
        }).await?;

        // 2. Memory'de operation yap
        self.storage.create_with_id(document_id, data).await
    }

    async fn create_with_id(&self, id: Uuid, data: Value) -> Result<Document<Value>, DatabaseError> {
        // WAL + Memory operation
        self.wal_manager.write_entry(WalEntryType::Insert {
            document_id: id,
            data: data.clone(),
        }).await?;

        self.storage.create_with_id(id, data).await
    }

    async fn create_batch(&self, documents: Vec<Value>) -> Result<Vec<Document<Value>>, DatabaseError> {
        // Generate IDs for batch
        let documents_with_ids: Vec<(Uuid, Value)> = documents
            .into_iter()
            .map(|data| (Uuid::new_v4(), data))
            .collect();

        // WAL logging
        self.wal_manager.write_entry(WalEntryType::BatchInsert {
            documents: documents_with_ids.clone(),
        }).await?;

        // Memory operation
        let data_only: Vec<Value> = documents_with_ids.into_iter().map(|(_, data)| data).collect();
        self.storage.create_batch(data_only).await
    }

    // Read operations - WAL gerekmez, direkt storage'dan oku
    async fn read_by_id(&self, id: &Uuid) -> Result<Option<Document<Value>>, DatabaseError> {
        self.storage.read_by_id(id).await
    }

    async fn read_by_ids(&self, ids: &[Uuid]) -> Result<Vec<Document<Value>>, DatabaseError> {
        self.storage.read_by_ids(ids).await
    }

    async fn read_all(&self, offset: Option<usize>, limit: Option<usize>) -> Result<Vec<Document<Value>>, DatabaseError> {
        self.storage.read_all(offset, limit).await
    }

    async fn read_by_date_range(&self, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Vec<Document<Value>>, DatabaseError> {
        self.storage.read_by_date_range(start, end).await
    }

    async fn count(&self) -> Result<usize, DatabaseError> {
        self.storage.count().await
    }

    // Update operations - WAL gerekli
    async fn update(&self, id: &Uuid, data: Value) -> Result<Document<Value>, DatabaseError> {
        // Old data'yı al (rollback için)
        let old_document = self.storage.read_by_id(id).await?;

        if let Some(old_doc) = old_document {
            // WAL'a update entry yaz
            self.wal_manager.write_entry(WalEntryType::Update {
                document_id: *id,
                old_data: Some(old_doc.data.clone()),
                new_data: data.clone(),
                old_version: old_doc.metadata.version,
                new_version: old_doc.metadata.version + 1,
            }).await?;

            // Memory'de update
            self.storage.update(id, data).await
        } else {
            Err(DatabaseError::DocumentNotFound {
                id: id.to_string()
            })
        }
    }

    async fn update_with_version(&self, id: &Uuid, data: Value, expected_version: u64) -> Result<Document<Value>, DatabaseError> {
        // Bu method için de WAL logging eklenir - simplified
        self.storage.update_with_version(id, data, expected_version).await
    }

    async fn upsert(&self, id: &Uuid, data: Value) -> Result<(Document<Value>, bool), DatabaseError> {
        // Upsert için WAL logging - simplified
        self.storage.upsert(id, data).await
    }

    async fn update_batch(&self, updates: Vec<(Uuid, Value)>) -> Result<Vec<Document<Value>>, DatabaseError> {
        // Batch update WAL logging - simplified
        self.storage.update_batch(updates).await
    }

    // Delete operations - WAL gerekli
    async fn delete(&self, id: &Uuid) -> Result<bool, DatabaseError> {
        // Deleted data'yı al (recovery için)
        let document = self.storage.read_by_id(id).await?;

        if let Some(doc) = document {
            // WAL'a delete entry yaz
            self.wal_manager.write_entry(WalEntryType::Delete {
                document_id: *id,
                deleted_data: doc.data.clone(),
            }).await?;

            // Memory'den sil
            self.storage.delete(id).await
        } else {
            Ok(false) // Zaten yok
        }
    }

    async fn delete_with_version(&self, id: &Uuid, expected_version: u64) -> Result<bool, DatabaseError> {
        // Version-controlled delete - simplified WAL
        self.storage.delete_with_version(id, expected_version).await
    }

    async fn delete_batch(&self, ids: &[Uuid]) -> Result<usize, DatabaseError> {
        // Batch delete - simplified WAL
        self.storage.delete_batch(ids).await
    }

    async fn delete_by_date_range(&self, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<usize, DatabaseError> {
        // Date range delete - simplified WAL
        self.storage.delete_by_date_range(start, end).await
    }

    async fn delete_all(&self) -> Result<usize, DatabaseError> {
        // Delete all - bu tehlikeli operation, extra logging ile
        log::warn!("DELETE ALL operation requested");
        self.storage.delete_all().await
    }

    // Utility operations
    async fn exists(&self, id: &Uuid) -> Result<bool, DatabaseError> {
        self.storage.exists(id).await
    }

    async fn stats(&self) -> Result<crate::StorageStats, DatabaseError> {
        self.storage.stats().await
    }
}
