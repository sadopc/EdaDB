# Müşteri Rapor SQL-like Veritabanı Motoru

Rust ile geliştirilmiş, SQL benzeri komutları destekleyen basit bir veritabanı motoru.

## Özellikler

### ✅ Tamamlanan Adımlar (1-10)

1. **🔧 Temel Veritabanı Çekirdeği** - In-memory taban, SQL benzeri yapı
2. **💾 Disk Persistansı** - Bellekten dosyaya yazma/yükleme
3. **🧱 Veri Tipleri** - Temel tip desteği: String, Integer, Boolean
4. **🧠 SQL Parser** - Küçük ve sınırlı bir nom tabanlı parser
5. **🔄 Sorgu Yürütücü** - AST'den çalıştırıcıya (Executor)
6. **🌐 Web Arayüzü Backend'i** - Axum ile JSON tabanlı API
7. **🖥️ Web Arayüzü Frontend'i** - HTML/JS ile basit UI
8. **🧹 Veri Güncelleme ve Silme** - UPDATE ve DELETE işlemleri
9. **🧪 Basit WHERE desteği** - SELECT, UPDATE, DELETE için filtreleme
10. **💾 Export/Import & Mini CLI** - Veri yedekleme ve etkileşimli CLI

### 🎯 Desteklenen SQL Komutları

- `CREATE TABLE table_name (column1 TYPE, column2 TYPE, ...)`
- `INSERT INTO table_name VALUES (value1, value2, ...)`
- `SELECT * FROM table_name`
- `SELECT column1, column2 FROM table_name`
- `SELECT * FROM table_name WHERE condition`
- `UPDATE table_name SET column = value WHERE condition`
- `DELETE FROM table_name WHERE condition`
- `DROP TABLE table_name`

### 🔍 WHERE Koşulları

- `=` (eşit)
- `!=` (eşit değil)
- `>` (büyük)
- `<` (küçük)
- `>=` (büyük eşit)
- `<=` (küçük eşit)

### 📊 Veri Tipleri

- `INT`/`INTEGER` - Tam sayılar
- `TEXT`/`VARCHAR`/`STRING` - Metin
- `BOOL`/`BOOLEAN` - Mantıksal değerler

## Kullanım

### 🔧 Temel Komutlar

```bash
# Demo/Test modu (varsayılan)
cargo run

# İnteraktif CLI
cargo run cli

# Web server
cargo run web [port]

# Veritabanı export/import
cargo run export <dosya.dbdump.json>
cargo run import <dosya.dbdump.json> [--clear]

# Yardım
cargo run help
```

### 💻 İnteraktif CLI Kullanımı

```bash
# CLI'yi başlat
cargo run cli

# CLI komutları
sql> .help                     # Yardım menüsü
sql> .tables                   # Tablo listesi
sql> .stats                    # Veritabanı istatistikleri
sql> .export backup.dbdump.json # Export
sql> .import backup.dbdump.json # Import
sql> .quit                     # Çıkış

# SQL sorguları direkt yazabilirsiniz
sql> CREATE TABLE users (id INT, name TEXT, active BOOL)
sql> INSERT INTO users VALUES (1, 'Ali', true)
sql> SELECT * FROM users WHERE active = true
```

### 🌐 Web Server Kullanımı

```bash
# Varsayılan port (3000)
cargo run web

# Özel port
cargo run web 8080
```

### 📡 Web API Kullanımı

```bash
# Sorgu gönderme
curl -X POST http://localhost:3000/query \
     -H "Content-Type: application/json" \
     -d '{"sql": "SELECT * FROM users"}'

# Yanıt formatı
{
  "success": true,
  "result": {
    "Select": {
      "columns": ["id", "name", "email"],
      "rows": [["1", "Ali", "ali@example.com"]]
    }
  },
  "error": null
}
```

### 💾 Export/Import Sistemi

```bash
# Tüm veritabanını yedekleme
cargo run export my_backup.dbdump.json

# Yedekten geri yükleme
cargo run import my_backup.dbdump.json

# Mevcut tabloları temizleyerek geri yükleme
cargo run import my_backup.dbdump.json --clear
```

**Dump Dosyası Formatı:**
```json
{
  "version": "1.0",
  "timestamp": "2024-01-01 10:00:00 UTC",
  "tables": { ... },
  "metadata": {
    "table_count": 3,
    "total_rows": 100,
    "created_at": "2024-01-01 10:00:00 UTC",
    "description": "Database dump with 3 tables and 100 total rows"
  }
}
```

## Test

```bash
cargo test
```

## Örnek Kullanım

```sql
-- Tablo oluştur
CREATE TABLE users (id INT, name TEXT, email TEXT, active BOOL)

-- Veri ekle
INSERT INTO users VALUES (1, 'John Doe', 'john@example.com', true)

-- Veri sorgula
SELECT * FROM users
SELECT name, email FROM users
SELECT * FROM users WHERE active = true

-- Veri güncelle
UPDATE users SET active = false WHERE id = 1

-- Veri sil
DELETE FROM users WHERE id = 1

-- Tablo sil
DROP TABLE users
```

## Teknik Detaylar

- **Dil**: Rust 2021 Edition
- **Parser**: nom 7.1
- **Serialization**: serde + serde_json
- **Timestamp**: chrono 0.4
- **Web Server**: Axum 0.7
- **Async Runtime**: tokio 1.0
- **Veri Saklama**: 
  - Tablolar: JSON dosyaları (`data/` dizini)
  - Yedekleme: `.dbdump.json` dosyaları (tek dosya)
- **Bellek**: HashMap tabanlı in-memory çalışma
- **CORS**: tower-http ile tam destek

## Mimari

```
src/
├── main.rs          # Ana program (CLI/Web/Export/Import)
├── lib.rs           # Kütüphane exports
├── database.rs      # Veritabanı yönetimi & Export/Import
├── cli.rs           # İnteraktif CLI arayüzü
├── table.rs         # Tablo yapısı
├── row.rs           # Satır yapısı
├── types.rs         # Veri tipleri
├── parser.rs        # SQL parser (nom)
├── executor.rs      # Sorgu yürütücüsü
├── errors.rs        # Hata yönetimi
└── web.rs           # Web server (Axum)
```

## Sürüm Geçmişi

- **v0.1.0** - Temel veritabanı çekirdeği
- **v0.2.0** - Disk persistansı
- **v0.3.0** - Veri tipleri
- **v0.4.0** - SQL Parser (nom tabanlı)
- **v0.5.0** - Sorgu yürütücüsü (Query Executor)
- **v0.6.0** - Web Backend (Axum + JSON API)
- **v0.7.0** - Web Frontend (HTML/JS UI)
- **v0.8.0** - Veri güncelleme (UPDATE/DELETE)
- **v0.9.0** - WHERE koşulları (filtreleme)
- **v1.0.0** - Export/Import & İnteraktif CLI

Bu proje 10 adımda MVP odaklı, monolitik ama genişlemeye açık yapıda geliştirilmiştir:

1. ✅ **Temel Veritabanı Çekirdeği** - In-memory HashMap tabanlı
2. ✅ **Disk Persistansı** - JSON dosyaları ile kalıcı saklama
3. ✅ **Veri Tipleri** - INT, TEXT, BOOL desteği
4. ✅ **SQL Parser** - nom ile AST tabanlı parsing
5. ✅ **Sorgu Yürütücü** - AST'den işlem çalıştırma
6. ✅ **Web Backend** - Axum ile REST API
7. ✅ **Web Frontend** - HTML/JS ile kullanıcı arayüzü
8. ✅ **Veri Güncelleme** - UPDATE ve DELETE işlemleri
9. ✅ **WHERE Desteği** - Koşullu sorgulama
10. ✅ **Export/Import & CLI** - Veri yedekleme ve etkileşimli CLI

### 🚀 Özellik Özeti

- **Çoklu Arayüz**: CLI, Web UI, REST API
- **Veri Yedekleme**: Tek dosyada export/import
- **Tip Güvenliği**: Rust'ın güvenlik garantileri
- **Hata Yönetimi**: Özelleştirilmiş hata türleri
- **Performans**: In-memory HashMap tabanlı hız
- **Kalıcılık**: JSON dosyaları ile disk saklama
- **Genişletilebilirlik**: Modüler yapı ile kolay geliştirme

## Lisans

MIT License 