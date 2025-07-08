use musterirapor::{Database, DatabaseCli, QueryResult, start_server};
use std::env;

#[tokio::main]
async fn main() {
    println!("🚀 SQL-like Veritabanı Motoru - v1.0.0 (Export/Import & CLI)");
    println!("=============================================================");
    
    let args: Vec<String> = env::args().collect();
    
    // Komut satırı argümanlarını kontrol et
    if args.len() > 1 {
        match args[1].as_str() {
            "web" => {
                // Web server modunda çalıştır
                let port = args.get(2)
                    .and_then(|p| p.parse::<u16>().ok())
                    .unwrap_or(3000);
                
                println!("🌐 Web server modunda başlatılıyor...");
                let db = Database::new();
                
                if let Err(e) = start_server(db, port).await {
                    eprintln!("❌ Web server hatası: {}", e);
                }
                return;
            }
            "cli" => {
                // İnteraktif CLI modunda çalıştır
                println!("💻 İnteraktif CLI modunda başlatılıyor...");
                let mut cli = DatabaseCli::new();
                cli.run();
                return;
            }
            "export" => {
                // Export komutu
                if args.len() < 3 {
                    eprintln!("❌ Kullanım: {} export <dosya_yolu>", args[0]);
                    return;
                }
                
                let file_path = &args[2];
                let db = Database::new();
                
                match db.export_dump(Some(file_path)) {
                    Ok(exported_path) => {
                        println!("✅ Veritabanı export edildi: {}", exported_path);
                        println!("📊 {} tablo, {} toplam satır", 
                            db.tables.len(),
                            db.tables.values().map(|t| t.get_all_rows().len()).sum::<usize>()
                        );
                    }
                    Err(e) => {
                        eprintln!("❌ Export hatası: {}", e);
                    }
                }
                return;
            }
            "import" => {
                // Import komutu
                if args.len() < 3 {
                    eprintln!("❌ Kullanım: {} import <dosya_yolu> [--clear]", args[0]);
                    return;
                }
                
                let file_path = &args[2];
                let clear_existing = args.get(3).map(|s| s == "--clear").unwrap_or(false);
                
                let mut db = Database::new();
                
                match db.import_dump(file_path, clear_existing) {
                    Ok(metadata) => {
                        println!("✅ Import tamamlandı");
                        println!("📊 {} tablo, {} toplam satır", metadata.table_count, metadata.total_rows);
                    }
                    Err(e) => {
                        eprintln!("❌ Import hatası: {}", e);
                    }
                }
                return;
            }
            "help" | "--help" | "-h" => {
                print_help(&args[0]);
                return;
            }
            _ => {
                eprintln!("❌ Bilinmeyen komut: {}", args[1]);
                print_help(&args[0]);
                return;
            }
        }
    }
    
    // Varsayılan mod - Demo/Test modu
    println!("🧪 Demo/Test modunda çalışıyor...");
    println!("ℹ️  Kullanım örnekleri:");
    println!("  {} cli                    - İnteraktif CLI", args[0]);
    println!("  {} web [port]             - Web server (varsayılan port: 3000)", args[0]);
    println!("  {} export <dosya>         - Veritabanını export et", args[0]);
    println!("  {} import <dosya> [--clear] - Veritabanını import et", args[0]);
    println!("  {} help                   - Yardım menüsü", args[0]);
    println!();
    
    let mut db = Database::new();
    
    // Tablo oluşturma testleri (sadece mevcut değilse)
    println!("📋 Tablolar kontrol ediliyor...");
    ensure_demo_tables(&mut db);
    
    // Mevcut tabloları listele
    list_current_tables(&db);
    
    // Örnek veri ekleme
    let user_count = db.tables.get("users").map(|t| t.get_all_rows().len()).unwrap_or(0);
    if user_count == 0 {
        println!("➕ Örnek veri ekleniyor...");
        add_demo_data(&mut db);
        
        // Gelişmiş işlemler testi
        println!("🧪 Gelişmiş SQL işlemleri testi...");
        test_advanced_operations(&mut db);
        
        // Hata yönetimi testi
        println!("🧪 Hata yönetimi testi...");
        test_error_handling(&mut db);
    } else {
        println!("📊 Mevcut veriler korunuyor (yeni veri eklenmedi)");
    }
    
    // Güncellenmiş verileri göster
    println!("📊 Tablolardan veri okuma:");
    display_table_data(&mut db);
    
    // Export/Import testi
    println!("🧪 Export/Import testi...");
    test_export_import(&mut db);
    
    // Özellik özeti
    println!("🎯 Desteklenen Özellikler:");
    print_features();
    
    println!("\n✨ Demo tamamlandı! İnteraktif CLI için 'cargo run cli' komutunu kullanın.");
}

fn print_help(program_name: &str) {
    println!("🆘 SQL-like Veritabanı Motoru - Yardım");
    println!("======================================");
    println!();
    println!("🔸 Kullanım:");
    println!("  {} cli                    - İnteraktif CLI modunda çalıştır", program_name);
    println!("  {} web [port]             - Web server modunda çalıştır (varsayılan: 3000)", program_name);
    println!("  {} export <dosya>         - Veritabanını .dbdump.json dosyasına export et", program_name);
    println!("  {} import <dosya> [--clear] - .dbdump.json dosyasından import et", program_name);
    println!("  {} help                   - Bu yardım menüsünü göster", program_name);
    println!();
    println!("🔸 Örnekler:");
    println!("  {} cli                           # İnteraktif CLI başlat", program_name);
    println!("  {} web 8080                      # Web server'ı 8080 portunda çalıştır", program_name);
    println!("  {} export my_backup.dbdump.json  # Veritabanını yedekle", program_name);
    println!("  {} import my_backup.dbdump.json  # Veritabanını geri yükle", program_name);
    println!("  {} import backup.dbdump.json --clear # Mevcut tabloları temizleyerek import et", program_name);
    println!();
    println!("🔸 Desteklenen SQL:");
    println!("  CREATE, INSERT, SELECT, UPDATE, DELETE, DROP TABLE");
    println!("  WHERE koşulları: =, !=, >, <, >=, <=");
    println!("  Veri tipleri: INT, TEXT, BOOL");
    println!();
    println!("🔸 Veri Saklama:");
    println!("  • Tablolar: data/*.json (ayrı dosyalar)");
    println!("  • Yedekleme: *.dbdump.json (tek dosya)");
    println!("  • Bellekte: HashMap tabanlı in-memory çalışma");
}

fn ensure_demo_tables(db: &mut Database) {
    let tables_to_create = vec![
        ("users", "CREATE TABLE users (id INT, name TEXT, email TEXT)"),
        ("products", "CREATE TABLE products (id INT, name TEXT, price INT)"),
        ("settings", "CREATE TABLE settings (key TEXT, value TEXT, active BOOL)"),
    ];
    
    for (table_name, sql) in tables_to_create {
        if !db.tables.contains_key(table_name) {
            println!("  📋 {} tablosu oluşturuluyor...", table_name);
            match db.execute_sql(sql) {
                Ok(_) => println!("  ✅ {} tablosu oluşturuldu", table_name),
                Err(e) => println!("  ❌ {} tablosu oluşturulamadı: {}", table_name, e),
            }
        }
    }
}

fn list_current_tables(db: &Database) {
    println!("📋 Mevcut tablolar:");
    if db.tables.is_empty() {
        println!("  Henüz tablo yok");
    } else {
        for (name, table) in &db.tables {
            println!("  📁 {}: {} sütun, {} satır", name, table.get_columns().len(), table.get_all_rows().len());
            
            // Kolon bilgilerini göster
            print!("      Kolonlar: ");
            for (i, column) in table.get_columns().iter().enumerate() {
                if i > 0 { print!(", "); }
                print!("{} ({})", column.name, column.data_type.to_string());
            }
            println!();
        }
    }
}

fn add_demo_data(db: &mut Database) {
    let demo_data = vec![
        "INSERT INTO users VALUES (1, 'Ali', 'ali@example.com')",
        "INSERT INTO users VALUES (2, 'Ayşe', 'ayse@example.com')",
        "INSERT INTO products VALUES (1, 'Laptop', 15000)",
        "INSERT INTO products VALUES (2, 'Mouse', 250)",
        "INSERT INTO settings VALUES ('debug_mode', 'enabled', true)",
        "INSERT INTO settings VALUES ('maintenance', 'scheduled', false)",
    ];
    
    for sql in demo_data {
        match db.execute_sql(sql) {
            Ok(_) => println!("  ✅ Veri eklendi: {}", sql.split("VALUES").next().unwrap_or("").trim()),
            Err(e) => println!("  ❌ Veri eklenemedi: {}", e),
        }
    }
}

fn test_advanced_operations(db: &mut Database) {
    let operations = vec![
        ("UPDATE users SET email = 'ali.yeni@example.com' WHERE id = 1", "UPDATE testi"),
        ("SELECT name FROM users", "SELECT belirli kolon testi"),
        ("DELETE FROM products WHERE id = 2", "DELETE testi"),
    ];
    
    for (sql, description) in operations {
        match db.execute_sql(sql) {
            Ok(result) => println!("  ✅ {}: {:?}", description, result),
            Err(e) => println!("  ❌ {} başarısız: {}", description, e),
        }
    }
}

fn test_error_handling(db: &mut Database) {
    let error_tests = vec![
        ("INSERT INTO users VALUES ('invalid_id', 'Test', 'test@example.com')", "Hatalı tip testi"),
        ("SELECT * FROM nonexistent_table", "Olmayan tablo testi"),
        ("INSERT INTO users VALUES (3, 'Mehmet')", "Eksik değer testi"),
    ];
    
    for (sql, description) in error_tests {
        match db.execute_sql(sql) {
            Ok(_) => println!("  ❌ {} başarılı oldu (hata olmalıydı!)", description),
            Err(e) => println!("  ✅ {} reddedildi: {}", description, e),
        }
    }
}

fn display_table_data(db: &mut Database) {
    let tables = vec!["users", "products", "settings"];
    
    for table_name in tables {
        match db.execute_sql(&format!("SELECT * FROM {}", table_name)) {
            Ok(QueryResult::Select { columns, rows, execution_time_ms }) => {
                println!("📊 {} tablosu ({}μs):", table_name, execution_time_ms);
                println!("   Columns: {:?}", columns);
                for (i, row) in rows.iter().enumerate() {
                    println!("   Row {}: {:?}", i + 1, row);
                }
            }
            Ok(result) => println!("  ⚠️  Beklenmeyen sonuç: {:?}", result),
            Err(e) => println!("  ❌ {} tablosu okunamadı: {}", table_name, e),
        }
    }
}

fn test_export_import(db: &mut Database) {
    // Export testi
    println!("  📤 Export testi...");
    match db.export_dump(Some("test_backup.dbdump.json")) {
        Ok(path) => println!("  ✅ Export başarılı: {}", path),
        Err(e) => println!("  ❌ Export hatası: {}", e),
    }
    
    // Import testi (yeni bir veritabanı instance'ı ile)
    println!("  📥 Import testi...");
    let mut new_db = Database::new_with_directory("test_data".to_string());
    
    match new_db.import_dump("test_backup.dbdump.json", true) {
        Ok(metadata) => {
            println!("  ✅ Import başarılı: {} tablo, {} satır", metadata.table_count, metadata.total_rows);
            
                         // Import edilen verileri kontrol et
             match new_db.execute_sql("SELECT * FROM users") {
                 Ok(QueryResult::Select { columns: _, rows, execution_time_ms }) => {
                     println!("  📊 Import edilen users tablosu: {} satır ({}μs)", rows.len(), execution_time_ms);
                 }
                 Ok(_) => println!("  ⚠️  Beklenmeyen sonuç"),
                 Err(e) => println!("  ❌ Import kontrolü hatası: {}", e),
             }
        }
        Err(e) => println!("  ❌ Import hatası: {}", e),
    }
    
    // Test dosyasını temizle
    if let Err(e) = std::fs::remove_file("test_backup.dbdump.json") {
        println!("  ⚠️  Test dosyası temizlenemedi: {}", e);
    }
    
    // Test veri dizinini temizle
    if let Err(e) = std::fs::remove_dir_all("test_data") {
        println!("  ⚠️  Test dizini temizlenemedi: {}", e);
    }
}

fn print_features() {
    println!("✅ Temel Veritabanı Çekirdeği - HashMap tabanlı in-memory");
    println!("✅ Disk Persistansı - JSON dosyaları (data/ dizini)");
    println!("✅ Veri Tipleri - INT, TEXT, BOOL");
    println!("✅ SQL Parser - nom tabanlı parser");
    println!("✅ Sorgu Yürütücü - AST yorumlayıcı");
    println!("✅ Web Backend - Axum ile JSON API");
    println!("✅ Veri Güncelleme - UPDATE ve DELETE");
    println!("✅ WHERE Desteği - Filtreleme işlemleri");
    println!("✅ Export/Import - .dbdump.json formatında");
    println!("✅ İnteraktif CLI - Komut satırı arayüzü");
    println!("✅ Özelleştirilmiş Hata Yönetimi");
    println!("✅ CORS Desteği - Web frontend için");
} 