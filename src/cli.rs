use crate::database::Database;
use crate::executor::QueryResult;
use std::io::{self, Write, IsTerminal};
use std::path::Path;

/// CLI komutları
#[derive(Debug, Clone)]
pub enum CliCommand {
    /// SQL sorgusu çalıştır
    Sql(String),
    /// Veritabanı dump'ını export et
    Export(String),  // dosya yolu
    /// Veritabanı dump'ını import et
    Import(String, bool), // dosya yolu, clear_existing
    /// Tablo listesini göster
    ListTables,
    /// Veritabanı istatistiklerini göster
    Stats,
    /// Yardım menüsünü göster
    Help,
    /// Programdan çık
    Quit,
}

/// CLI arayüzü
pub struct DatabaseCli {
    database: Database,
}

impl DatabaseCli {
    /// Yeni CLI instance'ı oluştur
    pub fn new() -> Self {
        Self {
            database: Database::new(),
        }
    }
    
    /// Belirtilen veri dizini ile CLI oluştur
    pub fn new_with_directory(data_directory: String) -> Self {
        Self {
            database: Database::new_with_directory(data_directory),
        }
    }
    
    /// Ana CLI döngüsünü başlat
    pub fn run(&mut self) {
        let is_interactive = io::stdin().is_terminal();
        
        if is_interactive {
            self.print_welcome();
        }
        
        loop {
            // Interactive modda prompt göster
            if is_interactive {
                print!("sql> ");
                io::stdout().flush().unwrap();
            }
            
            // Kullanıcı girişini al
            let mut input = String::new();
            match io::stdin().read_line(&mut input) {
                Ok(0) => {
                    // EOF reached (pipe ended)
                    if !is_interactive {
                        break;
                    }
                    continue;
                }
                Ok(_) => {
                    let input = input.trim();
                    
                    // Boş girdi kontrolü
                    if input.is_empty() {
                        continue;
                    }
                    
                    // Semicolon ile ayrılmış komutları parse et
                    let commands: Vec<&str> = input.split(';').collect();
                    for cmd in commands {
                        let cmd = cmd.trim();
                        if cmd.is_empty() {
                            continue;
                        }
                        
                        // Komut parse et ve çalıştır
                        match self.parse_command(cmd) {
                            Ok(CliCommand::Quit) => {
                                if is_interactive {
                                    println!("👋 Güle güle!");
                                }
                                return;
                            }
                            Ok(command) => {
                                if let Err(e) = self.execute_command(command) {
                                    if is_interactive {
                                        eprintln!("❌ Hata: {}", e);
                                    } else {
                                        eprintln!("Error: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                if is_interactive {
                                    eprintln!("❌ Komut parse hatası: {}", e);
                                } else {
                                    eprintln!("Parse error: {}", e);
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    if is_interactive {
                        eprintln!("❌ Girdi okuma hatası: {}", e);
                    }
                    break;
                }
            }
        }
    }
    
    /// Hoş geldin mesajını göster
    fn print_welcome(&self) {
        println!("🚀 SQL-like Veritabanı Motoru - CLI v1.0");
        println!("==========================================");
        println!("📋 Mevcut tablolar: {}", self.database.tables.len());
        println!("💡 Yardım için '.help' yazın, çıkmak için '.quit' yazın");
        println!("💡 SQL sorguları direkt yazabilirsiniz");
        println!();
    }
    
    /// Komut parse et
    fn parse_command(&self, input: &str) -> Result<CliCommand, String> {
        let trimmed = input.trim();
        
        // CLI komutları (. ile başlayan)
        if trimmed.starts_with('.') {
            match trimmed {
                ".help" | ".h" => Ok(CliCommand::Help),
                ".quit" | ".q" | ".exit" => Ok(CliCommand::Quit),
                ".tables" | ".t" => Ok(CliCommand::ListTables),
                ".stats" | ".s" => Ok(CliCommand::Stats),
                _ => {
                    // Export/Import komutları
                    if trimmed.starts_with(".export ") {
                        let file_path = trimmed[8..].trim();
                        if file_path.is_empty() {
                            return Err("Export dosya yolu belirtilmeli: .export <dosya_yolu>".to_string());
                        }
                        Ok(CliCommand::Export(file_path.to_string()))
                    } else if trimmed.starts_with(".import ") {
                        let args = trimmed[8..].trim();
                        let parts: Vec<&str> = args.split_whitespace().collect();
                        
                        if parts.is_empty() {
                            return Err("Import dosya yolu belirtilmeli: .import <dosya_yolu> [--clear]".to_string());
                        }
                        
                        let file_path = parts[0].to_string();
                        let clear_existing = parts.len() > 1 && parts[1] == "--clear";
                        
                        Ok(CliCommand::Import(file_path, clear_existing))
                    } else {
                        Err(format!("Bilinmeyen komut: {}", trimmed))
                    }
                }
            }
        } else {
            // SQL sorgusu
            Ok(CliCommand::Sql(trimmed.to_string()))
        }
    }
    
    /// Komutu çalıştır
    fn execute_command(&mut self, command: CliCommand) -> Result<(), String> {
        match command {
            CliCommand::Sql(sql) => {
                self.execute_sql(&sql)
            }
            CliCommand::Export(file_path) => {
                self.export_database(&file_path)
            }
            CliCommand::Import(file_path, clear_existing) => {
                self.import_database(&file_path, clear_existing)
            }
            CliCommand::ListTables => {
                self.list_tables()
            }
            CliCommand::Stats => {
                self.show_stats()
            }
            CliCommand::Help => {
                self.show_help()
            }
            CliCommand::Quit => {
                // Bu duruma gelmez, run() içinde handle edilir
                Ok(())
            }
        }
    }
    
    /// SQL sorgusu çalıştır
    fn execute_sql(&mut self, sql: &str) -> Result<(), String> {
        match self.database.execute_sql(sql) {
            Ok(QueryResult::Success { message, execution_time_ms }) => {
                println!("✅ {} ({}μs)", message, execution_time_ms);
                Ok(())
            }
            Ok(QueryResult::Select { columns, rows, execution_time_ms }) => {
                self.print_table_result(&columns, &rows);
                println!("⏱️ Sorgu süresi: {}μs", execution_time_ms);
                Ok(())
            }
            Err(e) => {
                Err(e.to_string())
            }
        }
    }
    
    /// Veritabanını export et
    fn export_database(&self, file_path: &str) -> Result<(), String> {
        println!("📤 Veritabanı export ediliyor...");
        
        match self.database.export_dump(Some(file_path)) {
            Ok(exported_path) => {
                println!("✅ Veritabanı export edildi: {}", exported_path);
                println!("📊 {} tablo, {} toplam satır", 
                    self.database.tables.len(),
                    self.database.tables.values().map(|t| t.get_all_rows().len()).sum::<usize>()
                );
                Ok(())
            }
            Err(e) => {
                Err(format!("Export hatası: {}", e))
            }
        }
    }
    
    /// Veritabanını import et
    fn import_database(&mut self, file_path: &str, clear_existing: bool) -> Result<(), String> {
        if !Path::new(file_path).exists() {
            return Err(format!("Dosya bulunamadı: {}", file_path));
        }
        
        println!("📥 Veritabanı import ediliyor...");
        if clear_existing {
            println!("⚠️  Mevcut tablolar temizlenecek!");
        }
        
        match self.database.import_dump(file_path, clear_existing) {
            Ok(metadata) => {
                println!("✅ Import tamamlandı");
                println!("📊 {} tablo, {} toplam satır", metadata.table_count, metadata.total_rows);
                Ok(())
            }
            Err(e) => {
                Err(format!("Import hatası: {}", e))
            }
        }
    }
    
    /// Tabloları listele
    fn list_tables(&self) -> Result<(), String> {
        if self.database.tables.is_empty() {
            println!("📋 Henüz tablo yok");
            return Ok(());
        }
        
        println!("📋 Mevcut tablolar ({} adet):", self.database.tables.len());
        println!("┌─────────────────────┬─────────┬─────────┐");
        println!("│ Tablo Adı           │ Sütun   │ Satır   │");
        println!("├─────────────────────┼─────────┼─────────┤");
        
        for (name, table) in &self.database.tables {
            println!("│ {:19} │ {:7} │ {:7} │", 
                name, 
                table.get_columns().len(), 
                table.get_all_rows().len()
            );
        }
        
        println!("└─────────────────────┴─────────┴─────────┘");
        Ok(())
    }
    
    /// Veritabanı istatistiklerini göster
    fn show_stats(&self) -> Result<(), String> {
        let stats = self.database.get_stats();
        
        println!("📊 Veritabanı İstatistikleri:");
        println!("═══════════════════════════");
        println!("📁 Toplam Tablo: {}", stats.get("table_count").unwrap_or(&serde_json::Value::Number(0.into())));
        println!("📄 Toplam Satır: {}", stats.get("total_rows").unwrap_or(&serde_json::Value::Number(0.into())));
        println!();
        
        if let Some(serde_json::Value::Object(tables)) = stats.get("tables") {
            println!("📋 Tablo Detayları:");
            for (table_name, info) in tables {
                if let serde_json::Value::Object(table_info) = info {
                    println!("  🔹 {}", table_name);
                    println!("     Sütun: {}", table_info.get("columns").unwrap_or(&serde_json::Value::Number(0.into())));
                    println!("     Satır: {}", table_info.get("rows").unwrap_or(&serde_json::Value::Number(0.into())));
                    
                    if let Some(serde_json::Value::Array(schema)) = table_info.get("schema") {
                        println!("     Şema: {}", schema.iter()
                            .map(|v| v.as_str().unwrap_or("unknown"))
                            .collect::<Vec<_>>()
                            .join(", "));
                    }
                    println!();
                }
            }
        }
        
        Ok(())
    }
    
    /// Yardım menüsünü göster
    fn show_help(&self) -> Result<(), String> {
        println!("🆘 Yardım - SQL-like Veritabanı Motoru CLI");
        println!("═══════════════════════════════════════════");
        println!();
        println!("🔸 CLI Komutları:");
        println!("  .help, .h              - Bu yardım menüsünü göster");
        println!("  .quit, .q, .exit       - Programdan çık");
        println!("  .tables, .t            - Tablo listesini göster");
        println!("  .stats, .s             - Veritabanı istatistiklerini göster");
        println!("  .export <dosya>        - Veritabanını export et");
        println!("  .import <dosya>        - Veritabanını import et");
        println!("  .import <dosya> --clear - Import et (mevcut tabloları temizle)");
        println!();
        println!("🔸 SQL Komutları:");
        println!("  CREATE TABLE name (col1 TYPE, col2 TYPE, ...)");
        println!("  INSERT INTO name VALUES (val1, val2, ...)");
        println!("  SELECT * FROM name");
        println!("  SELECT col1, col2 FROM name");
        println!("  SELECT * FROM name WHERE col = value");
        println!("  UPDATE name SET col = value WHERE condition");
        println!("  DELETE FROM name WHERE condition");
        println!("  DROP TABLE name");
        println!();
        println!("🔸 Veri Tipleri:");
        println!("  INT, INTEGER  - Tam sayı");
        println!("  TEXT, VARCHAR - Metin");
        println!("  BOOL, BOOLEAN - Mantıksal (true/false)");
        println!();
        println!("🔸 WHERE Operatörleri:");
        println!("  =, !=, >, <, >=, <=");
        println!("  AND, OR (gelecek sürümlerde)");
        println!();
        println!("🔸 Örnek Kullanım:");
        println!("  CREATE TABLE users (id INT, name TEXT, active BOOL)");
        println!("  INSERT INTO users VALUES (1, 'Ali', true)");
        println!("  SELECT * FROM users WHERE active = true");
        println!("  .export my_backup.dbdump.json");
        
        Ok(())
    }
    
    /// Tablo sonuçlarını güzel formatta göster
    fn print_table_result(&self, columns: &[String], rows: &[Vec<String>]) {
        if columns.is_empty() {
            println!("📋 Boş sonuç");
            return;
        }
        
        // Kolon genişliklerini hesapla
        let mut widths = columns.iter().map(|s| s.len()).collect::<Vec<_>>();
        
        for row in rows {
            for (i, cell) in row.iter().enumerate() {
                if i < widths.len() {
                    widths[i] = widths[i].max(cell.len());
                }
            }
        }
        
        // Minimum genişlik 8
        for width in &mut widths {
            *width = (*width).max(8);
        }
        
        // Üst çizgi
        print!("┌");
        for (i, width) in widths.iter().enumerate() {
            if i > 0 { print!("┬"); }
            print!("{}", "─".repeat(width + 2));
        }
        println!("┐");
        
        // Kolon başlıkları
        print!("│");
        for (i, (column, width)) in columns.iter().zip(widths.iter()).enumerate() {
            if i > 0 { print!("│"); }
            print!(" {:width$} ", column, width = width);
        }
        println!("│");
        
        // Orta çizgi
        print!("├");
        for (i, width) in widths.iter().enumerate() {
            if i > 0 { print!("┼"); }
            print!("{}", "─".repeat(width + 2));
        }
        println!("┤");
        
        // Satırlar
        for row in rows {
            print!("│");
            for (i, (cell, width)) in row.iter().zip(widths.iter()).enumerate() {
                if i > 0 { print!("│"); }
                print!(" {:width$} ", cell, width = width);
            }
            println!("│");
        }
        
        // Alt çizgi
        print!("└");
        for (i, width) in widths.iter().enumerate() {
            if i > 0 { print!("┴"); }
            print!("{}", "─".repeat(width + 2));
        }
        println!("┘");
        
        println!("📊 {} satır döndürüldü", rows.len());
    }
} 