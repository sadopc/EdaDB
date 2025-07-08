use axum::{
    extract::State,
    http::StatusCode,
    response::{Html, Json},
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tower_http::cors::CorsLayer;
use sysinfo::{System, Pid, get_current_pid};
use std::time::Duration;

use crate::{Database, QueryResult};

/// Web server için veritabanı state'i
pub type SharedDatabase = Arc<Mutex<Database>>;

/// Web server için sistem bilgilerini tutmak için shared state
pub type SharedSystem = Arc<Mutex<System>>;

/// /query endpoint'i için request body
#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub sql: String,
}

/// /query endpoint'i için response body
#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub success: bool,
    pub result: Option<QueryResult>,
    pub error: Option<String>,
    pub execution_time_ms: Option<u64>,
}

/// /backup endpoint'i için response body
#[derive(Debug, Serialize)]
pub struct BackupResponse {
    pub success: bool,
    pub backup_data: Option<String>,
    pub filename: Option<String>,
    pub error: Option<String>,
}

/// /api/stats endpoint'i için response body
#[derive(Debug, Serialize)]
pub struct PerformanceStats {
    pub success: bool,
    pub cpu_usage: f32,
    pub memory_usage: f64,
    pub memory_total: u64,
    pub memory_used: u64,
    pub process_memory: u64,
    pub process_cpu: f32,
    pub timestamp: String,
    pub database_stats: Option<DatabaseStats>,
}

/// Veritabanı istatistikleri
#[derive(Debug, Serialize)]
pub struct DatabaseStats {
    pub table_count: usize,
    pub total_rows: usize,
}

/// Axum web server'ı başlatır
pub async fn start_server(db: Database, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let shared_db = Arc::new(Mutex::new(db));
    
    // Sistem bilgilerini tutmak için shared state
    let shared_system = Arc::new(Mutex::new(System::new_all()));
    
    // Sistem bilgilerini düzenli olarak güncelleyen background task
    let system_updater = shared_system.clone();
    tokio::spawn(async move {
        loop {
            {
                let mut sys = system_updater.lock().unwrap();
                sys.refresh_all();
            }
            // CPU bilgilerini doğru almak için bekle
            tokio::time::sleep(Duration::from_millis(100)).await;
            
            // Her 1 saniyede bir güncelle
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
    });
    
    // App state'i oluştur
    let app_state = (shared_db, shared_system);
    
    // Router'ı oluştur
    let app = Router::new()
        .route("/", get(serve_frontend))
        .route("/query", post(handle_query))
        .route("/backup", post(handle_backup))
        .route("/stats", get(serve_stats_page))
        .route("/api/stats", get(handle_performance_stats))
        .layer(CorsLayer::permissive()) // CORS desteği
        .with_state(app_state);

    // Server'ı başlat
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    
    println!("🌐 Web server başlatılıyor...");
    println!("📍 Adres: http://localhost:{}", port);
    println!("🌐 Frontend: http://localhost:{}", port);
    println!("🔍 Performans İzleme: http://localhost:{}/stats", port);
    println!("🔗 Query endpoint: POST http://localhost:{}/query", port);
    println!("📋 Örnek kullanım:");
    println!("   curl -X POST http://localhost:{}/query \\", port);
    println!("        -H \"Content-Type: application/json\" \\");
    println!("        -d '{{\"sql\": \"SELECT * FROM users\"}}'");
    
    axum::serve(listener, app).await?;
    
    Ok(())
}

/// /query endpoint handler'ı
async fn handle_query(
    State((db, _)): State<(SharedDatabase, SharedSystem)>,
    Json(payload): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, StatusCode> {
    // Veritabanı lock'ını al
    let mut database = match db.lock() {
        Ok(db) => db,
        Err(_) => {
            return Ok(Json(QueryResponse {
                success: false,
                result: None,
                error: Some("Database lock error".to_string()),
                execution_time_ms: None,
            }));
        }
    };
    
    // SQL sorgusunu çalıştır
    match database.execute_sql(&payload.sql) {
        Ok(result) => {
            let execution_time_ms = match &result {
                QueryResult::Success { execution_time_ms, .. } => *execution_time_ms,
                QueryResult::Select { execution_time_ms, .. } => *execution_time_ms,
            };
            println!("✅ Sorgu başarılı ({}μs): {}", execution_time_ms, payload.sql);
            Ok(Json(QueryResponse {
                success: true,
                result: Some(result),
                error: None,
                execution_time_ms: Some(execution_time_ms),
            }))
        }
        Err(e) => {
            println!("❌ Sorgu hatası: {} - {}", payload.sql, e);
            Ok(Json(QueryResponse {
                success: false,
                result: None,
                error: Some(e.to_string()),
                execution_time_ms: None,
            }))
        }
    }
}

/// Frontend HTML sayfasını serve et
async fn serve_frontend() -> Html<String> {
    let html = std::fs::read_to_string("web_frontend.html")
        .unwrap_or_else(|_| {
            format!(r#"
<!DOCTYPE html>
<html>
<head>
    <title>Frontend Bulunamadı</title>
    <style>
        body {{ font-family: Arial, sans-serif; text-align: center; padding: 50px; }}
        .error {{ color: #c62828; background: #ffebee; padding: 20px; border-radius: 8px; }}
    </style>
</head>
<body>
    <div class="error">
        <h1>⚠️ Frontend dosyası bulunamadı</h1>
        <p>web_frontend.html dosyasının proje kök dizininde olduğundan emin olun.</p>
        <p>Alternatif olarak POST /query endpoint'ini kullanabilirsiniz.</p>
    </div>
</body>
</html>
            "#)
        });
    
    Html(html)
}

/// /backup endpoint handler'ı
async fn handle_backup(
    State((db, _)): State<(SharedDatabase, SharedSystem)>,
) -> Result<Json<BackupResponse>, StatusCode> {
    // Veritabanı lock'ını al
    let database = match db.lock() {
        Ok(db) => db,
        Err(_) => {
            return Ok(Json(BackupResponse {
                success: false,
                backup_data: None,
                filename: None,
                error: Some("Database lock error".to_string()),
            }));
        }
    };
    
    // Backup verilerini JSON formatında hazırla
    match database.export_dump(None) {
        Ok(_) => {
            // Backup verilerini string olarak hazırla
            let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();
            let filename = format!("database_backup_{}.dbdump.json", timestamp);
            
            // Backup verilerini doğrudan JSON string olarak döndür
            use crate::database::{DatabaseDump, DatabaseMetadata};
            let total_rows = database.tables.values().map(|t| t.get_all_rows().len()).sum();
            
            let metadata = DatabaseMetadata {
                table_count: database.tables.len(),
                total_rows,
                created_at: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string(),
                description: format!("Database backup with {} tables and {} total rows", database.tables.len(), total_rows),
            };
            
            let dump = DatabaseDump {
                version: "1.0".to_string(),
                timestamp: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string(),
                tables: database.tables.clone(),
                metadata,
            };
            
            match serde_json::to_string_pretty(&dump) {
                Ok(json_data) => {
                    println!("✅ Backup oluşturuldu: {}", filename);
                    Ok(Json(BackupResponse {
                        success: true,
                        backup_data: Some(json_data),
                        filename: Some(filename),
                        error: None,
                    }))
                }
                Err(e) => {
                    println!("❌ Backup serialization hatası: {}", e);
                    Ok(Json(BackupResponse {
                        success: false,
                        backup_data: None,
                        filename: None,
                        error: Some(format!("Backup serialization error: {}", e)),
                    }))
                }
            }
        }
        Err(e) => {
            println!("❌ Backup hatası: {}", e);
            Ok(Json(BackupResponse {
                success: false,
                backup_data: None,
                filename: None,
                error: Some(e.to_string()),
            }))
        }
    }
}

/// Health check endpoint (opsiyonel)
#[allow(dead_code)]
async fn health_check() -> &'static str {
    "OK"
}

/// Server istatistikleri (opsiyonel)
#[derive(Debug, Serialize)]
#[allow(dead_code)]
pub struct ServerStats {
    pub tables_count: usize,
    pub total_rows: usize,
}

#[allow(dead_code)]
async fn get_stats(State((db, _)): State<(SharedDatabase, SharedSystem)>) -> Result<Json<ServerStats>, StatusCode> {
    let database = match db.lock() {
        Ok(db) => db,
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };
    
    let tables_count = database.tables.len();
    let total_rows = database.tables.values()
        .map(|table| table.get_all_rows().len())
        .sum();
    
    Ok(Json(ServerStats {
        tables_count,
        total_rows,
    }))
}

/// /stats sayfasını serve et
async fn serve_stats_page() -> Html<String> {
    let html = r#"
<!DOCTYPE html>
<html lang="tr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sistem Performans İzleme</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
            color: #333;
        }

        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 12px;
            box-shadow: 0 15px 35px rgba(0,0,0,0.1);
            overflow: hidden;
        }

        .header {
            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }

        .header h1 {
            font-size: 2.5rem;
            margin-bottom: 10px;
            text-shadow: 0 2px 4px rgba(0,0,0,0.3);
        }

        .header p {
            font-size: 1.1rem;
            opacity: 0.9;
        }

        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            padding: 40px;
        }

        .stat-card {
            background: #fff;
            border-radius: 10px;
            padding: 25px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.1);
            border-left: 4px solid #4CAF50;
            transition: transform 0.3s ease;
        }

        .stat-card:hover {
            transform: translateY(-5px);
        }

        .stat-card h3 {
            color: #333;
            margin-bottom: 15px;
            font-size: 1.3rem;
        }

        .stat-value {
            font-size: 2.5rem;
            font-weight: bold;
            color: #4CAF50;
            margin-bottom: 10px;
        }

        .stat-label {
            font-size: 0.9rem;
            color: #666;
            text-transform: uppercase;
            letter-spacing: 1px;
        }

        .progress-bar {
            width: 100%;
            height: 10px;
            background: #e0e0e0;
            border-radius: 5px;
            overflow: hidden;
            margin-top: 10px;
        }

        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, #4CAF50 0%, #45a049 100%);
            transition: width 0.3s ease;
            border-radius: 5px;
        }

        .timestamp {
            text-align: center;
            padding: 20px;
            background: #f8f9fa;
            color: #666;
            font-size: 0.9rem;
        }

        .status {
            display: inline-block;
            padding: 5px 15px;
            border-radius: 20px;
            font-size: 0.8rem;
            font-weight: bold;
            text-transform: uppercase;
            letter-spacing: 1px;
        }

        .status.online {
            background: #d4edda;
            color: #155724;
        }

        .status.loading {
            background: #fff3cd;
            color: #856404;
        }

        .status.error {
            background: #f8d7da;
            color: #721c24;
        }

        .navigation {
            text-align: center;
            padding: 20px;
        }

        .nav-btn {
            display: inline-block;
            padding: 10px 20px;
            margin: 0 10px;
            background: linear-gradient(135deg, #4CAF50 0%, #45a049 100%);
            color: white;
            text-decoration: none;
            border-radius: 6px;
            transition: all 0.3s;
            font-weight: 600;
        }

        .nav-btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(76, 175, 80, 0.3);
        }

        .cpu-card {
            border-left-color: #ff6b6b;
        }

        .memory-card {
            border-left-color: #4ecdc4;
        }

        .database-card {
            border-left-color: #45b7d1;
        }

        .auto-refresh {
            text-align: center;
            padding: 10px;
            background: #e8f5e8;
            color: #2e7d32;
            font-weight: 600;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔍 Sistem Performans İzleme</h1>
            <p>Gerçek zamanlı CPU, RAM ve Veritabanı istatistikleri</p>
        </div>

        <div class="auto-refresh">
            <span class="status" id="status">Yükleniyor...</span>
            <span id="auto-refresh-text">Otomatik güncelleme: 2 saniye</span>
        </div>

        <div class="stats-grid">
            <div class="stat-card cpu-card">
                <h3>🖥️ CPU Kullanımı</h3>
                <div class="stat-value" id="cpu-usage">--%</div>
                <div class="stat-label">İşlemci Kullanımı</div>
                <div class="progress-bar">
                    <div class="progress-fill" id="cpu-progress"></div>
                </div>
            </div>

            <div class="stat-card memory-card">
                <h3>🧠 RAM Kullanımı</h3>
                <div class="stat-value" id="memory-usage">--%</div>
                <div class="stat-label">Bellek Kullanımı</div>
                <div class="progress-bar">
                    <div class="progress-fill" id="memory-progress"></div>
                </div>
                <div style="margin-top: 10px; font-size: 0.9rem; color: #666;">
                    <span id="memory-details">-- MB / -- MB</span>
                </div>
            </div>

            <div class="stat-card">
                <h3>⚡ Süreç Belleği</h3>
                <div class="stat-value" id="process-memory">-- MB</div>
                <div class="stat-label">Uygulama Bellek Kullanımı</div>
            </div>

            <div class="stat-card database-card">
                <h3>🗄️ Veritabanı</h3>
                <div class="stat-value" id="table-count">--</div>
                <div class="stat-label">Toplam Tablo Sayısı</div>
                <div style="margin-top: 15px;">
                    <div class="stat-value" style="font-size: 1.5rem;" id="total-rows">--</div>
                    <div class="stat-label">Toplam Satır Sayısı</div>
                </div>
            </div>
        </div>

        <div class="timestamp">
            Son güncelleme: <span id="last-update">--</span>
        </div>

        <div class="navigation">
            <a href="/" class="nav-btn">🏠 Ana Sayfa</a>
            <a href="javascript:void(0)" class="nav-btn" onclick="refreshStats()">🔄 Yenile</a>
        </div>
    </div>

    <script>
        let refreshInterval;
        let isLoading = false;

        async function fetchStats() {
            if (isLoading) return;
            
            isLoading = true;
            const statusElement = document.getElementById('status');
            statusElement.textContent = 'Yükleniyor...';
            statusElement.className = 'status loading';

            try {
                const response = await fetch('/api/stats');
                const data = await response.json();
                
                if (data.success) {
                    // CPU bilgilerini güncelle
                    document.getElementById('cpu-usage').textContent = data.cpu_usage.toFixed(1) + '%';
                    document.getElementById('cpu-progress').style.width = data.cpu_usage + '%';
                    
                    // RAM bilgilerini güncelle
                    document.getElementById('memory-usage').textContent = data.memory_usage.toFixed(1) + '%';
                    document.getElementById('memory-progress').style.width = data.memory_usage + '%';
                    document.getElementById('memory-details').textContent = 
                        Math.round(data.memory_used / 1024 / 1024) + ' MB / ' + 
                        Math.round(data.memory_total / 1024 / 1024) + ' MB';
                    
                    // Süreç belleğini güncelle
                    document.getElementById('process-memory').textContent = 
                        Math.round(data.process_memory / 1024 / 1024) + ' MB';
                    
                    // Veritabanı bilgilerini güncelle
                    if (data.database_stats) {
                        document.getElementById('table-count').textContent = data.database_stats.table_count;
                        document.getElementById('total-rows').textContent = data.database_stats.total_rows.toLocaleString();
                    }
                    
                    // Son güncelleme zamanını güncelle
                    document.getElementById('last-update').textContent = new Date().toLocaleString('tr-TR');
                    
                    // Durumu güncelle
                    statusElement.textContent = 'Çevrimiçi';
                    statusElement.className = 'status online';
                } else {
                    throw new Error('Veri alınamadı');
                }
            } catch (error) {
                console.error('Stats fetch error:', error);
                statusElement.textContent = 'Hata';
                statusElement.className = 'status error';
            } finally {
                isLoading = false;
            }
        }

        function refreshStats() {
            fetchStats();
        }

        function startAutoRefresh() {
            // İlk yükleme
            fetchStats();
            
            // 2 saniyede bir güncelle
            refreshInterval = setInterval(fetchStats, 2000);
        }

        function stopAutoRefresh() {
            if (refreshInterval) {
                clearInterval(refreshInterval);
            }
        }

        // Sayfa yüklendiğinde otomatik güncellemeyi başlat
        window.addEventListener('load', startAutoRefresh);
        
        // Sayfa kapatılırken otomatik güncellemeyi durdur
        window.addEventListener('beforeunload', stopAutoRefresh);
        
        // Sayfa görünür olduğunda otomatik güncellemeyi başlat
        document.addEventListener('visibilitychange', function() {
            if (document.hidden) {
                stopAutoRefresh();
            } else {
                startAutoRefresh();
            }
        });
    </script>
</body>
</html>
    "#;
    
    Html(html.to_string())
}

/// /api/stats endpoint handler'ı - performans verilerini döndürür
async fn handle_performance_stats(
    State((db, system)): State<(SharedDatabase, SharedSystem)>,
) -> Result<Json<PerformanceStats>, StatusCode> {
    // Sistem bilgilerini shared state'den al
    let (cpu_usage, memory_total, memory_used, memory_usage, process_memory, process_cpu) = {
        let sys = match system.lock() {
            Ok(sys) => sys,
            Err(_) => {
                return Ok(Json(PerformanceStats {
                    success: false,
                    cpu_usage: 0.0,
                    memory_usage: 0.0,
                    memory_total: 0,
                    memory_used: 0,
                    process_memory: 0,
                    process_cpu: 0.0,
                    timestamp: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string(),
                    database_stats: None,
                }));
            }
        };
        
        // CPU kullanımını al
        let cpu_usage = sys.global_cpu_info().cpu_usage();
        
        // RAM kullanımını al
        let memory_total = sys.total_memory();
        let memory_used = sys.used_memory();
        let memory_usage = (memory_used as f64 / memory_total as f64) * 100.0;
        
        // Mevcut sürecin bilgilerini al
        let current_pid = get_current_pid().unwrap_or(Pid::from(0));
        let process_memory = sys.process(current_pid)
            .map(|p| p.memory())
            .unwrap_or(0);
        let process_cpu = sys.process(current_pid)
            .map(|p| p.cpu_usage())
            .unwrap_or(0.0);
        
        (cpu_usage, memory_total, memory_used, memory_usage, process_memory, process_cpu)
    };
    
    // Veritabanı istatistiklerini al
    let database_stats = match db.lock() {
        Ok(database) => {
            let tables_count = database.tables.len();
            let total_rows = database.tables.values()
                .map(|table| table.get_all_rows().len())
                .sum();
            
            Some(DatabaseStats {
                table_count: tables_count,
                total_rows,
            })
        }
        Err(_) => None,
    };
    
    let timestamp = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string();
    
    Ok(Json(PerformanceStats {
        success: true,
        cpu_usage,
        memory_usage,
        memory_total,
        memory_used,
        process_memory,
        process_cpu,
        timestamp,
        database_stats,
    }))
} 