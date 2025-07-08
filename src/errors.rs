use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum DbError {
    // Parser hataları
    ParseError(String),
    SyntaxError(String),
    
    // Tablo hataları
    TableNotFound(String),
    TableAlreadyExists(String),
    
    // Kolon hataları
    ColumnNotFound(String),
    InvalidColumnCount(usize, usize), // expected, actual
    
    // Tip hataları
    TypeMismatch(String, String), // expected, actual
    InvalidTypeConversion(String),
    
    // Veri hataları
    InvalidValue(String),
    NullConstraintViolation(String),
    
    // Dosya sistemi hataları
    FileSystemError(String),
    SerializationError(String),
    
    // Genel hatalar
    ExecutionError(String),
    InternalError(String),
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::ParseError(msg) => write!(f, "Parse Error: {}", msg),
            DbError::SyntaxError(msg) => write!(f, "Syntax Error: {}", msg),
            DbError::TableNotFound(table) => write!(f, "Table '{}' not found", table),
            DbError::TableAlreadyExists(table) => write!(f, "Table '{}' already exists", table),
            DbError::ColumnNotFound(column) => write!(f, "Column '{}' not found", column),
            DbError::InvalidColumnCount(expected, actual) => {
                write!(f, "Invalid column count: expected {}, got {}", expected, actual)
            }
            DbError::TypeMismatch(expected, actual) => {
                write!(f, "Type mismatch: expected {}, got {}", expected, actual)
            }
            DbError::InvalidTypeConversion(msg) => write!(f, "Invalid type conversion: {}", msg),
            DbError::InvalidValue(msg) => write!(f, "Invalid value: {}", msg),
            DbError::NullConstraintViolation(column) => {
                write!(f, "NULL constraint violation for column '{}'", column)
            }
            DbError::FileSystemError(msg) => write!(f, "File system error: {}", msg),
            DbError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            DbError::ExecutionError(msg) => write!(f, "Execution error: {}", msg),
            DbError::InternalError(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for DbError {}

// String'den DbError'a dönüştürme için From trait
impl From<String> for DbError {
    fn from(msg: String) -> Self {
        DbError::ExecutionError(msg)
    }
}

impl From<&str> for DbError {
    fn from(msg: &str) -> Self {
        DbError::ExecutionError(msg.to_string())
    }
}

// Yaygın hata yaratım fonksiyonları
impl DbError {
    pub fn parse_error(msg: &str) -> Self {
        DbError::ParseError(msg.to_string())
    }
    
    pub fn table_not_found(table_name: &str) -> Self {
        DbError::TableNotFound(table_name.to_string())
    }
    
    pub fn table_already_exists(table_name: &str) -> Self {
        DbError::TableAlreadyExists(table_name.to_string())
    }
    
    pub fn column_not_found(column_name: &str) -> Self {
        DbError::ColumnNotFound(column_name.to_string())
    }
    
    pub fn type_mismatch(expected: &str, actual: &str) -> Self {
        DbError::TypeMismatch(expected.to_string(), actual.to_string())
    }
    
    pub fn invalid_column_count(expected: usize, actual: usize) -> Self {
        DbError::InvalidColumnCount(expected, actual)
    }
    
    pub fn execution_error(msg: &str) -> Self {
        DbError::ExecutionError(msg.to_string())
    }
} 