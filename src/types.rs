use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    INT,
    TEXT,
    BOOL,
}

impl DataType {
    pub fn from_string(s: &str) -> Result<Self, String> {
        match s.to_uppercase().as_str() {
            "INT" | "INTEGER" => Ok(DataType::INT),
            "TEXT" | "VARCHAR" | "STRING" => Ok(DataType::TEXT),
            "BOOL" | "BOOLEAN" => Ok(DataType::BOOL),
            _ => Err(format!("Unsupported data type: {}", s)),
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            DataType::INT => "INT".to_string(),
            DataType::TEXT => "TEXT".to_string(),
            DataType::BOOL => "BOOL".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

impl Column {
    pub fn new(name: String, data_type: DataType) -> Self {
        Self { name, data_type }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TypedValue {
    Integer(i64),
    Text(String),
    Boolean(bool),
    Null,
}

impl TypedValue {
    pub fn from_string(s: &str, data_type: &DataType) -> Result<Self, String> {
        match data_type {
            DataType::INT => {
                if s.is_empty() || s.to_uppercase() == "NULL" {
                    Ok(TypedValue::Null)
                } else {
                    s.parse::<i64>()
                        .map(TypedValue::Integer)
                        .map_err(|_| format!("Cannot parse '{}' as integer", s))
                }
            }
            DataType::TEXT => {
                if s.to_uppercase() == "NULL" {
                    Ok(TypedValue::Null)
                } else {
                    Ok(TypedValue::Text(s.to_string()))
                }
            }
            DataType::BOOL => {
                if s.is_empty() || s.to_uppercase() == "NULL" {
                    Ok(TypedValue::Null)
                } else {
                    match s.to_uppercase().as_str() {
                        "TRUE" | "1" | "YES" => Ok(TypedValue::Boolean(true)),
                        "FALSE" | "0" | "NO" => Ok(TypedValue::Boolean(false)),
                        _ => Err(format!("Cannot parse '{}' as boolean", s)),
                    }
                }
            }
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            TypedValue::Integer(i) => i.to_string(),
            TypedValue::Text(s) => s.clone(),
            TypedValue::Boolean(b) => b.to_string(),
            TypedValue::Null => "NULL".to_string(),
        }
    }

    pub fn get_type(&self) -> Option<DataType> {
        match self {
            TypedValue::Integer(_) => Some(DataType::INT),
            TypedValue::Text(_) => Some(DataType::TEXT),
            TypedValue::Boolean(_) => Some(DataType::BOOL),
            TypedValue::Null => None,
        }
    }
} 