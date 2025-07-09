use serde::{Deserialize, Serialize};
use std::fmt;

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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TypedValue {
    Integer(i64),
    Text(String),
    Boolean(bool),
    Null,
}

impl PartialOrd for TypedValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TypedValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (self, other) {
            // Null is always the smallest
            (TypedValue::Null, TypedValue::Null) => Ordering::Equal,
            (TypedValue::Null, _) => Ordering::Less,
            (_, TypedValue::Null) => Ordering::Greater,
            
            // Boolean comparisons
            (TypedValue::Boolean(a), TypedValue::Boolean(b)) => a.cmp(b),
            
            // Integer comparisons
            (TypedValue::Integer(a), TypedValue::Integer(b)) => a.cmp(b),
            
            // Text comparisons
            (TypedValue::Text(a), TypedValue::Text(b)) => a.cmp(b),
            
            // Cross-type comparisons: Boolean < Integer < Text
            (TypedValue::Boolean(_), TypedValue::Integer(_)) => Ordering::Less,
            (TypedValue::Boolean(_), TypedValue::Text(_)) => Ordering::Less,
            (TypedValue::Integer(_), TypedValue::Boolean(_)) => Ordering::Greater,
            (TypedValue::Integer(_), TypedValue::Text(_)) => Ordering::Less,
            (TypedValue::Text(_), TypedValue::Boolean(_)) => Ordering::Greater,
            (TypedValue::Text(_), TypedValue::Integer(_)) => Ordering::Greater,
        }
    }
}

impl fmt::Display for TypedValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TypedValue::Integer(i) => write!(f, "{}", i),
            TypedValue::Text(s) => write!(f, "{}", s),
            TypedValue::Boolean(b) => write!(f, "{}", b),
            TypedValue::Null => write!(f, "NULL"),
        }
    }
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



    pub fn get_type(&self) -> Option<DataType> {
        match self {
            TypedValue::Integer(_) => Some(DataType::INT),
            TypedValue::Text(_) => Some(DataType::TEXT),
            TypedValue::Boolean(_) => Some(DataType::BOOL),
            TypedValue::Null => None,
        }
    }
} 