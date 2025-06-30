use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use regex::Regex;
use crate::schema::format::Format;

/// Supported field types in JSON Schema
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldType {
    String,
    Number,
    Integer, 
    Boolean,
    Array,
    Object,
    Null,
    /// Union type - value can be any of the specified types
    Union(Vec<FieldType>),
}

impl FieldType {
    pub fn as_str(&self) -> &str {
        match self {
            FieldType::String => "string",
            FieldType::Number => "number", 
            FieldType::Integer => "integer",
            FieldType::Boolean => "boolean",
            FieldType::Array => "array",
            FieldType::Object => "object",
            FieldType::Null => "null",
            FieldType::Union(_) => "union",
        }
    }

    /// Check if a JSON value matches this field type
    pub fn matches_value(&self, value: &Value) -> bool {
        match self {
            FieldType::String => value.is_string(),
            FieldType::Number => value.is_number(),
            FieldType::Integer => value.is_i64() || value.is_u64(),
            FieldType::Boolean => value.is_boolean(),
            FieldType::Array => value.is_array(),
            FieldType::Object => value.is_object(),
            FieldType::Null => value.is_null(),
            FieldType::Union(types) => types.iter().any(|t| t.matches_value(value)),
        }
    }

    /// Get the actual type name from a JSON value
    pub fn from_value(value: &Value) -> String {
        match value {
            Value::String(_) => "string".to_string(),
            Value::Number(n) => {
                if n.is_i64() || n.is_u64() {
                    "integer".to_string()
                } else {
                    "number".to_string()
                }
            }
            Value::Bool(_) => "boolean".to_string(),
            Value::Array(_) => "array".to_string(),
            Value::Object(_) => "object".to_string(),
            Value::Null => "null".to_string(),
        }
    }
}

/// Numeric constraints (for number and integer types)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NumericConstraint {
    pub minimum: Option<f64>,
    pub maximum: Option<f64>,
    pub exclusive_minimum: Option<f64>,
    pub exclusive_maximum: Option<f64>,
    pub multiple_of: Option<f64>,
}

impl NumericConstraint {
    pub fn new() -> Self {
        Self {
            minimum: None,
            maximum: None,
            exclusive_minimum: None,
            exclusive_maximum: None,
            multiple_of: None,
        }
    }

    pub fn min(mut self, min: f64) -> Self {
        self.minimum = Some(min);
        self
    }

    pub fn max(mut self, max: f64) -> Self {
        self.maximum = Some(max);
        self
    }

    pub fn exclusive_min(mut self, min: f64) -> Self {
        self.exclusive_minimum = Some(min);
        self
    }

    pub fn exclusive_max(mut self, max: f64) -> Self {
        self.exclusive_maximum = Some(max);
        self
    }

    pub fn multiple_of(mut self, multiple: f64) -> Self {
        self.multiple_of = Some(multiple);
        self
    }

    /// Validate a numeric value against these constraints
    pub fn validate(&self, value: f64) -> Vec<String> {
        let mut errors = Vec::new();

        if let Some(min) = self.minimum {
            if value < min {
                errors.push(format!("Value {} is less than minimum {}", value, min));
            }
        }

        if let Some(max) = self.maximum {
            if value > max {
                errors.push(format!("Value {} is greater than maximum {}", value, max));
            }
        }

        if let Some(exclusive_min) = self.exclusive_minimum {
            if value <= exclusive_min {
                errors.push(format!("Value {} is not greater than exclusive minimum {}", value, exclusive_min));
            }
        }

        if let Some(exclusive_max) = self.exclusive_maximum {
            if value >= exclusive_max {
                errors.push(format!("Value {} is not less than exclusive maximum {}", value, exclusive_max));
            }
        }

        if let Some(multiple) = self.multiple_of {
            if multiple != 0.0 && (value % multiple).abs() > f64::EPSILON {
                errors.push(format!("Value {} is not a multiple of {}", value, multiple));
            }
        }

        errors
    }
}

impl Default for NumericConstraint {
    fn default() -> Self {
        Self::new()
    }
}

/// String constraints  
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StringConstraint {
    pub min_length: Option<usize>,
    pub max_length: Option<usize>,
    pub pattern: Option<String>,
    #[serde(skip)]
    pub pattern_regex: Option<Regex>,
    pub format: Option<Format>,
}

impl StringConstraint {
    pub fn new() -> Self {
        Self {
            min_length: None,
            max_length: None,
            pattern: None,
            pattern_regex: None,
            format: None,
        }
    }

    pub fn min_length(mut self, min: usize) -> Self {
        self.min_length = Some(min);
        self
    }

    pub fn max_length(mut self, max: usize) -> Self {
        self.max_length = Some(max);
        self
    }

    pub fn pattern(mut self, pattern: &str) -> Result<Self, regex::Error> {
        self.pattern = Some(pattern.to_string());
        self.pattern_regex = Some(Regex::new(pattern)?);
        Ok(self)
    }

    pub fn format(mut self, format: Format) -> Self {
        self.format = Some(format);
        self
    }

    /// Validate a string value against these constraints
    pub fn validate(&self, value: &str) -> Vec<String> {
        let mut errors = Vec::new();

        if let Some(min_len) = self.min_length {
            if value.len() < min_len {
                errors.push(format!("String length {} is less than minimum {}", value.len(), min_len));
            }
        }

        if let Some(max_len) = self.max_length {
            if value.len() > max_len {
                errors.push(format!("String length {} is greater than maximum {}", value.len(), max_len));
            }
        }

        if let Some(regex) = &self.pattern_regex {
            if !regex.is_match(value) {
                errors.push(format!("String does not match required pattern"));
            }
        }

        errors
    }
}

impl PartialEq for StringConstraint {
    fn eq(&self, other: &Self) -> bool {
        self.min_length == other.min_length
            && self.max_length == other.max_length
            && self.pattern == other.pattern
            && self.format == other.format
    }
}

impl Default for StringConstraint {
    fn default() -> Self {
        Self::new()
    }
}

/// Array constraints
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArrayConstraint {
    pub min_items: Option<usize>,
    pub max_items: Option<usize>,
    pub unique_items: bool,
    pub items_schema: Option<Box<FieldSchema>>,
}

impl ArrayConstraint {
    pub fn new() -> Self {
        Self {
            min_items: None,
            max_items: None,
            unique_items: false,
            items_schema: None,
        }
    }

    pub fn min_items(mut self, min: usize) -> Self {
        self.min_items = Some(min);
        self
    }

    pub fn max_items(mut self, max: usize) -> Self {
        self.max_items = Some(max);
        self
    }

    pub fn unique_items(mut self, unique: bool) -> Self {
        self.unique_items = unique;
        self
    }

    pub fn items_schema(mut self, schema: FieldSchema) -> Self {
        self.items_schema = Some(Box::new(schema));
        self
    }

    /// Validate an array against these constraints
    pub fn validate(&self, array: &[Value]) -> Vec<String> {
        let mut errors = Vec::new();

        if let Some(min_items) = self.min_items {
            if array.len() < min_items {
                errors.push(format!("Array has {} items, minimum required is {}", array.len(), min_items));
            }
        }

        if let Some(max_items) = self.max_items {
            if array.len() > max_items {
                errors.push(format!("Array has {} items, maximum allowed is {}", array.len(), max_items));
            }
        }

        if self.unique_items {
            let mut seen = HashSet::new();
            for (index, item) in array.iter().enumerate() {
                let item_str = serde_json::to_string(item).unwrap_or_default();
                if seen.contains(&item_str) {
                    errors.push(format!("Duplicate item found at index {}", index));
                    break;
                }
                seen.insert(item_str);
            }
        }

        errors
    }
}

impl Default for ArrayConstraint {
    fn default() -> Self {
        Self::new()
    }
}

/// Object constraints
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectConstraint {
    pub min_properties: Option<usize>,
    pub max_properties: Option<usize>,
    pub additional_properties: bool,
    pub required_properties: HashSet<String>,
    pub properties: HashMap<String, FieldSchema>,
    pub property_names_pattern: Option<String>,
    #[serde(skip)]
    pub property_names_regex: Option<Regex>,
}

impl ObjectConstraint {
    pub fn new() -> Self {
        Self {
            min_properties: None,
            max_properties: None,
            additional_properties: true,
            required_properties: HashSet::new(),
            properties: HashMap::new(),
            property_names_pattern: None,
            property_names_regex: None,
        }
    }

    pub fn min_properties(mut self, min: usize) -> Self {
        self.min_properties = Some(min);
        self
    }

    pub fn max_properties(mut self, max: usize) -> Self {
        self.max_properties = Some(max);
        self
    }

    pub fn additional_properties(mut self, allowed: bool) -> Self {
        self.additional_properties = allowed;
        self
    }

    pub fn required_property(mut self, name: &str) -> Self {
        self.required_properties.insert(name.to_string());
        self
    }

    pub fn property(mut self, name: &str, schema: FieldSchema) -> Self {
        self.properties.insert(name.to_string(), schema);
        self
    }

    pub fn property_names_pattern(mut self, pattern: &str) -> Result<Self, regex::Error> {
        self.property_names_pattern = Some(pattern.to_string());
        self.property_names_regex = Some(Regex::new(pattern)?);
        Ok(self)
    }

    /// Validate an object against these constraints
    pub fn validate(&self, object: &serde_json::Map<String, Value>) -> Vec<String> {
        let mut errors = Vec::new();

        // Check property count constraints
        if let Some(min_props) = self.min_properties {
            if object.len() < min_props {
                errors.push(format!("Object has {} properties, minimum required is {}", object.len(), min_props));
            }
        }

        if let Some(max_props) = self.max_properties {
            if object.len() > max_props {
                errors.push(format!("Object has {} properties, maximum allowed is {}", object.len(), max_props));
            }
        }

        // Check required properties
        for required_prop in &self.required_properties {
            if !object.contains_key(required_prop) {
                errors.push(format!("Required property '{}' is missing", required_prop));
            }
        }

        // Check property name patterns
        if let Some(regex) = &self.property_names_regex {
            for key in object.keys() {
                if !regex.is_match(key) {
                    errors.push(format!("Property name '{}' does not match required pattern", key));
                }
            }
        }

        // Check additional properties
        if !self.additional_properties {
            for key in object.keys() {
                if !self.properties.contains_key(key) {
                    errors.push(format!("Additional property '{}' is not allowed", key));
                }
            }
        }

        errors
    }
}

impl PartialEq for ObjectConstraint {
    fn eq(&self, other: &Self) -> bool {
        self.min_properties == other.min_properties
            && self.max_properties == other.max_properties
            && self.additional_properties == other.additional_properties
            && self.required_properties == other.required_properties
            && self.properties == other.properties
            && self.property_names_pattern == other.property_names_pattern
    }
}

impl Default for ObjectConstraint {
    fn default() -> Self {
        Self::new()
    }
}

/// General constraint type that encompasses all constraint types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Constraint {
    Numeric(NumericConstraint),
    String(StringConstraint),
    Array(ArrayConstraint),
    Object(ObjectConstraint),
}

/// Complete field schema definition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldSchema {
    pub field_type: FieldType,
    pub required: bool,
    pub nullable: bool,
    pub default_value: Option<Value>,
    pub description: Option<String>,
    pub title: Option<String>,
    pub examples: Vec<Value>,
    pub constraints: Vec<Constraint>,
    
    // Conditional validation
    pub if_condition: Option<Box<FieldSchema>>,
    pub then_schema: Option<Box<FieldSchema>>,
    pub else_schema: Option<Box<FieldSchema>>,
    
    // Cross-field dependencies
    pub dependencies: Vec<String>,
}

impl FieldSchema {
    pub fn new(field_type: FieldType) -> Self {
        Self {
            field_type,
            required: false,
            nullable: false,
            default_value: None,
            description: None,
            title: None,
            examples: Vec::new(),
            constraints: Vec::new(),
            if_condition: None,
            then_schema: None,
            else_schema: None,
            dependencies: Vec::new(),
        }
    }

    // Convenience constructors
    pub fn string() -> Self {
        Self::new(FieldType::String)
    }

    pub fn number() -> Self {
        Self::new(FieldType::Number)
    }

    pub fn integer() -> Self {
        Self::new(FieldType::Integer)
    }

    pub fn boolean() -> Self {
        Self::new(FieldType::Boolean)
    }

    pub fn array() -> Self {
        Self::new(FieldType::Array)
    }

    pub fn object() -> Self {
        Self::new(FieldType::Object)
    }

    pub fn null() -> Self {
        Self::new(FieldType::Null)
    }

    // Builder methods
    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    pub fn optional(mut self) -> Self {
        self.required = false;
        self
    }

    pub fn nullable(mut self) -> Self {
        self.nullable = true;
        self
    }

    pub fn default_value(mut self, value: Value) -> Self {
        self.default_value = Some(value);
        self
    }

    pub fn description(mut self, desc: &str) -> Self {
        self.description = Some(desc.to_string());
        self
    }

    pub fn title(mut self, title: &str) -> Self {
        self.title = Some(title.to_string());
        self
    }

    pub fn example(mut self, example: Value) -> Self {
        self.examples.push(example);
        self
    }

    pub fn constraint(mut self, constraint: Constraint) -> Self {
        self.constraints.push(constraint);
        self
    }

    pub fn depends_on(mut self, field: &str) -> Self {
        self.dependencies.push(field.to_string());
        self
    }

    // Specific constraint methods
    pub fn min_length(mut self, min: usize) -> Self {
        if matches!(self.field_type, FieldType::String) {
            let constraint = StringConstraint::new().min_length(min);
            self.constraints.push(Constraint::String(constraint));
        }
        self
    }

    pub fn max_length(mut self, max: usize) -> Self {
        if matches!(self.field_type, FieldType::String) {
            let constraint = StringConstraint::new().max_length(max);
            self.constraints.push(Constraint::String(constraint));
        }
        self
    }

    pub fn pattern(mut self, pattern: &str) -> Result<Self, regex::Error> {
        if matches!(self.field_type, FieldType::String) {
            let constraint = StringConstraint::new().pattern(pattern)?;
            self.constraints.push(Constraint::String(constraint));
        }
        Ok(self)
    }

    pub fn format(mut self, format: Format) -> Self {
        if matches!(self.field_type, FieldType::String) {
            let constraint = StringConstraint::new().format(format);
            self.constraints.push(Constraint::String(constraint));
        }
        self
    }

    pub fn min(mut self, min: f64) -> Self {
        if matches!(self.field_type, FieldType::Number | FieldType::Integer) {
            let constraint = NumericConstraint::new().min(min);
            self.constraints.push(Constraint::Numeric(constraint));
        }
        self
    }

    pub fn max(mut self, max: f64) -> Self {
        if matches!(self.field_type, FieldType::Number | FieldType::Integer) {
            let constraint = NumericConstraint::new().max(max);
            self.constraints.push(Constraint::Numeric(constraint));
        }
        self
    }

    pub fn min_items(mut self, min: usize) -> Self {
        if matches!(self.field_type, FieldType::Array) {
            let constraint = ArrayConstraint::new().min_items(min);
            self.constraints.push(Constraint::Array(constraint));
        }
        self
    }

    pub fn max_items(mut self, max: usize) -> Self {
        if matches!(self.field_type, FieldType::Array) {
            let constraint = ArrayConstraint::new().max_items(max);
            self.constraints.push(Constraint::Array(constraint));
        }
        self
    }

    pub fn unique_items(mut self) -> Self {
        if matches!(self.field_type, FieldType::Array) {
            let constraint = ArrayConstraint::new().unique_items(true);
            self.constraints.push(Constraint::Array(constraint));
        }
        self
    }
}

/// Complete schema definition for a document or collection
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaDefinition {
    pub schema_id: String,
    pub version: String,
    pub title: Option<String>,
    pub description: Option<String>,  
    pub properties: HashMap<String, FieldSchema>,
    pub required_fields: HashSet<String>,
    pub additional_properties: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl SchemaDefinition {
    pub fn new(schema_id: &str) -> Self {
        let now = chrono::Utc::now();
        Self {
            schema_id: schema_id.to_string(),
            version: "1.0.0".to_string(),
            title: None,
            description: None,
            properties: HashMap::new(),
            required_fields: HashSet::new(),
            additional_properties: true,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn version(mut self, version: &str) -> Self {
        self.version = version.to_string();
        self.updated_at = chrono::Utc::now();
        self
    }

    pub fn title(mut self, title: &str) -> Self {
        self.title = Some(title.to_string());
        self.updated_at = chrono::Utc::now();
        self
    }

    pub fn description(mut self, description: &str) -> Self {
        self.description = Some(description.to_string());
        self.updated_at = chrono::Utc::now();
        self
    }

    pub fn field(mut self, name: &str, schema: FieldSchema) -> Self {
        if schema.required {
            self.required_fields.insert(name.to_string());
        }
        self.properties.insert(name.to_string(), schema);
        self.updated_at = chrono::Utc::now();
        self
    }

    pub fn required_field(mut self, name: &str) -> Self {
        self.required_fields.insert(name.to_string());
        self.updated_at = chrono::Utc::now();
        self
    }

    pub fn additional_properties(mut self, allowed: bool) -> Self {
        self.additional_properties = allowed;
        self.updated_at = chrono::Utc::now();
        self
    }

    pub fn get_field_schema(&self, field_name: &str) -> Option<&FieldSchema> {
        self.properties.get(field_name)
    }

    pub fn is_field_required(&self, field_name: &str) -> bool {
        self.required_fields.contains(field_name)
    }

    pub fn field_count(&self) -> usize {
        self.properties.len()
    }

    pub fn required_field_count(&self) -> usize {
        self.required_fields.len()
    }
} 