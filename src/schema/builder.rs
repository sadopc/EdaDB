use serde_json::Value;
use crate::schema::definition::{
    SchemaDefinition, FieldSchema, FieldType, Constraint,
    NumericConstraint, StringConstraint, ArrayConstraint, ObjectConstraint
};
use crate::schema::format::Format;

/// Fluent API builder for creating schema definitions
#[derive(Debug)]
pub struct SchemaBuilder {
    schema: SchemaDefinition,
}

impl SchemaBuilder {
    pub fn new(schema_id: &str) -> Self {
        Self {
            schema: SchemaDefinition::new(schema_id),
        }
    }

    pub fn version(mut self, version: &str) -> Self {
        self.schema = self.schema.version(version);
        self
    }

    pub fn title(mut self, title: &str) -> Self {
        self.schema = self.schema.title(title);
        self
    }

    pub fn description(mut self, description: &str) -> Self {
        self.schema = self.schema.description(description);
        self
    }

    pub fn field(mut self, name: &str, field_schema: FieldSchema) -> Self {
        self.schema = self.schema.field(name, field_schema);
        self
    }

    pub fn required_field(mut self, name: &str) -> Self {
        self.schema = self.schema.required_field(name);
        self
    }

    pub fn additional_properties(mut self, allowed: bool) -> Self {
        self.schema = self.schema.additional_properties(allowed);
        self
    }

    pub fn build(self) -> SchemaDefinition {
        self.schema
    }
}

/// Fluent API builder for creating field schemas
#[derive(Debug)]
pub struct FieldSchemaBuilder {
    field_schema: FieldSchema,
}

impl FieldSchemaBuilder {
    pub fn new(field_type: FieldType) -> Self {
        Self {
            field_schema: FieldSchema::new(field_type),
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

    pub fn union(types: Vec<FieldType>) -> Self {
        Self::new(FieldType::Union(types))
    }

    // Basic properties
    pub fn required(mut self) -> Self {
        self.field_schema.required = true;
        self
    }

    pub fn optional(mut self) -> Self {
        self.field_schema.required = false;
        self
    }

    pub fn nullable(mut self) -> Self {
        self.field_schema.nullable = true;
        self
    }

    pub fn default_value(mut self, value: Value) -> Self {
        self.field_schema.default_value = Some(value);
        self
    }

    pub fn description(mut self, desc: &str) -> Self {
        self.field_schema.description = Some(desc.to_string());
        self
    }

    pub fn title(mut self, title: &str) -> Self {
        self.field_schema.title = Some(title.to_string());
        self
    }

    pub fn example(mut self, example: Value) -> Self {
        self.field_schema.examples.push(example);
        self
    }

    pub fn depends_on(mut self, field: &str) -> Self {
        self.field_schema.dependencies.push(field.to_string());
        self
    }

    // String constraints
    pub fn min_length(mut self, min: usize) -> Self {
        if matches!(self.field_schema.field_type, FieldType::String) {
            let constraint = StringConstraint::new().min_length(min);
            self.field_schema.constraints.push(Constraint::String(constraint));
        }
        self
    }

    pub fn max_length(mut self, max: usize) -> Self {
        if matches!(self.field_schema.field_type, FieldType::String) {
            let constraint = StringConstraint::new().max_length(max);
            self.field_schema.constraints.push(Constraint::String(constraint));
        }
        self
    }

    pub fn length_range(self, min: usize, max: usize) -> Self {
        self.min_length(min).max_length(max)
    }

    pub fn pattern(mut self, pattern: &str) -> Result<Self, regex::Error> {
        if matches!(self.field_schema.field_type, FieldType::String) {
            let constraint = StringConstraint::new().pattern(pattern)?;
            self.field_schema.constraints.push(Constraint::String(constraint));
        }
        Ok(self)
    }

    pub fn format(mut self, format: Format) -> Self {
        if matches!(self.field_schema.field_type, FieldType::String) {
            let constraint = StringConstraint::new().format(format);
            self.field_schema.constraints.push(Constraint::String(constraint));
        }
        self
    }

    // Numeric constraints  
    pub fn minimum(mut self, min: f64) -> Self {
        if matches!(self.field_schema.field_type, FieldType::Number | FieldType::Integer) {
            let constraint = NumericConstraint::new().min(min);
            self.field_schema.constraints.push(Constraint::Numeric(constraint));
        }
        self
    }

    pub fn maximum(mut self, max: f64) -> Self {
        if matches!(self.field_schema.field_type, FieldType::Number | FieldType::Integer) {
            let constraint = NumericConstraint::new().max(max);
            self.field_schema.constraints.push(Constraint::Numeric(constraint));
        }
        self
    }

    pub fn range(self, min: f64, max: f64) -> Self {
        self.minimum(min).maximum(max)
    }

    pub fn exclusive_minimum(mut self, min: f64) -> Self {
        if matches!(self.field_schema.field_type, FieldType::Number | FieldType::Integer) {
            let constraint = NumericConstraint::new().exclusive_min(min);
            self.field_schema.constraints.push(Constraint::Numeric(constraint));
        }
        self
    }

    pub fn exclusive_maximum(mut self, max: f64) -> Self {
        if matches!(self.field_schema.field_type, FieldType::Number | FieldType::Integer) {
            let constraint = NumericConstraint::new().exclusive_max(max);
            self.field_schema.constraints.push(Constraint::Numeric(constraint));
        }
        self
    }

    pub fn multiple_of(mut self, multiple: f64) -> Self {
        if matches!(self.field_schema.field_type, FieldType::Number | FieldType::Integer) {
            let constraint = NumericConstraint::new().multiple_of(multiple);
            self.field_schema.constraints.push(Constraint::Numeric(constraint));
        }
        self
    }

    pub fn positive(self) -> Self {
        self.exclusive_minimum(0.0)
    }

    pub fn non_negative(self) -> Self {
        self.minimum(0.0)
    }

    // Array constraints
    pub fn min_items(mut self, min: usize) -> Self {
        if matches!(self.field_schema.field_type, FieldType::Array) {
            let constraint = ArrayConstraint::new().min_items(min);
            self.field_schema.constraints.push(Constraint::Array(constraint));
        }
        self
    }

    pub fn max_items(mut self, max: usize) -> Self {
        if matches!(self.field_schema.field_type, FieldType::Array) {
            let constraint = ArrayConstraint::new().max_items(max);
            self.field_schema.constraints.push(Constraint::Array(constraint));
        }
        self
    }

    pub fn items_range(self, min: usize, max: usize) -> Self {
        self.min_items(min).max_items(max)
    }

    pub fn unique_items(mut self) -> Self {
        if matches!(self.field_schema.field_type, FieldType::Array) {
            let constraint = ArrayConstraint::new().unique_items(true);
            self.field_schema.constraints.push(Constraint::Array(constraint));
        }
        self
    }

    pub fn items_schema(mut self, item_schema: FieldSchema) -> Self {
        if matches!(self.field_schema.field_type, FieldType::Array) {
            let constraint = ArrayConstraint::new().items_schema(item_schema);
            self.field_schema.constraints.push(Constraint::Array(constraint));
        }
        self
    }

    // Object constraints
    pub fn min_properties(mut self, min: usize) -> Self {
        if matches!(self.field_schema.field_type, FieldType::Object) {
            let constraint = ObjectConstraint::new().min_properties(min);
            self.field_schema.constraints.push(Constraint::Object(constraint));
        }
        self
    }

    pub fn max_properties(mut self, max: usize) -> Self {
        if matches!(self.field_schema.field_type, FieldType::Object) {
            let constraint = ObjectConstraint::new().max_properties(max);
            self.field_schema.constraints.push(Constraint::Object(constraint));
        }
        self
    }

    pub fn properties_range(self, min: usize, max: usize) -> Self {
        self.min_properties(min).max_properties(max)
    }

    pub fn additional_properties(mut self, allowed: bool) -> Self {
        if matches!(self.field_schema.field_type, FieldType::Object) {
            let constraint = ObjectConstraint::new().additional_properties(allowed);
            self.field_schema.constraints.push(Constraint::Object(constraint));
        }
        self
    }

    pub fn property(mut self, name: &str, schema: FieldSchema) -> Self {
        if matches!(self.field_schema.field_type, FieldType::Object) {
            let mut constraint = ObjectConstraint::new().property(name, schema);
            self.field_schema.constraints.push(Constraint::Object(constraint));
        }
        self
    }

    pub fn required_property(mut self, name: &str) -> Self {
        if matches!(self.field_schema.field_type, FieldType::Object) {
            let constraint = ObjectConstraint::new().required_property(name);
            self.field_schema.constraints.push(Constraint::Object(constraint));
        }
        self
    }

    // Convenience methods for common patterns
    pub fn email(self) -> Self {
        self.format(Format::Email)
    }

    pub fn url(self) -> Self {
        self.format(Format::Url)
    }

    pub fn phone(self) -> Self {
        self.format(Format::Phone)
    }

    pub fn date(self) -> Self {
        self.format(Format::Date)
    }

    pub fn datetime(self) -> Self {
        self.format(Format::DateTime)
    }

    pub fn uuid(self) -> Self {
        self.format(Format::Uuid)
    }

    pub fn credit_card(self) -> Self {
        self.format(Format::CreditCard)
    }

    pub fn ipv4(self) -> Self {
        self.format(Format::Ipv4)
    }

    // Build the final field schema
    pub fn build(self) -> FieldSchema {
        self.field_schema
    }
}

// Convenience functions for quick schema creation
impl SchemaBuilder {
    /// Quick method to add a required string field
    pub fn string_field(self, name: &str) -> Self {
        self.field(name, FieldSchemaBuilder::string().required().build())
    }

    /// Quick method to add an optional string field
    pub fn optional_string_field(self, name: &str) -> Self {
        self.field(name, FieldSchemaBuilder::string().optional().build())
    }

    /// Quick method to add a required email field
    pub fn email_field(self, name: &str) -> Self {
        self.field(name, FieldSchemaBuilder::string().required().email().build())
    }

    /// Quick method to add a required number field
    pub fn number_field(self, name: &str) -> Self {
        self.field(name, FieldSchemaBuilder::number().required().build())
    }

    /// Quick method to add a required integer field
    pub fn integer_field(self, name: &str) -> Self {
        self.field(name, FieldSchemaBuilder::integer().required().build())
    }

    /// Quick method to add a required boolean field
    pub fn boolean_field(self, name: &str) -> Self {
        self.field(name, FieldSchemaBuilder::boolean().required().build())
    }

    /// Quick method to add a required array field
    pub fn array_field(self, name: &str) -> Self {
        self.field(name, FieldSchemaBuilder::array().required().build())
    }

    /// Quick method to add a required object field
    pub fn object_field(self, name: &str) -> Self {
        self.field(name, FieldSchemaBuilder::object().required().build())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_schema_builder() {
        let schema = SchemaBuilder::new("user_schema")
            .version("1.0.0")
            .title("User Schema")
            .description("Schema for user documents")
            .string_field("name")
            .email_field("email")
            .field("age", FieldSchemaBuilder::integer().required().minimum(0.0).maximum(150.0).build())
            .additional_properties(false)
            .build();

        assert_eq!(schema.schema_id, "user_schema");
        assert_eq!(schema.version, "1.0.0");
        assert_eq!(schema.title, Some("User Schema".to_string()));
        assert_eq!(schema.properties.len(), 3);
        assert!(schema.required_fields.contains("name"));
        assert!(schema.required_fields.contains("email"));
        assert!(schema.required_fields.contains("age"));
        assert!(!schema.additional_properties);
    }

    #[test]
    fn test_field_schema_builder() {
        let field = FieldSchemaBuilder::string()
            .required()
            .min_length(2)
            .max_length(50)
            .description("User's full name")
            .example(json!("John Doe"))
            .build();

        assert!(matches!(field.field_type, FieldType::String));
        assert!(field.required);
        assert_eq!(field.description, Some("User's full name".to_string()));
        assert_eq!(field.examples.len(), 1);
        assert_eq!(field.constraints.len(), 2); // min_length and max_length
    }

    #[test]
    fn test_numeric_field_builder() {
        let field = FieldSchemaBuilder::number()
            .required()
            .minimum(0.0)
            .maximum(100.0)
            .multiple_of(0.5)
            .build();

        assert!(matches!(field.field_type, FieldType::Number));
        assert!(field.required);
        assert_eq!(field.constraints.len(), 3);
    }

    #[test]
    fn test_array_field_builder() {
        let item_schema = FieldSchemaBuilder::string().min_length(1).build();
        let field = FieldSchemaBuilder::array()
            .required()
            .min_items(1)
            .max_items(10)
            .unique_items()
            .items_schema(item_schema)
            .build();

        assert!(matches!(field.field_type, FieldType::Array));
        assert!(field.required);
        assert_eq!(field.constraints.len(), 4);
    }
} 