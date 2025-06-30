// ast.rs - Abstract Syntax Tree for SQL-like query language

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Root query enum that represents all possible SQL-like queries
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Query {
    Select(SelectQuery),
    Insert(InsertQuery),
    Update(UpdateQuery),
    Delete(DeleteQuery),
    Create(CreateQuery),
}

/// SELECT query representation
/// Example: SELECT name, age FROM users WHERE age > 30 AND city = "Berlin"
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectQuery {
    pub fields: Vec<Field>,
    pub from: String, // collection name
    pub where_clause: Option<Condition>,
    pub order_by: Option<Vec<OrderBy>>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

/// INSERT query representation
/// Example: INSERT INTO users (name, age) VALUES ("John", 30)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InsertQuery {
    pub into: String, // collection name
    pub fields: Vec<String>,
    pub values: Vec<Value>,
}

/// UPDATE query representation
/// Example: UPDATE users SET name = "Jane" WHERE id = "123"
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UpdateQuery {
    pub table: String, // collection name
    pub set: Vec<Assignment>,
    pub where_clause: Option<Condition>,
}

/// DELETE query representation
/// Example: DELETE FROM users WHERE age < 18
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeleteQuery {
    pub from: String, // collection name
    pub where_clause: Option<Condition>,
}

/// CREATE query representation
/// Example: CREATE COLLECTION users
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateQuery {
    pub collection_name: String,
}

/// Field representation for SELECT queries
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Field {
    /// Wildcard selection: *
    All,
    /// Specific field: field_name
    Named(String),
    /// Field with alias: field_name AS alias
    Aliased { field: String, alias: String },
}

/// Assignment for UPDATE SET clauses
/// Example: name = "John", age = 30
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Assignment {
    pub field: String,
    pub value: Value,
}

/// WHERE clause condition representation
/// Supports nested conditions with AND/OR operators
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Condition {
    /// Simple comparison: field = value, field > value, etc.
    Comparison {
        field: String,
        operator: ComparisonOperator,
        value: Value,
    },
    /// Logical AND: condition1 AND condition2
    And(Box<Condition>, Box<Condition>),
    /// Logical OR: condition1 OR condition2
    Or(Box<Condition>, Box<Condition>),
    /// Logical NOT: NOT condition
    Not(Box<Condition>),
    /// Parenthesized condition: (condition)
    Parenthesized(Box<Condition>),
}

/// Comparison operators for WHERE clauses
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ComparisonOperator {
    Equal,              // =
    NotEqual,           // !=, <>
    GreaterThan,        // >
    GreaterThanOrEqual, // >=
    LessThan,           // <
    LessThanOrEqual,    // <=
    Like,               // LIKE (for string pattern matching)
    NotLike,            // NOT LIKE
    In,                 // IN (value1, value2, ...)
    NotIn,              // NOT IN (value1, value2, ...)
    IsNull,             // IS NULL
    IsNotNull,          // IS NOT NULL
}

/// ORDER BY clause representation
/// Example: ORDER BY name ASC, age DESC
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBy {
    pub field: String,
    pub direction: SortDirection,
}

/// Sort direction for ORDER BY
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SortDirection {
    Asc,  // Ascending
    Desc, // Descending
}

/// Expression representation for complex values
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    /// Literal value: "string", 123, true, null
    Literal(Value),
    /// Field reference: field_name
    Field(String),
    /// Function call: COUNT(*), SUM(field), etc.
    Function { name: String, args: Vec<Expression> },
    /// Binary operation: field + 10, field * 2
    Binary {
        left: Box<Expression>,
        operator: BinaryOperator,
        right: Box<Expression>,
    },
}

/// Binary operators for expressions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BinaryOperator {
    Add,      // +
    Subtract, // -
    Multiply, // *
    Divide,   // /
    Modulo,   // %
}

impl Query {
    /// Returns the type of query as a string
    pub fn query_type(&self) -> &'static str {
        match self {
            Query::Select(_) => "SELECT",
            Query::Insert(_) => "INSERT",
            Query::Update(_) => "UPDATE",
            Query::Delete(_) => "DELETE",
            Query::Create(_) => "CREATE",
        }
    }

    /// Returns the collection/table name being operated on
    pub fn target_collection(&self) -> &str {
        match self {
            Query::Select(q) => &q.from,
            Query::Insert(q) => &q.into,
            Query::Update(q) => &q.table,
            Query::Delete(q) => &q.from,
            Query::Create(q) => &q.collection_name,
        }
    }
}

impl SelectQuery {
    /// Creates a new SELECT query with basic fields
    pub fn new(fields: Vec<Field>, from: String) -> Self {
        Self {
            fields,
            from,
            where_clause: None,
            order_by: None,
            limit: None,
            offset: None,
        }
    }

    /// Adds a WHERE clause to the query
    pub fn with_where(mut self, condition: Condition) -> Self {
        self.where_clause = Some(condition);
        self
    }

    /// Adds ORDER BY clause to the query
    pub fn with_order_by(mut self, order_by: Vec<OrderBy>) -> Self {
        self.order_by = Some(order_by);
        self
    }

    /// Adds LIMIT clause to the query
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Adds OFFSET clause to the query
    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }
}

impl InsertQuery {
    /// Creates a new INSERT query
    pub fn new(into: String, fields: Vec<String>, values: Vec<Value>) -> Self {
        Self { into, fields, values }
    }
}

impl UpdateQuery {
    /// Creates a new UPDATE query
    pub fn new(table: String, set: Vec<Assignment>) -> Self {
        Self {
            table,
            set,
            where_clause: None,
        }
    }

    /// Adds a WHERE clause to the UPDATE query
    pub fn with_where(mut self, condition: Condition) -> Self {
        self.where_clause = Some(condition);
        self
    }
}

impl DeleteQuery {
    /// Creates a new DELETE query
    pub fn new(from: String) -> Self {
        Self {
            from,
            where_clause: None,
        }
    }

    /// Adds a WHERE clause to the DELETE query
    pub fn with_where(mut self, condition: Condition) -> Self {
        self.where_clause = Some(condition);
        self
    }
}

impl CreateQuery {
    /// Creates a new CREATE COLLECTION query
    pub fn new(collection_name: String) -> Self {
        Self { collection_name }
    }
}

impl Condition {
    /// Creates a simple comparison condition
    pub fn comparison(field: String, operator: ComparisonOperator, value: Value) -> Self {
        Self::Comparison { field, operator, value }
    }

    /// Creates an AND condition
    pub fn and(left: Condition, right: Condition) -> Self {
        Self::And(Box::new(left), Box::new(right))
    }

    /// Creates an OR condition
    pub fn or(left: Condition, right: Condition) -> Self {
        Self::Or(Box::new(left), Box::new(right))
    }

    /// Creates a NOT condition
    pub fn not(condition: Condition) -> Self {
        Self::Not(Box::new(condition))
    }

    /// Creates a parenthesized condition
    pub fn parenthesized(condition: Condition) -> Self {
        Self::Parenthesized(Box::new(condition))
    }
}

impl Assignment {
    /// Creates a new assignment
    pub fn new(field: String, value: Value) -> Self {
        Self { field, value }
    }
}

impl OrderBy {
    /// Creates a new ORDER BY clause
    pub fn new(field: String, direction: SortDirection) -> Self {
        Self { field, direction }
    }

    /// Creates an ascending ORDER BY clause
    pub fn asc(field: String) -> Self {
        Self::new(field, SortDirection::Asc)
    }

    /// Creates a descending ORDER BY clause
    pub fn desc(field: String) -> Self {
        Self::new(field, SortDirection::Desc)
    }
}

impl Field {
    /// Creates a named field
    pub fn named(name: String) -> Self {
        Self::Named(name)
    }

    /// Creates an aliased field
    pub fn aliased(field: String, alias: String) -> Self {
        Self::Aliased { field, alias }
    }

    /// Returns the actual field name (resolving aliases)
    pub fn field_name(&self) -> Option<&str> {
        match self {
            Self::All => None,
            Self::Named(name) => Some(name),
            Self::Aliased { field, .. } => Some(field),
        }
    }

    /// Returns the display name (alias if present, otherwise field name)
    pub fn display_name(&self) -> Option<&str> {
        match self {
            Self::All => None,
            Self::Named(name) => Some(name),
            Self::Aliased { alias, .. } => Some(alias),
        }
    }
} 