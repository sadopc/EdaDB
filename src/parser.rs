use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_until, take_while1},
    character::complete::{char, multispace0, multispace1},
    combinator::{map, opt, value},
    multi::separated_list1,
    sequence::{delimited, tuple},
    IResult,
};
use crate::types::DataType;
use crate::table::IndexType;

// AST Node tanımları
#[derive(Debug, Clone, PartialEq)]
pub enum SqlStatement {
    CreateTable {
        table_name: String,
        columns: Vec<ColumnDefinition>,
    },
    CreateIndex {
        table_name: String,
        column_name: String,
        index_type: IndexType,
    },
    Insert {
        table_name: String,
        values: Vec<SqlValue>,
    },
    Select {
        table_name: String,
        columns: Vec<String>, // "*" için boş vec
        where_clause: Option<WhereClause>,
    },
    Update {
        table_name: String,
        assignments: Vec<Assignment>,
        where_clause: Option<WhereClause>,
    },
    Delete {
        table_name: String,
        where_clause: Option<WhereClause>,
    },
    DropTable {
        table_name: String,
    },
    ShowStats {
        table_name: String,
    },
    Explain {
        statement: Box<SqlStatement>,
    },
    SetStorageFormat {
        table_name: String,
        format: String, // "ROW", "COLUMN", or "HYBRID"
    },
    ShowStorageInfo {
        table_name: String,
    },
    CompressColumns {
        table_name: String,
    },
    AnalyticalQuery {
        table_name: String,
        operation: String, // "COUNT", "SUM", "AVG", "MIN", "MAX"
        column_name: String,
    },
    BeginTransaction {
        isolation_level: Option<String>, // "READ_COMMITTED", "REPEATABLE_READ", "SERIALIZABLE"
    },
    CommitTransaction,
    RollbackTransaction,
    ShowTransactions,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: SqlValue,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WhereClause {
    pub condition: Condition,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Condition {
    Equal(String, SqlValue),
    NotEqual(String, SqlValue),
    Greater(String, SqlValue),
    Less(String, SqlValue),
    GreaterEqual(String, SqlValue),
    LessEqual(String, SqlValue),
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Integer(i64),
    Text(String),
    Boolean(bool),
    Null,
}

// Parser fonksiyonları
pub fn parse_sql(input: &str) -> Result<SqlStatement, String> {
    let trimmed = input.trim();
    
    match sql_statement(trimmed) {
        Ok((remaining, statement)) => {
            if remaining.trim().is_empty() {
                Ok(statement)
            } else {
                Err(format!("Unexpected input: {}", remaining))
            }
        }
        Err(e) => Err(format!("Parse error: {}", e)),
    }
}

fn sql_statement(input: &str) -> IResult<&str, SqlStatement> {
    alt((
        explain_statement,
        create_table_statement,
        create_index_statement,
        insert_statement,
        select_statement,
        update_statement,
        delete_statement,
        drop_table_statement,
        show_stats_statement,
        set_storage_format_statement,
        show_storage_info_statement,
        compress_columns_statement,
        analytical_query_statement,
        begin_transaction_statement,
        commit_transaction_statement,
        rollback_transaction_statement,
        show_transactions_statement,
    ))(input)
}

// CREATE TABLE parser
fn create_table_statement(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("CREATE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("TABLE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table_name) = identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, columns) = delimited(
        char('('),
        separated_list1(
            tuple((multispace0, char(','), multispace0)),
            column_definition,
        ),
        char(')'),
    )(input)?;
    
    Ok((
        input,
        SqlStatement::CreateTable {
            table_name: table_name.to_string(),
            columns,
        },
    ))
}

// CREATE INDEX parser
fn create_index_statement(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("CREATE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("INDEX")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table_name) = identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, column_name) = identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;
    let (input, _) = multispace0(input)?;
    
    // İsteğe bağlı index type parsing (HASH veya BTREE)
    let (input, index_type) = opt(alt((
        value(IndexType::Hash, tag_no_case("HASH")),
        value(IndexType::BTree, tag_no_case("BTREE")),
    )))(input)?;
    
    Ok((
        input,
        SqlStatement::CreateIndex {
            table_name: table_name.to_string(),
            column_name: column_name.to_string(),
            index_type: index_type.unwrap_or(IndexType::Hash), // Default to Hash for backward compatibility
        },
    ))
}

fn column_definition(input: &str) -> IResult<&str, ColumnDefinition> {
    let (input, _) = multispace0(input)?;
    let (input, name) = identifier(input)?;
    let (input, _) = multispace1(input)?;
    let (input, data_type) = data_type_parser(input)?;
    let (input, _) = multispace0(input)?;
    
    Ok((
        input,
        ColumnDefinition {
            name: name.to_string(),
            data_type,
        },
    ))
}

fn data_type_parser(input: &str) -> IResult<&str, DataType> {
    alt((
        value(DataType::INT, tag_no_case("INT")),
        value(DataType::INT, tag_no_case("INTEGER")),
        value(DataType::TEXT, tag_no_case("TEXT")),
        value(DataType::TEXT, tag_no_case("VARCHAR")),
        value(DataType::TEXT, tag_no_case("STRING")),
        value(DataType::BOOL, tag_no_case("BOOL")),
        value(DataType::BOOL, tag_no_case("BOOLEAN")),
    ))(input)
}

// INSERT parser
fn insert_statement(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("INSERT")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("INTO")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table_name) = identifier(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("VALUES")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, values) = delimited(
        char('('),
        separated_list1(
            tuple((multispace0, char(','), multispace0)),
            sql_value,
        ),
        char(')'),
    )(input)?;
    
    Ok((
        input,
        SqlStatement::Insert {
            table_name: table_name.to_string(),
            values,
        },
    ))
}

// SELECT parser
fn select_statement(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("SELECT")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, columns) = alt((
        map(char('*'), |_| Vec::new()),
        separated_list1(
            tuple((multispace0, char(','), multispace0)),
            identifier,
        ),
    ))(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("FROM")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table_name) = identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, where_clause) = opt(where_clause_parser)(input)?;
    
    Ok((
        input,
        SqlStatement::Select {
            table_name: table_name.to_string(),
            columns: columns.into_iter().map(|s| s.to_string()).collect(),
            where_clause,
        },
    ))
}

// UPDATE parser
fn update_statement(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("UPDATE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table_name) = identifier(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("SET")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, assignments) = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        assignment,
    )(input)?;
    let (input, _) = multispace0(input)?;
    let (input, where_clause) = opt(where_clause_parser)(input)?;
    
    Ok((
        input,
        SqlStatement::Update {
            table_name: table_name.to_string(),
            assignments,
            where_clause,
        },
    ))
}

// DELETE parser
fn delete_statement(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("DELETE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("FROM")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table_name) = identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, where_clause) = opt(where_clause_parser)(input)?;
    
    Ok((
        input,
        SqlStatement::Delete {
            table_name: table_name.to_string(),
            where_clause,
        },
    ))
}

// DROP TABLE parser
fn drop_table_statement(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("DROP")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("TABLE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table_name) = identifier(input)?;
    
    Ok((
        input,
        SqlStatement::DropTable {
            table_name: table_name.to_string(),
        },
    ))
}

// SHOW STATS parser
fn show_stats_statement(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("SHOW")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("STATS")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table_name) = identifier(input)?;
    
    Ok((
        input,
        SqlStatement::ShowStats {
            table_name: table_name.to_string(),
        },
    ))
}

// EXPLAIN parser
fn explain_statement(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("EXPLAIN")(input)?;
    let (input, _) = multispace1(input)?;
    
    // Parse the statement to be explained (must be SELECT for now)
    let (input, statement) = alt((
        select_statement,
        // Can be extended to support other statements
    ))(input)?;
    
    Ok((
        input,
        SqlStatement::Explain {
            statement: Box::new(statement),
        },
    ))
}

// Assignment parser (for UPDATE)
// SET STORAGE FORMAT parser
fn set_storage_format_statement(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("SET")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("STORAGE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("FORMAT")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table_name) = identifier(input)?;
    let (input, _) = multispace1(input)?;
    let (input, format) = alt((
        value("ROW", tag_no_case("ROW")),
        value("COLUMN", tag_no_case("COLUMN")),
        value("HYBRID", tag_no_case("HYBRID")),
    ))(input)?;
    
    Ok((
        input,
        SqlStatement::SetStorageFormat {
            table_name: table_name.to_string(),
            format: format.to_string(),
        },
    ))
}

// SHOW STORAGE INFO parser
fn show_storage_info_statement(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("SHOW")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("STORAGE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("INFO")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table_name) = identifier(input)?;
    
    Ok((
        input,
        SqlStatement::ShowStorageInfo {
            table_name: table_name.to_string(),
        },
    ))
}

// COMPRESS COLUMNS parser
fn compress_columns_statement(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("COMPRESS")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("COLUMNS")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table_name) = identifier(input)?;
    
    Ok((
        input,
        SqlStatement::CompressColumns {
            table_name: table_name.to_string(),
        },
    ))
}

// Analytical query parser: SELECT COUNT(column) FROM table
fn analytical_query_statement(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("SELECT")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, operation) = alt((
        value("COUNT", tag_no_case("COUNT")),
        value("SUM", tag_no_case("SUM")),
        value("AVG", tag_no_case("AVG")),
        value("MIN", tag_no_case("MIN")),
        value("MAX", tag_no_case("MAX")),
    ))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, column_name) = identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("FROM")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table_name) = identifier(input)?;
    
    Ok((
        input,
        SqlStatement::AnalyticalQuery {
            table_name: table_name.to_string(),
            operation: operation.to_string(),
            column_name: column_name.to_string(),
        },
    ))
}

fn assignment(input: &str) -> IResult<&str, Assignment> {
    let (input, column) = identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('=')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, value) = sql_value(input)?;
    
    Ok((
        input,
        Assignment {
            column: column.to_string(),
            value,
        },
    ))
}

// WHERE clause parser
fn where_clause_parser(input: &str) -> IResult<&str, WhereClause> {
    let (input, _) = tag_no_case("WHERE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, condition) = condition_parser(input)?;
    
    Ok((input, WhereClause { condition }))
}

// Condition parser with AND/OR support
fn condition_parser(input: &str) -> IResult<&str, Condition> {
    or_condition(input)
}

// OR has lower precedence than AND
fn or_condition(input: &str) -> IResult<&str, Condition> {
    let (input, left) = and_condition(input)?;
    let (input, _) = multispace0(input)?;
    
    // Check if there's an OR operator
    if let Ok((input, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("OR")(input) {
        let (input, _) = multispace1(input)?;
        let (input, right) = or_condition(input)?;
        Ok((input, Condition::Or(Box::new(left), Box::new(right))))
    } else {
        Ok((input, left))
    }
}

// AND has higher precedence than OR
fn and_condition(input: &str) -> IResult<&str, Condition> {
    let (input, left) = primary_condition(input)?;
    let (input, _) = multispace0(input)?;
    
    // Check if there's an AND operator
    if let Ok((input, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("AND")(input) {
        let (input, _) = multispace1(input)?;
        let (input, right) = and_condition(input)?;
        Ok((input, Condition::And(Box::new(left), Box::new(right))))
    } else {
        Ok((input, left))
    }
}

// Primary condition with parentheses support
fn primary_condition(input: &str) -> IResult<&str, Condition> {
    alt((
        // Parentheses group
        delimited(
            tuple((multispace0, char('('), multispace0)),
            or_condition,
            tuple((multispace0, char(')'), multispace0)),
        ),
        // Simple comparison
        simple_condition,
    ))(input)
}

// Simple comparison condition
fn simple_condition(input: &str) -> IResult<&str, Condition> {
    let (input, column) = identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, op) = alt((
        tag(">="),
        tag("<="),
        tag("!="),
        tag("="),
        tag(">"),
        tag("<"),
    ))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, value) = sql_value(input)?;
    
    let condition = match op {
        "=" => Condition::Equal(column.to_string(), value),
        "!=" => Condition::NotEqual(column.to_string(), value),
        ">" => Condition::Greater(column.to_string(), value),
        "<" => Condition::Less(column.to_string(), value),
        ">=" => Condition::GreaterEqual(column.to_string(), value),
        "<=" => Condition::LessEqual(column.to_string(), value),
        _ => unreachable!(),
    };
    
    Ok((input, condition))
}

// SQL value parser
fn sql_value(input: &str) -> IResult<&str, SqlValue> {
    alt((
        map(quoted_string, SqlValue::Text),
        map(boolean_value, SqlValue::Boolean),
        map(integer_value, SqlValue::Integer),
        value(SqlValue::Null, tag_no_case("NULL")),
    ))(input)
}

// Quoted string parser
fn quoted_string(input: &str) -> IResult<&str, String> {
    alt((
        delimited(char('\''), take_until("'"), char('\'')),
        delimited(char('"'), take_until("\""), char('"')),
    ))(input)
    .map(|(remaining, s)| (remaining, s.to_string()))
}

// Boolean value parser
fn boolean_value(input: &str) -> IResult<&str, bool> {
    alt((
        value(true, tag_no_case("TRUE")),
        value(false, tag_no_case("FALSE")),
    ))(input)
}

// Integer value parser
fn integer_value(input: &str) -> IResult<&str, i64> {
    let (input, sign) = opt(alt((char('+'), char('-'))))(input)?;
    let (input, digits) = take_while1(|c: char| c.is_ascii_digit())(input)?;
    
    let number: i64 = digits.parse().map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Digit))
    })?;
    
    let result = match sign {
        Some('-') => -number,
        _ => number,
    };
    
    Ok((input, result))
}

// Identifier parser
fn identifier(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_ascii_alphanumeric() || c == '_')(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_table_parsing() {
        let sql = "CREATE TABLE users (id INT, name TEXT, active BOOL)";
        let result = parse_sql(sql).unwrap();
        
        match result {
            SqlStatement::CreateTable { table_name, columns } => {
                assert_eq!(table_name, "users");
                assert_eq!(columns.len(), 3);
                assert_eq!(columns[0].name, "id");
                assert_eq!(columns[0].data_type, DataType::INT);
            }
            _ => panic!("Expected CreateTable statement"),
        }
    }

    #[test]
    fn test_create_index_parsing() {
        let input = "CREATE INDEX users (name)";
        let result = parse_sql(input);
        
        assert!(result.is_ok());
        let statement = result.unwrap();
        
        match statement {
            SqlStatement::CreateIndex { table_name, column_name, index_type } => {
                assert_eq!(table_name, "users");
                assert_eq!(column_name, "name");
                assert_eq!(index_type, IndexType::Hash);
            }
            _ => panic!("Expected CreateIndex statement"),
        }
    }
    
    #[test]
    fn test_create_btree_index_parsing() {
        let input = "CREATE INDEX users (age) BTREE";
        let result = parse_sql(input);
        
        assert!(result.is_ok());
        let statement = result.unwrap();
        
        match statement {
            SqlStatement::CreateIndex { table_name, column_name, index_type } => {
                assert_eq!(table_name, "users");
                assert_eq!(column_name, "age");
                assert_eq!(index_type, IndexType::BTree);
            }
            _ => panic!("Expected CreateIndex statement"),
        }
    }
    
    #[test]
    fn test_create_hash_index_parsing() {
        let input = "CREATE INDEX users (name) HASH";
        let result = parse_sql(input);
        
        assert!(result.is_ok());
        let statement = result.unwrap();
        
        match statement {
            SqlStatement::CreateIndex { table_name, column_name, index_type } => {
                assert_eq!(table_name, "users");
                assert_eq!(column_name, "name");
                assert_eq!(index_type, IndexType::Hash);
            }
            _ => panic!("Expected CreateIndex statement"),
        }
    }

    #[test]
    fn test_insert_parsing() {
        let sql = "INSERT INTO users VALUES (1, 'John', true)";
        let result = parse_sql(sql).unwrap();
        
        match result {
            SqlStatement::Insert { table_name, values } => {
                assert_eq!(table_name, "users");
                assert_eq!(values.len(), 3);
                assert_eq!(values[0], SqlValue::Integer(1));
                assert_eq!(values[1], SqlValue::Text("John".to_string()));
                assert_eq!(values[2], SqlValue::Boolean(true));
            }
            _ => panic!("Expected Insert statement"),
        }
    }

    #[test]
    fn test_select_parsing() {
        let sql = "SELECT * FROM users";
        let result = parse_sql(sql).unwrap();
        
        match result {
            SqlStatement::Select { table_name, columns, where_clause } => {
                assert_eq!(table_name, "users");
                assert_eq!(columns.len(), 0); // * için boş vec
                assert!(where_clause.is_none());
            }
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_select_with_where_parsing() {
        let sql = "SELECT name, age FROM users WHERE id = 1";
        let result = parse_sql(sql).unwrap();
        
        match result {
            SqlStatement::Select { table_name, columns, where_clause } => {
                assert_eq!(table_name, "users");
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0], "name");
                assert_eq!(columns[1], "age");
                assert!(where_clause.is_some());
            }
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_update_parsing() {
        let sql = "UPDATE users SET name = 'Jane', age = 30 WHERE id = 1";
        let result = parse_sql(sql).unwrap();
        
        match result {
            SqlStatement::Update { table_name, assignments, where_clause } => {
                assert_eq!(table_name, "users");
                assert_eq!(assignments.len(), 2);
                assert_eq!(assignments[0].column, "name");
                assert_eq!(assignments[0].value, SqlValue::Text("Jane".to_string()));
                assert_eq!(assignments[1].column, "age");
                assert_eq!(assignments[1].value, SqlValue::Integer(30));
                assert!(where_clause.is_some());
            }
            _ => panic!("Expected Update statement"),
        }
    }

    #[test]
    fn test_delete_parsing() {
        let sql = "DELETE FROM users WHERE id = 1";
        let result = parse_sql(sql).unwrap();
        
        match result {
            SqlStatement::Delete { table_name, where_clause } => {
                assert_eq!(table_name, "users");
                assert!(where_clause.is_some());
            }
            _ => panic!("Expected Delete statement"),
        }
    }

    #[test]
    fn test_delete_all_parsing() {
        let sql = "DELETE FROM users";
        let result = parse_sql(sql).unwrap();
        
        match result {
            SqlStatement::Delete { table_name, where_clause } => {
                assert_eq!(table_name, "users");
                assert!(where_clause.is_none());
            }
            _ => panic!("Expected Delete statement"),
        }
    }
    
    #[test]
    fn test_show_stats_parsing() {
        let sql = "SHOW STATS users";
        let result = parse_sql(sql).unwrap();
        
        match result {
            SqlStatement::ShowStats { table_name } => {
                assert_eq!(table_name, "users");
            }
            _ => panic!("Expected ShowStats statement"),
        }
    }

    #[test]
    fn test_update_without_where_parsing() {
        let sql = "UPDATE users SET active = true";
        let result = parse_sql(sql).unwrap();
        
        match result {
            SqlStatement::Update { table_name, assignments, where_clause } => {
                assert_eq!(table_name, "users");
                assert_eq!(assignments.len(), 1);
                assert_eq!(assignments[0].column, "active");
                assert_eq!(assignments[0].value, SqlValue::Boolean(true));
                assert!(where_clause.is_none());
            }
            _ => panic!("Expected Update statement"),
        }
    }

    #[test]
    fn test_select_with_and_condition() {
        let sql = "SELECT * FROM users WHERE age > 25 AND active = true";
        let result = parse_sql(sql).unwrap();
        
        match result {
            SqlStatement::Select { table_name, columns, where_clause } => {
                assert_eq!(table_name, "users");
                assert_eq!(columns.len(), 0); // * için boş vec
                assert!(where_clause.is_some());
                
                let where_clause = where_clause.unwrap();
                match where_clause.condition {
                    Condition::And(left, right) => {
                        match (*left, *right) {
                            (Condition::Greater(col1, val1), Condition::Equal(col2, val2)) => {
                                assert_eq!(col1, "age");
                                assert_eq!(val1, SqlValue::Integer(25));
                                assert_eq!(col2, "active");
                                assert_eq!(val2, SqlValue::Boolean(true));
                            }
                            _ => panic!("Expected Greater AND Equal conditions"),
                        }
                    }
                    _ => panic!("Expected AND condition"),
                }
            }
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_select_with_or_condition() {
        let sql = "SELECT * FROM users WHERE age < 20 OR age > 60";
        let result = parse_sql(sql).unwrap();
        
        match result {
            SqlStatement::Select { table_name, columns, where_clause } => {
                assert_eq!(table_name, "users");
                assert!(where_clause.is_some());
                
                let where_clause = where_clause.unwrap();
                match where_clause.condition {
                    Condition::Or(left, right) => {
                        match (*left, *right) {
                            (Condition::Less(col1, val1), Condition::Greater(col2, val2)) => {
                                assert_eq!(col1, "age");
                                assert_eq!(val1, SqlValue::Integer(20));
                                assert_eq!(col2, "age");
                                assert_eq!(val2, SqlValue::Integer(60));
                            }
                            _ => panic!("Expected Less OR Greater conditions"),
                        }
                    }
                    _ => panic!("Expected OR condition"),
                }
            }
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_select_with_parentheses() {
        let sql = "SELECT * FROM users WHERE (age > 25 AND active = true) OR name = 'Admin'";
        let result = parse_sql(sql).unwrap();
        
        match result {
            SqlStatement::Select { table_name, columns, where_clause } => {
                assert_eq!(table_name, "users");
                assert!(where_clause.is_some());
                
                let where_clause = where_clause.unwrap();
                match where_clause.condition {
                    Condition::Or(left, right) => {
                        match (*left, *right) {
                            (Condition::And(_, _), Condition::Equal(col, val)) => {
                                assert_eq!(col, "name");
                                assert_eq!(val, SqlValue::Text("Admin".to_string()));
                            }
                            _ => panic!("Expected AND OR Equal conditions"),
                        }
                    }
                    _ => panic!("Expected OR condition"),
                }
            }
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_update_with_complex_where() {
        let sql = "UPDATE users SET active = false WHERE age >= 18 AND age <= 65 AND name != 'System'";
        let result = parse_sql(sql).unwrap();
        
        match result {
            SqlStatement::Update { table_name, assignments, where_clause } => {
                assert_eq!(table_name, "users");
                assert_eq!(assignments.len(), 1);
                assert!(where_clause.is_some());
                
                let where_clause = where_clause.unwrap();
                match where_clause.condition {
                    Condition::And(left, right) => {
                        match (*left, *right) {
                            (Condition::GreaterEqual(col1, val1), Condition::And(nested_left, nested_right)) => {
                                assert_eq!(col1, "age");
                                assert_eq!(val1, SqlValue::Integer(18));
                                
                                match (*nested_left, *nested_right) {
                                    (Condition::LessEqual(col2, val2), Condition::NotEqual(col3, val3)) => {
                                        assert_eq!(col2, "age");
                                        assert_eq!(val2, SqlValue::Integer(65));
                                        assert_eq!(col3, "name");
                                        assert_eq!(val3, SqlValue::Text("System".to_string()));
                                    }
                                    _ => panic!("Expected LessEqual AND NotEqual conditions"),
                                }
                            }
                            _ => panic!("Expected GreaterEqual AND nested conditions"),
                        }
                    }
                    _ => panic!("Expected AND condition"),
                }
            }
            _ => panic!("Expected Update statement"),
        }
    }

    #[test]
    fn test_delete_with_or_condition() {
        let sql = "DELETE FROM users WHERE active = false OR age < 18";
        let result = parse_sql(sql).unwrap();
        
        match result {
            SqlStatement::Delete { table_name, where_clause } => {
                assert_eq!(table_name, "users");
                assert!(where_clause.is_some());
                
                let where_clause = where_clause.unwrap();
                match where_clause.condition {
                    Condition::Or(left, right) => {
                        match (*left, *right) {
                            (Condition::Equal(col1, val1), Condition::Less(col2, val2)) => {
                                assert_eq!(col1, "active");
                                assert_eq!(val1, SqlValue::Boolean(false));
                                assert_eq!(col2, "age");
                                assert_eq!(val2, SqlValue::Integer(18));
                            }
                            _ => panic!("Expected Equal OR Less conditions"),
                        }
                    }
                    _ => panic!("Expected OR condition"),
                }
            }
            _ => panic!("Expected Delete statement"),
        }
    }

    #[test]
    fn test_begin_transaction_parsing() {
        let sql = "BEGIN TRANSACTION";
        let result = parse_sql(sql).unwrap();
        
        match result {
            SqlStatement::BeginTransaction { isolation_level } => {
                assert!(isolation_level.is_none());
            }
            _ => panic!("Expected BeginTransaction statement"),
        }
    }

    #[test]
    fn test_begin_transaction_with_isolation_parsing() {
        let sql = "BEGIN TRANSACTION ISOLATION READ_COMMITTED";
        let result = parse_sql(sql).unwrap();
        
        match result {
            SqlStatement::BeginTransaction { isolation_level } => {
                assert_eq!(isolation_level, Some("READ_COMMITTED".to_string()));
            }
            _ => panic!("Expected BeginTransaction statement"),
        }
    }

    #[test]
    fn test_commit_transaction_parsing() {
        let sql = "COMMIT";
        let result = parse_sql(sql).unwrap();
        
        match result {
            SqlStatement::CommitTransaction => {
                // Success
            }
            _ => panic!("Expected CommitTransaction statement"),
        }
    }

    #[test]
    fn test_rollback_transaction_parsing() {
        let sql = "ROLLBACK";
        let result = parse_sql(sql).unwrap();
        
        match result {
            SqlStatement::RollbackTransaction => {
                // Success
            }
            _ => panic!("Expected RollbackTransaction statement"),
        }
    }

    #[test]
    fn test_show_transactions_parsing() {
        let sql = "SHOW TRANSACTIONS";
        let result = parse_sql(sql).unwrap();
        
        match result {
            SqlStatement::ShowTransactions => {
                // Success
            }
            _ => panic!("Expected ShowTransactions statement"),
        }
    }
}

// Transaction parser functions
fn begin_transaction_statement(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("BEGIN")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("TRANSACTION")(input)?;
    let (input, _) = multispace0(input)?;
    
    // İsteğe bağlı isolation level
    let (input, isolation_level) = opt(tuple((
        multispace1,
        tag_no_case("ISOLATION"),
        multispace1,
        alt((
            tag_no_case("READ_COMMITTED"),
            tag_no_case("REPEATABLE_READ"),
            tag_no_case("SERIALIZABLE"),
        )),
    )))(input)?;
    
    Ok((
        input,
        SqlStatement::BeginTransaction {
            isolation_level: isolation_level.map(|(_, _, _, level)| level.to_string()),
        },
    ))
}

fn commit_transaction_statement(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("COMMIT")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = opt(tag_no_case("TRANSACTION"))(input)?;
    
    Ok((input, SqlStatement::CommitTransaction))
}

fn rollback_transaction_statement(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("ROLLBACK")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = opt(tag_no_case("TRANSACTION"))(input)?;
    
    Ok((input, SqlStatement::RollbackTransaction))
}

fn show_transactions_statement(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("SHOW")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("TRANSACTIONS")(input)?;
    
    Ok((input, SqlStatement::ShowTransactions))
}