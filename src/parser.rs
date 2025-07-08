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

// AST Node tanımları
#[derive(Debug, Clone, PartialEq)]
pub enum SqlStatement {
    CreateTable {
        table_name: String,
        columns: Vec<ColumnDefinition>,
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
        create_table_statement,
        insert_statement,
        select_statement,
        update_statement,
        delete_statement,
        drop_table_statement,
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

// Assignment parser (for UPDATE)
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
} 