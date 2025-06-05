// src/sql_frontend/parser.rs
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::ast::Statement;

/// Parses a SQL string into a vector of AST Statements.
///
/// # Arguments
/// * `sql` - A string slice representing the SQL query.
///
/// # Returns
/// A `Result` containing a `Vec<Statement>` if parsing is successful,
/// or a `ParserError` if an error occurs.
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>, ParserError> {
    let dialect = GenericDialect {}; // Or AnsiDialect {}, depending on desired compatibility
    Parser::parse_sql(&dialect, sql)
}

// Basic tests for the parser
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let sql = "SELECT id, name FROM users WHERE id = 1";
        let result = parse_sql(sql);
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);
        // Further assertions can be made on the statement structure if needed
    }

    #[test]
    fn test_invalid_sql() {
        let sql = "SELECT FROM WHERE id = 1"; // Invalid SQL
        let result = parse_sql(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_sql_empty_query() {
        let sql = "";
        let result = parse_sql(sql);
        assert!(result.is_ok()); // sqlparser-rs returns Ok(vec![]) for empty string
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_parse_sql_multiple_statements() {
         let sql = "SELECT * FROM table1; SELECT * FROM table2;";
         let result = parse_sql(sql);
         assert!(result.is_ok());
         assert_eq!(result.unwrap().len(), 2);
    }
}
