// src/sql_frontend/mod.rs
pub mod converter;
pub mod logical_plan;
pub mod parser;

// Re-export key types for easier access if desired, e.g.:
// pub use logical_plan::{LogicalPlan, Expression, LiteralValue, Operator};
// pub use parser::parse_sql;
// pub use converter::ast_to_logical_plan;
// For now, users can use sql_frontend::logical_plan::LogicalPlan, etc.

use logical_plan::LogicalPlan; // Added for the facade function's return type

/// Parses a SQL string and converts it into a LogicalPlan.
///
/// This function serves as the main entry point for the SQL frontend.
/// It encapsulates the parsing of the SQL string into an AST
/// and then converts that AST into the engine's LogicalPlan representation.
///
/// # Arguments
/// * `sql` - A string slice representing the SQL query.
///
/// # Returns
/// A `Result` containing the `LogicalPlan` if successful,
/// or a `String` error message if parsing or conversion fails.
pub fn sql_to_logical_plan(sql: &str) -> Result<LogicalPlan, String> {
    let statements =
        parser::parse_sql(sql).map_err(|e| format!("SQL Parsing Error: {}", e))?;

    if statements.is_empty() {
        return Err("No SQL statements provided.".to_string());
    }

    // Assuming, for now, that we process only the first statement.
    // Production systems might handle multiple statements or specific kinds of statements.
    if statements.len() > 1 {
        // Or, iterate through them and return Vec<LogicalPlan>
        // For now, let's stick to the common case of a single query.
        eprintln!("Warning: Multiple SQL statements found, only the first one will be processed.");
    }

    converter::ast_to_logical_plan(&statements[0])
}

#[cfg(test)]
mod tests;
