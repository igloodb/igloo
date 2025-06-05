// src/sql_frontend/logical_plan.rs

/// Represents a literal value in an expression.
#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    String(String),
    Number(String), // Using String for numbers as per plan, for simplicity with sqlparser output
    Boolean(bool),
    Null,
}

/// Represents a binary operator in an expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Operator {
    Eq,         // =
    NotEq,      // <> or !=
    Lt,         // <
    LtEq,       // <=
    Gt,         // >
    GtEq,       // >=
    And,        // AND
    Or,         // OR
    // Add more operators as needed, e.g., Plus, Minus, Multiply, Divide for arithmetic
}

/// Represents an expression in a logical plan.
/// These are the building blocks for predicates and projections.
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    /// A named column, e.g., "users.id" or "id".
    Column(String),
    /// A literal value, e.g., 'hello', 123, TRUE.
    Literal(LiteralValue),
    /// A binary expression, e.g., "age > 25".
    BinaryExpr {
        left: Box<Expression>,
        op: Operator,
        right: Box<Expression>,
    },
    /// Represents an aliased expression, e.g., `SELECT count(id) AS total_users`.
    /// For now, the `alias` will just be the string name. The `expr` is the expression being aliased.
    /// This might be more relevant for projections.
    Alias {
        expr: Box<Expression>,
        alias: String,
    },
    /// Represents a wildcard selection, e.g., `*` in `SELECT *`.
    Wildcard,
}

/// Represents a logical plan for a query.
/// This is a tree-like structure that defines the operations to be performed.
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    /// Projects a set of expressions.
    Projection {
        input: Box<LogicalPlan>,
        expressions: Vec<Expression>, // These are the columns/expressions to output
    },
    /// Filters rows based on a predicate.
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expression,
    },
    /// Scans a table, optionally with a specific list of columns.
    /// If `columns` is `None`, it implies all columns (e.g., for `SELECT *`).
    Scan {
        table_name: String,
        /// Specific columns to scan from the table.
        /// `None` means all columns. For a `SELECT *` this would be `None`.
        /// For `SELECT a, b`, this might be `Some(vec!["a".to_string(), "b".to_string()])`
        /// if the scan operation itself can optimize by fetching only these.
        /// However, the `Projection` node is the primary place where output columns are defined.
        /// Keeping it here allows for potential pushdowns later.
        columns: Option<Vec<String>>,
    },
    // Other potential nodes for future: Aggregate, Sort, Join, Limit, etc.
}
