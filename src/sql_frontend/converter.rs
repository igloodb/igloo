// src/sql_frontend/converter.rs
use crate::sql_frontend::logical_plan::{Expression, LiteralValue, LogicalPlan, Operator};
use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, Expr as SqlExpr, Ident, ObjectName, Query as SqlQuery,
    SelectItem as SqlSelectItem, SetExpr as SqlSetExpr, Statement as SqlStatement,
    TableFactor as SqlTableFactor, TableWithJoins as SqlTableWithJoins, Value as SqlValue,
};

// Helper to get a string representation of the statement type
fn get_statement_variant_name(statement: &SqlStatement) -> &'static str {
    match statement {
        SqlStatement::Query(_) => "Query",
        SqlStatement::Insert { .. } => "Insert",
        SqlStatement::Update { .. } => "Update",
        SqlStatement::Delete { .. } => "Delete",
        SqlStatement::CreateTable { .. } => "CreateTable",
        SqlStatement::AlterTable { .. } => "AlterTable",
        SqlStatement::Drop { .. } => "Drop",
        SqlStatement::SetVariable { .. } => "SetVariable",
        SqlStatement::ShowVariable { .. } => "ShowVariable",
        SqlStatement::ShowVariables { .. } => "ShowVariables",
        SqlStatement::ShowCollation { .. } => "ShowCollation",
        SqlStatement::ShowColumns { .. } => "ShowColumns",
        SqlStatement::ShowTables { .. } => "ShowTables",
        SqlStatement::ShowCreate { .. } => "ShowCreate",
        SqlStatement::ShowFunctions { .. } => "ShowFunctions",
        SqlStatement::ShowCharset { .. } => "ShowCharset",
        SqlStatement::Assert { .. } => "Assert",
        SqlStatement::Explain { .. } => "Explain",
        SqlStatement::ExplainTable { .. } => "ExplainTable",
        SqlStatement::Analyze { .. } => "Analyze",
        SqlStatement::Execute { .. } => "Execute",
        SqlStatement::Declare { .. } => "Declare",
        SqlStatement::Fetch { .. } => "Fetch",
        SqlStatement::Close { .. } => "Close",
        SqlStatement::Prepare { .. } => "Prepare",
        SqlStatement::Deallocate { .. } => "Deallocate",
        SqlStatement::SetRole { .. } => "SetRole",
        SqlStatement::SetTimeZone { .. } => "SetTimeZone",
        SqlStatement::SetTransaction { .. } => "SetTransaction",
        SqlStatement::StartTransaction { .. } => "StartTransaction",
        SqlStatement::Commit { .. } => "Commit",
        SqlStatement::Rollback { .. } => "Rollback",
        SqlStatement::CreateSchema { .. } => "CreateSchema",
        SqlStatement::CreateView { .. } => "CreateView",
        SqlStatement::CreateFunction { .. } => "CreateFunction",
        SqlStatement::CreateProcedure { .. } => "CreateProcedure",
        SqlStatement::CreateMacro { .. } => "CreateMacro",
        SqlStatement::CreateStage { .. } => "CreateStage",
        SqlStatement::CreateIndex { .. } => "CreateIndex",
        SqlStatement::CreateVirtualTable { .. } => "CreateVirtualTable",
        SqlStatement::CreateSequence { .. } => "CreateSequence",
        SqlStatement::CreateType { .. } => "CreateType",
        SqlStatement::CreateDomain { .. } => "CreateDomain",
        SqlStatement::CreateExtension { .. } => "CreateExtension",
        SqlStatement::Comment { .. } => "Comment",
        SqlStatement::Grant { .. } => "Grant",
        SqlStatement::Revoke { .. } => "Revoke",
        SqlStatement::Discard { .. } => "Discard",
        SqlStatement::AlterFunction { .. } => "AlterFunction",
        SqlStatement::AlterIndex { .. } => "AlterIndex",
        SqlStatement::AlterSchema { .. } => "AlterSchema",
        SqlStatement::AlterView { .. } => "AlterView",
        SqlStatement::AlterSequence { .. } => "AlterSequence",
        SqlStatement::AlterType { .. } => "AlterType",
        SqlStatement::AlterDomain { .. } => "AlterDomain",
        SqlStatement::AlterExtension { .. } => "AlterExtension",
        SqlStatement::AlterDatabase { .. } => "AlterDatabase",
        SqlStatement::Copy { .. } => "Copy",
        SqlStatement::AttachDatabase { .. } => "AttachDatabase",
        SqlStatement::DetachDatabase { .. } => "DetachDatabase",
        SqlStatement::Kill { .. } => "Kill",
        SqlStatement::Merge { .. } => "Merge",
        SqlStatement::Msck { .. } => "Msck",
        SqlStatement::Savepoint { .. } => "Savepoint",
        SqlStatement::Release { .. } => "Release",
        SqlStatement::SetNames { .. } => "SetNames",
        SqlStatement::SetSchema { .. } => "SetSchema",
        SqlStatement::Truncate { .. } => "Truncate",
        SqlStatement::Use { .. } => "Use",
        SqlStatement::Values { .. } => "Values",
        _ => "OtherStatement", // Generic fallback
    }
}

// Helper to convert sqlparser::ast::Value to logical_plan::LiteralValue
fn sql_value_to_logical_literal(sql_value: &SqlValue) -> Result<LiteralValue, String> {
    match sql_value {
        SqlValue::Number(s, _) => Ok(LiteralValue::Number(s.clone())),
        SqlValue::SingleQuotedString(s) => Ok(LiteralValue::String(s.clone())),
        SqlValue::NationalStringLiteral(s) => Ok(LiteralValue::String(s.clone())),
        SqlValue::HexStringLiteral(s) => Ok(LiteralValue::String(s.clone())),
        SqlValue::DoubleQuotedString(s) => Ok(LiteralValue::String(s.clone())),
        SqlValue::Boolean(b) => Ok(LiteralValue::Boolean(*b)),
        SqlValue::Null => Ok(LiteralValue::Null),
        // SqlValue::Interval {..} | SqlValue::Placeholder(_) | SqlValue::UnQuotedString(_) etc.
        _ => Err(format!("Unsupported SQL value: {:?}", sql_value)),
    }
}

// Helper to convert sqlparser::ast::BinaryOperator to logical_plan::Operator
fn sql_operator_to_logical_operator(op: &SqlBinaryOperator) -> Result<Operator, String> {
    match op {
        SqlBinaryOperator::Eq => Ok(Operator::Eq),
        SqlBinaryOperator::NotEq => Ok(Operator::NotEq),
        SqlBinaryOperator::Lt => Ok(Operator::Lt),
        SqlBinaryOperator::LtEq => Ok(Operator::LtEq),
        SqlBinaryOperator::Gt => Ok(Operator::Gt),
        SqlBinaryOperator::GtEq => Ok(Operator::GtEq),
        SqlBinaryOperator::And => Ok(Operator::And),
        SqlBinaryOperator::Or => Ok(Operator::Or),
        _ => Err(format!("Unsupported SQL operator: {:?}", op)),
    }
}

// Helper to convert sqlparser::ast::Expr to logical_plan::Expression
// This function will be recursive for nested expressions.
fn sql_expr_to_logical_expr(sql_expr: &SqlExpr) -> Result<Expression, String> {
    match sql_expr {
        SqlExpr::Identifier(ident) => Ok(Expression::Column(ident.value.clone())),
        SqlExpr::CompoundIdentifier(idents) => {
            // For simplicity, join idents with "." e.g., "table.column"
            // This might need more sophisticated handling for schemas, etc. later
            Ok(Expression::Column(
                idents.iter().map(|i| i.value.clone()).collect::<Vec<String>>().join("."),
            ))
        }
        SqlExpr::Value(value) => {
            let literal_val = sql_value_to_logical_literal(value)?;
            Ok(Expression::Literal(literal_val))
        }
        SqlExpr::BinaryOp { left, op, right } => {
            let logical_left = Box::new(sql_expr_to_logical_expr(left)?);
            let logical_op = sql_operator_to_logical_operator(op)?;
            let logical_right = Box::new(sql_expr_to_logical_expr(right)?);
            Ok(Expression::BinaryExpr {
                left: logical_left,
                op: logical_op,
                right: logical_right,
            })
        }
        SqlExpr::Function(func) => {
            Err(format!("Function expressions are not yet supported: {:?}", func))
        }
        SqlExpr::InList { expr, list, negated } => {
            Err(format!(
                "InList expressions (e.g., IN or NOT IN) are not yet supported: {:?}, {:?}, negated: {}",
                expr, list, negated
            ))
        }
        // SqlExpr::IsNull(_) | SqlExpr::IsNotNull(_) etc.
        _ => Err(format!("Unsupported SQL expression: {:?}", sql_expr)),
    }
}

fn sql_select_item_to_logical_expression(item: &SqlSelectItem) -> Result<Expression, String> {
    match item {
        SqlSelectItem::UnnamedExpr(expr) => sql_expr_to_logical_expr(expr),
        SqlSelectItem::ExprWithAlias { expr, alias } => {
            let logical_expr = sql_expr_to_logical_expr(expr)?;
            Ok(Expression::Alias {
                expr: Box::new(logical_expr),
                alias: alias.value.clone(),
            })
        }
        SqlSelectItem::QualifiedWildcard(object_name, _) => {
            // For `table.*` or `schema.table.*`
            // Represent as a special kind of column or handle in Projection directly
            // For now, map to a string like "table.*"
            Ok(Expression::Column(format!("{}.*", object_name.0.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join("."))))
        }
        SqlSelectItem::Wildcard(_) => {
            Ok(Expression::Wildcard) // Represents `*`
        }
    }
}


/// Converts a parsed SQL AST Statement into a LogicalPlan.
/// Currently supports simple SELECT ... FROM ... WHERE queries.
pub fn ast_to_logical_plan(statement: &SqlStatement) -> Result<LogicalPlan, String> {
    match statement {
        SqlStatement::Query(query) => {
            let SqlQuery { body, .. } = &**query; // Dereference Box<Query>
            match body {
                SqlSetExpr::Select(select_statement) => {
                    let from_clause = &select_statement.from;
                    let selection_clause = &select_statement.selection; // This is the WHERE clause
                    let projection_list = &select_statement.projection;

                    // 1. Process FROM clause -> LogicalPlan::Scan
                    //    For now, assume a single table and no joins.
                    if from_clause.len() != 1 {
                        return Err(format!(
                            "Expected exactly one table in FROM clause, found {}",
                            from_clause.len()
                        ));
                    }
                    let table_with_joins: &SqlTableWithJoins = &from_clause[0];
                    if !table_with_joins.joins.is_empty() {
                        return Err("Joins are not supported yet".to_string());
                    }
                    let table_name_parts: Vec<String> = match &table_with_joins.relation {
                        SqlTableFactor::Table { name, .. } => {
                            // name is ObjectName { 0: Vec<Ident> }
                            name.0.iter().map(|ident| ident.value.clone()).collect()
                        }
                        _ => return Err("Unsupported table factor in FROM clause. Expected simple table name.".to_string()),
                    };
                    let table_name = table_name_parts.join("."); // e.g., "users" or "schema.users"

                    // Determine columns for Scan. If projection is wildcard, columns is None.
                    // Otherwise, it's Some(vec![col_names...]) if we want to pass this info to Scan.
                    // For now, let's simplify: if any projection item is a wildcard, columns for Scan is None.
                    // This is a simplification; actual columns might be derived from non-wildcard projection items.
                    let mut scan_columns: Option<Vec<String>> = None;
                    // let mut is_wildcard_projection = false; // This variable is not used
                    let mut projected_expressions = Vec::new();

                    for item in projection_list {
                        let logical_expr = sql_select_item_to_logical_expression(item)?;
                        if logical_expr == Expression::Wildcard {
                            // is_wildcard_projection = true; // This variable is not used
                        }
                        // If item is `table.*`, it's also a form of wildcard for that table.
                        // Our current Expression::Column("table.*") will be passed to projection.
                        projected_expressions.push(logical_expr);
                    }

                    // If `*` is present (or table.*), Scan node gets `columns = None` indicating all columns.
                    // The Projection node will also contain `Expression::Wildcard` or `Expression::Column("table.*")`.
                    // If no `*`, we could potentially list the explicitly named columns for the Scan node.
                    // For now, let's keep it None and let Projection handle it. This can be an optimization later.
                    scan_columns = None;


                    let mut current_plan = LogicalPlan::Scan {
                        table_name,
                        columns: scan_columns,
                    };

                    // 2. Process WHERE clause -> LogicalPlan::Filter
                    if let Some(predicate_expr) = selection_clause {
                        let logical_predicate = sql_expr_to_logical_expr(predicate_expr)?;
                        current_plan = LogicalPlan::Filter {
                            input: Box::new(current_plan),
                            predicate: logical_predicate,
                        };
                    }

                    // 3. Process SELECT list -> LogicalPlan::Projection
                    //    `projected_expressions` already built above.
                    current_plan = LogicalPlan::Projection {
                        input: Box::new(current_plan),
                        expressions: projected_expressions,
                    };

                    Ok(current_plan)
                }
                _ => Err("Unsupported query type in SET expression. Expected SELECT.".to_string()),
            }
        }
        _ => Err(format!(
            "Unsupported SQL statement type: {}. Only Query statements are supported.",
            get_statement_variant_name(statement)
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql_frontend::parser::parse_sql; // Assuming parser is accessible

    fn parse_and_convert(sql: &str) -> Result<LogicalPlan, String> {
        let statements = parse_sql(sql).map_err(|e| format!("Parse error: {}", e))?;
        if statements.is_empty() {
            return Err("No SQL statements found".to_string());
        }
        if statements.len() > 1 {
            return Err("Expected single SQL statement for conversion".to_string());
        }
        ast_to_logical_plan(&statements[0])
    }

    #[test]
    fn test_simple_select_from_where() {
        let sql = "SELECT id, name FROM users WHERE id = 1";
        let plan = parse_and_convert(sql).unwrap();

        // Expected: Projection { expressions: [Column("id"), Column("name")], input: Filter { predicate: (Column("id") Eq Literal(Number("1"))), input: Scan { table_name: "users", columns: None } } }
        if let LogicalPlan::Projection { expressions, input } = plan {
            assert_eq!(expressions.len(), 2);
            assert_eq!(expressions[0], Expression::Column("id".to_string()));
            assert_eq!(expressions[1], Expression::Column("name".to_string()));

            if let LogicalPlan::Filter { predicate, input: filter_input } = *input {
                // Predicate: id = 1
                if let Expression::BinaryExpr {left, op, right} = predicate {
                    assert_eq!(*left, Expression::Column("id".to_string()));
                    assert_eq!(op, Operator::Eq);
                    assert_eq!(*right, Expression::Literal(LiteralValue::Number("1".to_string())));
                } else {
                    panic!("Expected BinaryExpr for predicate, got {:?}", predicate);
                }

                if let LogicalPlan::Scan { table_name, columns } = *filter_input {
                    assert_eq!(table_name, "users");
                    assert_eq!(columns, None); // Defaulting to None for now
                } else {
                    panic!("Expected Scan for filter input, got {:?}", *filter_input);
                }
            } else {
                panic!("Expected Filter node, got {:?}", *input);
            }
        } else {
            panic!("Expected Projection node at the top, got {:?}", plan);
        }
    }

    #[test]
    fn test_select_star_from() {
        let sql = "SELECT * FROM customers";
        let plan = parse_and_convert(sql).unwrap();
        // Expected: Projection { expressions: [Wildcard], input: Scan { table_name: "customers", columns: None } }
        if let LogicalPlan::Projection { expressions, input } = plan {
            assert_eq!(expressions.len(), 1);
            assert_eq!(expressions[0], Expression::Wildcard);
            if let LogicalPlan::Scan { table_name, columns } = *input {
                assert_eq!(table_name, "customers");
                assert_eq!(columns, None);
            } else {
                panic!("Expected Scan node for Projection input, got {:?}", *input);
            }
        } else {
            panic!("Expected Projection node for SELECT *, got {:?}", plan);
        }
    }

    #[test]
    fn test_select_with_alias() {
        let sql = "SELECT name AS customer_name FROM customers";
        let plan = parse_and_convert(sql).unwrap();
        // Expected: Projection { expressions: [Alias { expr: Column("name"), alias: "customer_name" }], input: Scan { table_name: "customers", columns: None } }
        if let LogicalPlan::Projection { expressions, input: _ } = plan { // Corrected to _input
            assert_eq!(expressions.len(), 1);
            assert_eq!(expressions[0], Expression::Alias { expr: Box::new(Expression::Column("name".to_string())), alias: "customer_name".to_string() });
            // Check input is Scan (can be added if necessary)
        } else {
            panic!("Expected Projection node for SELECT with alias, got {:?}", plan);
        }
    }

    #[test]
    fn test_unsupported_statement() {
        let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
        let result = parse_and_convert(sql);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Unsupported SQL statement type: Insert. Only Query statements are supported.".to_string()
        );
    }

    #[test]
    fn test_compound_identifier_in_projection() {
        let sql = "SELECT users.name FROM users";
        let plan = parse_and_convert(sql).unwrap();
        if let LogicalPlan::Projection { expressions, .. } = plan {
            assert_eq!(expressions.len(), 1);
            assert_eq!(expressions[0], Expression::Column("users.name".to_string()));
        } else {
            panic!("Expected Projection node, got {:?}", plan);
        }
    }

    #[test]
    fn test_compound_identifier_in_filter() {
        let sql = "SELECT name FROM users WHERE users.age > 25";
        let plan = parse_and_convert(sql).unwrap();
        if let LogicalPlan::Projection { input, .. } = plan {
            if let LogicalPlan::Filter { predicate, .. } = *input {
                if let Expression::BinaryExpr { left, op, right } = predicate {
                    assert_eq!(*left, Expression::Column("users.age".to_string()));
                    assert_eq!(op, Operator::Gt);
                    assert_eq!(*right, Expression::Literal(LiteralValue::Number("25".to_string())));
                } else {
                        panic!("Expected BinaryExpr for predicate, got {:?}", predicate);
                }
            } else {
                    panic!("Expected Filter node, got {:?}", *input);
            }
        } else {
                panic!("Expected Projection node, got {:?}", plan);
        }
    }

   #[test]
   fn test_unsupported_function_expression() {
       let sql = "SELECT MYFUNC(column_a) FROM test_table";
       let result = parse_and_convert(sql);
       assert!(result.is_err(), "Expected an error for function expression, but got Ok: {:?}", result.ok());
       let error_message = result.unwrap_err();
       // The exact formatting of `Function { name: ..., args: ..., ... }` might be verbose.
       // We need to ensure the error message starts correctly.
       // Example: "Function expressions are not yet supported: Function { name: ObjectName([Ident { value: "MYFUNC", quote_style: None }]), args: [Identifier(Ident { value: "column_a", quote_style: None })], over: None, distinct: false, special: false, order_by: [] }"
       // For robustness, let's check the start of the message.
       assert!(
           error_message.starts_with("Function expressions are not yet supported:"),
           "Error message mismatch. Expected it to start with 'Function expressions are not yet supported:', but got: {}",
           error_message
       );
       // Optionally, also check if it contains key info like the function name:
       assert!(
           error_message.contains("MYFUNC") && error_message.contains("column_a"),
           "Error message for function did not contain expected function/argument names: {}",
           error_message
       );
   }

   #[test]
   fn test_unsupported_inlist_expression() {
       let sql = "SELECT column_a FROM test_table WHERE column_b IN (1, 'test_val')";
       let result = parse_and_convert(sql);
       assert!(result.is_err(), "Expected an error for IN LIST expression, but got Ok: {:?}", result.ok());
       let error_message = result.unwrap_err();
       // Example error: "InList expressions (e.g., IN or NOT IN) are not yet supported: Identifier(Ident { value: "column_b", quote_style: None }), [Value(Number("1", false)), Value(SingleQuotedString("test_val"))], negated: false"
       assert!(
           error_message.starts_with("InList expressions (e.g., IN or NOT IN) are not yet supported:"),
           "Error message mismatch. Expected it to start with 'InList expressions ... not yet supported:', but got: {}",
           error_message
       );
       // Check for key parts of the InList debug output
       assert!(
           error_message.contains("column_b") && error_message.contains("Number(\"1\", false)") && error_message.contains("SingleQuotedString(\"test_val\")") && error_message.contains("negated: false"),
           "Error message for IN list did not contain expected components: {}",
           error_message
       );
   }

   #[test]
   fn test_unsupported_notinlist_expression() {
       let sql = "SELECT column_a FROM test_table WHERE column_b NOT IN (1, 2)";
       let result = parse_and_convert(sql);
       assert!(result.is_err(), "Expected an error for NOT IN LIST expression, but got Ok: {:?}", result.ok());
       let error_message = result.unwrap_err();
        // Example error: "InList expressions (e.g., IN or NOT IN) are not yet supported: Identifier(Ident { value: "column_b", quote_style: None }), [Value(Number("1", false)), Value(Number("2", false))], negated: true"
       assert!(
           error_message.starts_with("InList expressions (e.g., IN or NOT IN) are not yet supported:"),
           "Error message mismatch. Expected it to start with 'InList expressions ... not yet supported:', but got: {}",
           error_message
       );
       assert!(
           error_message.contains("column_b") && error_message.contains("Number(\"1\", false)") && error_message.contains("Number(\"2\", false)") && error_message.contains("negated: true"),
           "Error message for NOT IN list did not contain expected components: {}",
           error_message
       );
   }
}
