// src/sql_frontend/tests.rs
use crate::sql_frontend::sql_to_logical_plan;
use crate::sql_frontend::logical_plan::{Expression, LiteralValue, LogicalPlan, Operator};

#[test]
fn test_e2e_select_name_age_from_users_where_age_gt_25() {
    let sql = "SELECT name, age FROM users WHERE age > 25";
    let plan_result = sql_to_logical_plan(sql);
    assert!(plan_result.is_ok(), "sql_to_logical_plan failed: {:?}", plan_result.err());

    let plan = plan_result.unwrap();

    // Expected:
    // Projection {
    //   expressions: [Column("name"), Column("age")],
    //   input: Filter {
    //     predicate: (Column("age") Gt Literal(Number("25"))),
    //     input: Scan { table_name: "users", columns: None }
    //   }
    // }
    if let LogicalPlan::Projection { expressions, input } = plan {
        assert_eq!(expressions.len(), 2);
        assert_eq!(expressions[0], Expression::Column("name".to_string()));
        assert_eq!(expressions[1], Expression::Column("age".to_string()));

        if let LogicalPlan::Filter { predicate, input: filter_input } = *input {
            if let Expression::BinaryExpr {left, op, right} = predicate {
                assert_eq!(*left, Expression::Column("age".to_string()));
                assert_eq!(op, Operator::Gt);
                // Assuming sqlparser gives "25" as a string for Number
                assert_eq!(*right, Expression::Literal(LiteralValue::Number("25".to_string())));
            } else {
                panic!("Expected BinaryExpr for predicate, got {:?}", predicate);
            }

            if let LogicalPlan::Scan { table_name, columns } = *filter_input {
                assert_eq!(table_name, "users");
                assert_eq!(columns, None, "Scan columns should be None for this query structure");
            } else {
                panic!("Expected Scan for filter input, got {:?}", *filter_input);
            }
        } else {
            panic!("Expected Filter node as input to Projection, got {:?}", *input);
        }
    } else {
        panic!("Expected Projection node at the top, got {:?}", plan);
    }
}

#[test]
fn test_e2e_select_star_from_table() {
    let sql = "SELECT * FROM orders";
    let plan_result = sql_to_logical_plan(sql);
    assert!(plan_result.is_ok(), "sql_to_logical_plan failed: {:?}", plan_result.err());
    let plan = plan_result.unwrap();

    // Expected:
    // Projection {
    //   expressions: [Wildcard],
    //   input: Scan { table_name: "orders", columns: None }
    // }
    if let LogicalPlan::Projection { expressions, input } = plan {
        assert_eq!(expressions.len(), 1);
        assert_eq!(expressions[0], Expression::Wildcard);

        if let LogicalPlan::Scan { table_name, columns } = *input {
            assert_eq!(table_name, "orders");
            assert_eq!(columns, None, "Scan columns should be None for SELECT *");
        } else {
            panic!("Expected Scan node as input to Projection for SELECT *, got {:?}", *input);
        }
    } else {
        panic!("Expected Projection node for SELECT *, got {:?}", plan);
    }
}

#[test]
fn test_e2e_select_columns_from_table_no_where() {
    let sql = "SELECT product_id, quantity FROM order_items";
    let plan_result = sql_to_logical_plan(sql);
    assert!(plan_result.is_ok(), "sql_to_logical_plan failed: {:?}", plan_result.err());
    let plan = plan_result.unwrap();

    // Expected:
    // Projection {
    //   expressions: [Column("product_id"), Column("quantity")],
    //   input: Scan { table_name: "order_items", columns: None }
    // }
    if let LogicalPlan::Projection { expressions, input } = plan {
        assert_eq!(expressions.len(), 2);
        assert_eq!(expressions[0], Expression::Column("product_id".to_string()));
        assert_eq!(expressions[1], Expression::Column("quantity".to_string()));

        if let LogicalPlan::Scan { table_name, columns } = *input {
            assert_eq!(table_name, "order_items");
            assert_eq!(columns, None, "Scan columns should be None");
        } else {
            panic!("Expected Scan node as input to Projection, got {:?}", *input);
        }
    } else {
        panic!("Expected Projection node, got {:?}", plan);
    }
}

#[test]
fn test_e2e_select_with_alias_and_filter() {
    let sql = "SELECT name AS cust_name, email FROM customers WHERE country = 'USA'";
    let plan_result = sql_to_logical_plan(sql);
    assert!(plan_result.is_ok(), "sql_to_logical_plan failed: {:?}", plan_result.err());
    let plan = plan_result.unwrap();

    // Expected: Projection -> Filter -> Scan
    if let LogicalPlan::Projection { expressions, input } = plan {
        assert_eq!(expressions.len(), 2);
        assert_eq!(expressions[0], Expression::Alias {
            expr: Box::new(Expression::Column("name".to_string())),
            alias: "cust_name".to_string(),
        });
        assert_eq!(expressions[1], Expression::Column("email".to_string()));

        if let LogicalPlan::Filter { predicate, input: filter_input } = *input {
            // Predicate: country = 'USA'
            if let Expression::BinaryExpr {left, op, right} = predicate {
                assert_eq!(*left, Expression::Column("country".to_string()));
                assert_eq!(op, Operator::Eq);
                assert_eq!(*right, Expression::Literal(LiteralValue::String("USA".to_string())));
            } else {
                panic!("Expected BinaryExpr for predicate, got {:?}", predicate);
            }

            if let LogicalPlan::Scan { table_name, columns } = *filter_input {
                assert_eq!(table_name, "customers");
                assert_eq!(columns, None);
            } else {
                panic!("Expected Scan for filter input, got {:?}", *filter_input);
            }
        } else {
            panic!("Expected Filter node, got {:?}", *input);
        }
    } else {
        panic!("Expected Projection node, got {:?}", plan);
    }
}
