use datafusion::error::DataFusionError;
use sqlparser::{
  ast::{Expr, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins},
  dialect::GenericDialect,
  parser::Parser,
};
use std::collections::HashSet;

pub fn extract_table_names_and_ctes(sql_query: &str) -> Result<(HashSet<String>, HashSet<String>), DataFusionError> {
  // Parse SQL to extract table names
  let dialect = GenericDialect {};
  let statements = Parser::parse_sql(&dialect, sql_query).map_err(|e| DataFusionError::Execution(format!("Failed to parse SQL: {}", e)))?;

  let mut table_names = HashSet::new();
  let mut cte_names = HashSet::new();

  for statement in statements {
    match statement {
      Statement::Query(query) => {
        extract_from_query(&query, &mut table_names, &mut cte_names);
      }
      Statement::Insert(insert) => {
        // Extract table name from INSERT - note: insert.into is a boolean flag
        // The actual table name is in a different location depending on sqlparser version
        // We'll skip INSERT table extraction for now as it's less critical

        // Extract from subquery if present
        if let Some(query) = &insert.source {
          extract_from_query(query, &mut table_names, &mut cte_names);
        }
      }
      Statement::Update { table, selection, .. } => {
        // Extract table name from UPDATE
        extract_from_table_with_joins(&table, &mut table_names);
        // Extract from WHERE clause
        if let Some(expr) = selection {
          extract_from_expr(&expr, &mut table_names);
        }
      }
      Statement::Delete(delete) => {
        // Extract table names from DELETE
        for table in &delete.tables {
          if let Some(table_name) = table.0.last() {
            table_names.insert(table_name.value.to_lowercase());
          }
        }
        // Also check the FROM clause for DELETE with FROM
        if let sqlparser::ast::FromTable::WithFromKeyword(tables) = &delete.from {
          for from_table in tables {
            extract_from_table_with_joins(from_table, &mut table_names);
          }
        }
      }
      _ => {
        // Handle other statement types if needed
      }
    }
  }

  Ok((table_names, cte_names))
}

/// Extract table names from a Query AST node
fn extract_from_query(query: &Query, table_names: &mut HashSet<String>, cte_names: &mut HashSet<String>) {
  // Extract CTE names first
  if let Some(with) = &query.with {
    for cte in &with.cte_tables {
      cte_names.insert(cte.alias.name.value.to_lowercase());
      // Also traverse the CTE query body
      extract_from_query(&cte.query, table_names, cte_names);
    }
  }

  // Extract from main query body
  extract_from_set_expr(&query.body, table_names, cte_names);

  // Extract from ORDER BY clause if it contains subqueries
  // Note: query.order_by is a Vec<OrderBy>, not an OrderByExpr
  // OrderBy is likely a struct containing the expression to order by
  // For now, we skip ORDER BY extraction as it's less critical for table discovery
}

/// Extract table names from SetExpr (SELECT, UNION, etc.)
fn extract_from_set_expr(set_expr: &SetExpr, table_names: &mut HashSet<String>, cte_names: &mut HashSet<String>) {
  match set_expr {
    SetExpr::Select(select) => {
      extract_from_select(select, table_names, cte_names);
    }
    SetExpr::Query(query) => {
      extract_from_query(query, table_names, cte_names);
    }
    SetExpr::SetOperation { left, right, .. } => {
      // Handle UNION, INTERSECT, EXCEPT
      extract_from_set_expr(left, table_names, cte_names);
      extract_from_set_expr(right, table_names, cte_names);
    }
    SetExpr::Values(_) => {
      // VALUES clause doesn't contain table references
    }
    SetExpr::Insert(_) => {
      // Handle INSERT if needed
    }
    SetExpr::Update(_) => {
      // Handle UPDATE if needed
    }
    SetExpr::Table(table) => {
      // Direct table reference
      if let Some(table_name_str) = &table.table_name {
        table_names.insert(table_name_str.to_lowercase());
      }
    }
  }
}

/// Extract table names from a SELECT statement
fn extract_from_select(select: &Select, table_names: &mut HashSet<String>, _cte_names: &mut HashSet<String>) {
  // Extract from FROM clause
  for table_with_joins in &select.from {
    extract_from_table_with_joins(table_with_joins, table_names);
  }

  // Extract from SELECT items (for subqueries in select list)
  for item in &select.projection {
    match item {
      SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
        extract_from_expr(expr, table_names);
      }
      _ => {}
    }
  }

  // Extract from WHERE clause
  if let Some(selection) = &select.selection {
    extract_from_expr(selection, table_names);
  }

  // Extract from GROUP BY
  match &select.group_by {
    sqlparser::ast::GroupByExpr::All(_) => {}
    sqlparser::ast::GroupByExpr::Expressions(exprs, _) => {
      for expr in exprs {
        extract_from_expr(expr, table_names);
      }
    }
  }

  // Extract from HAVING clause
  if let Some(having) = &select.having {
    extract_from_expr(having, table_names);
  }
}

/// Extract table names from TableWithJoins (handles JOINs)
fn extract_from_table_with_joins(table_with_joins: &TableWithJoins, table_names: &mut HashSet<String>) {
  // Extract from main table
  extract_from_table_factor(&table_with_joins.relation, table_names);

  // Extract from JOIN clauses
  for join in &table_with_joins.joins {
    extract_from_table_factor(&join.relation, table_names);

    // Extract from JOIN condition
    match &join.join_operator {
      sqlparser::ast::JoinOperator::Inner(constraint)
      | sqlparser::ast::JoinOperator::LeftOuter(constraint)
      | sqlparser::ast::JoinOperator::RightOuter(constraint)
      | sqlparser::ast::JoinOperator::FullOuter(constraint) => {
        if let sqlparser::ast::JoinConstraint::On(expr) = constraint {
          extract_from_expr(expr, table_names);
        }
      }
      _ => {}
    }
  }
}

/// Extract table names from TableFactor (base table, subquery, or derived table)
fn extract_from_table_factor(table_factor: &TableFactor, table_names: &mut HashSet<String>) {
  match table_factor {
    TableFactor::Table { name, .. } => {
      // Extract the table name (use the last part for qualified names)
      if let Some(table_name) = name.0.last() {
        table_names.insert(table_name.value.to_lowercase());
      }
    }
    TableFactor::Derived { subquery, .. } => {
      // Extract from subquery
      let mut temp_cte_names = HashSet::new();
      extract_from_query(subquery, table_names, &mut temp_cte_names);
    }
    TableFactor::TableFunction { .. } => {
      // Table functions don't reference stored tables
    }
    TableFactor::UNNEST { .. } => {
      // UNNEST doesn't reference stored tables
    }
    TableFactor::NestedJoin { table_with_joins, .. } => {
      extract_from_table_with_joins(table_with_joins, table_names);
    }
    TableFactor::Pivot { .. } | TableFactor::Unpivot { .. } => {
      // Handle PIVOT/UNPIVOT if needed
    }
    TableFactor::MatchRecognize { .. } => {
      // Handle MATCH_RECOGNIZE if needed
    }
    TableFactor::JsonTable { .. } => {
      // JSON_TABLE doesn't reference stored tables
    }
    TableFactor::Function { .. } => {
      // Table functions don't reference stored tables
    }
    TableFactor::OpenJsonTable { .. } => {
      // OPENJSON doesn't reference stored tables
    }
  }
}

/// Extract table names from expressions (for subqueries in expressions)
fn extract_from_expr(expr: &Expr, table_names: &mut HashSet<String>) {
  match expr {
    Expr::Subquery(query) => {
      let mut temp_cte_names = HashSet::new();
      extract_from_query(query, table_names, &mut temp_cte_names);
    }
    Expr::InSubquery { subquery, .. } => {
      let mut temp_cte_names = HashSet::new();
      extract_from_query(subquery, table_names, &mut temp_cte_names);
    }
    Expr::Exists { subquery, .. } => {
      let mut temp_cte_names = HashSet::new();
      extract_from_query(subquery, table_names, &mut temp_cte_names);
    }
    Expr::BinaryOp { left, right, .. } => {
      extract_from_expr(left, table_names);
      extract_from_expr(right, table_names);
    }
    Expr::UnaryOp { expr, .. } => {
      extract_from_expr(expr, table_names);
    }
    Expr::Cast { expr, .. } => {
      extract_from_expr(expr, table_names);
    }
    Expr::Nested(expr) => {
      extract_from_expr(expr, table_names);
    }
    Expr::Function(func) => match &func.args {
      sqlparser::ast::FunctionArguments::None => {}
      sqlparser::ast::FunctionArguments::Subquery(_) => {}
      sqlparser::ast::FunctionArguments::List(arg_list) => {
        for arg in &arg_list.args {
          match arg {
            sqlparser::ast::FunctionArg::Named { arg, .. } => {
              extract_from_function_arg_expr(arg, table_names);
            }
            sqlparser::ast::FunctionArg::Unnamed(arg) => {
              extract_from_function_arg_expr(arg, table_names);
            }
            sqlparser::ast::FunctionArg::ExprNamed { arg, .. } => {
              extract_from_function_arg_expr(arg, table_names);
            }
          }
        }
      }
    },
    Expr::Case {
      operand,
      conditions,
      results,
      else_result,
      ..
    } => {
      if let Some(op) = operand {
        extract_from_expr(op, table_names);
      }
      for cond in conditions {
        extract_from_expr(cond, table_names);
      }
      for result in results {
        extract_from_expr(result, table_names);
      }
      if let Some(else_expr) = else_result {
        extract_from_expr(else_expr, table_names);
      }
    }
    Expr::InList { expr, list, .. } => {
      extract_from_expr(expr, table_names);
      for item in list {
        extract_from_expr(item, table_names);
      }
    }
    Expr::Between { expr, low, high, .. } => {
      extract_from_expr(expr, table_names);
      extract_from_expr(low, table_names);
      extract_from_expr(high, table_names);
    }
    _ => {
      // Other expression types that don't contain subqueries
    }
  }
}

/// Helper to extract from FunctionArgExpr
fn extract_from_function_arg_expr(arg: &sqlparser::ast::FunctionArgExpr, table_names: &mut HashSet<String>) {
  match arg {
    sqlparser::ast::FunctionArgExpr::Expr(expr) => {
      extract_from_expr(expr, table_names);
    }
    sqlparser::ast::FunctionArgExpr::QualifiedWildcard(_) => {}
    sqlparser::ast::FunctionArgExpr::Wildcard => {}
  }
}
