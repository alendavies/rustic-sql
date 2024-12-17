mod clauses;
mod errors;
mod logical_operator;
mod operator;
mod register;
mod table;
mod tokens;
mod utils;

use clauses::{delete_sql::Delete, insert_sql::Insert, select_sql::Select, update_sql::Update};
use errors::SqlError;
use std::env;
use tokens::tokens_from_query;
use utils::table_to_csv;

/// Matches the first token of the query and executes the corresponding SQL clause.
///
/// Returns a vector of strings with the result of the query for SELECT clauses.
///
/// Returns an empty vector for INSERT, DELETE and UPDATE clauses because they don't show results in the console.
///
/// Returns an error for invalid syntax or unknown clauses.
///
/// # Arguments
///
/// * `folder_path` - A string slice that holds the path to the folder where the tables are stored.
/// * `query` - A string slice that holds the SQL query to be executed.
///
/// # Examples
///
/// ```
/// let folder_path = "tables";
/// let query = "SELECT * FROM table1";
/// let result = exec_query(folder_path, query);
///
/// assert_eq!(result, Ok(vec!["1,Alen,25".to_string()]));
///
///
/// let folder_path = "tables";
/// let query = "INSERT INTO table1 (id, name, age) VALUES (2, Bob, 30)";
/// let result = exec_query(folder_path, query);
///
/// assert_eq!(result, Ok(vec![]));
/// ```
///
fn exec_query(folder_path: &str, query: &str) -> Result<Vec<String>, SqlError> {
    let tokens = tokens_from_query(query);
    let mut result_csv = Vec::new();

    match tokens.first().ok_or(SqlError::InvalidSyntax)?.as_str() {
        "SELECT" => {
            let clause = Select::new_from_tokens(tokens)?;
            let table = clause.open_table(folder_path)?;

            let result = clause.apply_to_table(table)?;
            if clause.columns[0] == "*" {
                result_csv = table_to_csv(&result, &result.columns)?;
            } else {
                result_csv = table_to_csv(&result, &clause.columns)?;
            }
        }
        "INSERT" => {
            let mut clause = Insert::new_from_tokens(tokens)?;
            let mut file = clause.open_table(folder_path)?;

            clause.apply_to_table(&mut file)?;
        }
        "DELETE" => {
            let clause = Delete::new_from_tokens(tokens)?;
            let table = clause.open_table(folder_path)?;

            let result = clause.apply_to_table(table)?;

            let csv = table_to_csv(&result, &result.columns)?;

            clause.write_table(csv, folder_path)?;
        }
        "UPDATE" => {
            let clause = Update::new_from_tokens(tokens)?;
            let table = clause.open_table(folder_path)?;

            let result = clause.apply_to_table(table)?;

            let csv = table_to_csv(&result, &result.columns)?;

            clause.write_table(csv, folder_path)?;
        }
        _ => {
            return Err(SqlError::InvalidSyntax);
        }
    }
    Ok(result_csv)
}

fn main() -> Result<(), SqlError> {
    let args: Vec<String> = env::args().collect();

    let result = exec_query(&args[1], &args[2]);

    match result {
        Ok(csv) => {
            for line in csv {
                println!("{}", line);
            }
        }
        Err(e) => println!("{}", e),
    }

    Ok(())
}
