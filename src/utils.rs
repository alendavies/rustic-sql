use crate::{errors::SqlError, table::Table};
use std::{fs, path::Path};

/// Searches for the file given in the folder path, returns true if the file is found.
///
/// # Examples
///
/// ```
/// let folder_path = "tables";
/// let file_name = "clients.csv";
/// let result = utils::find_file_in_folder(folder_path, file_name);
/// assert_eq!(result, true);
/// ```
///
pub fn find_file_in_folder(folder_path: &str, file_name: &str) -> bool {
    let path = Path::new(folder_path);
    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.flatten() {
            if let Ok(file_type) = entry.file_type() {
                if file_type.is_file() && entry.file_name() == file_name {
                    return true;
                }
            }
        }
    }
    false
}

/// Transforms a table into a csv format where the first line is the column names and the following lines are the registers.
/// The column order is given by the column_order parameter.
/// Returns a vector of strings, where each string is a line in the csv.
///
/// # Examples
///
/// ```
/// let table = Table {
///     name: "clients".to_string(),
///     columns: vec!["id".to_string(), "name".to_string(), "age".to_string()],
///     registers: vec![
///         Register {values: vec!["1".to_string(), "Alen".to_string(), "30".to_string()]},
///         Register {values: vec!["2".to_string(), "Emily".to_string(), "25".to_string()]},
///     ],
/// };
///
/// let column_order = vec!["id".to_string(), "name".to_string(), "age".to_string()];
/// let result = utils::table_to_csv(&table, &column_order);
/// assert_eq!(result, vec!["id,name,age", "1,Alen,30", "2,Emily,25"]);
/// ```
///
pub fn table_to_csv(table: &Table, column_order: &Vec<String>) -> Result<Vec<String>, SqlError> {
    let mut result: Vec<String> = Vec::new();

    result.push(column_order.join(","));

    for register in &table.registers {
        let register_csv = register.to_csv(column_order)?;
        result.push(register_csv);
    }

    Ok(result)
}

/// Returns true if the token can be converted to an i32 value.
///
/// # Examples
///
/// ```
/// let token = "123";
/// let result = utils::is_number(token);
/// assert_eq!(result, true);
///
/// let token = "hola"
/// let result = utils::is_number(token);
/// assert_eq!(result, false);
/// ```
///
pub fn is_number(token: &str) -> bool {
    token.parse::<i32>().is_ok()
}

/// Returns true if the token is equal to "AND".
pub fn is_and(token: &str) -> bool {
    token == "AND"
}

/// Returns true if the token is equal to "OR".
pub fn is_or(token: &str) -> bool {
    token == "OR"
}

/// Returns true if the token is equal to "NOT".
pub fn is_not(token: &str) -> bool {
    token == "NOT"
}

/// Returns true if the token is equal to "(".
pub fn is_left_paren(token: &str) -> bool {
    token == "("
}

/// Returns true if the token is equal to ")".
pub fn is_right_paren(token: &str) -> bool {
    token == ")"
}

/// Returns true if the token is equal to "WHERE".
pub fn is_where(token: &str) -> bool {
    token == "WHERE"
}

/// Returns true if the token is equal to "SELECT".
pub fn is_select(token: &str) -> bool {
    token == "SELECT"
}

/// Returns true if the token is equal to "UPDATE".
pub fn is_update(token: &str) -> bool {
    token == "UPDATE"
}

/// Returns true if the token is equal to "INSERT".
pub fn is_insert(token: &str) -> bool {
    token == "INSERT"
}

/// Returns true if the token is equal to "INTO".
pub fn is_into(token: &str) -> bool {
    token == "INTO"
}

/// Returns true if the token is equal to "FROM".
pub fn is_from(token: &str) -> bool {
    token == "FROM"
}

/// Returns true if the token is equal to "ORDER".
pub fn is_order(token1: &str) -> bool {
    token1 == "ORDER"
}

/// Returns true if the token is equal to "BY".
pub fn is_by(token1: &str) -> bool {
    token1 == "BY"
}

/// Returns true if the token is equal to "DELETE".
pub fn is_delete(token: &str) -> bool {
    token == "DELETE"
}

/// Returns true if the token is equal to "SET".
pub fn is_set(token: &str) -> bool {
    token == "SET"
}

/// Returns true if the token is equal to "VALUES".
pub fn is_values(token: &str) -> bool {
    token == "VALUES"
}
