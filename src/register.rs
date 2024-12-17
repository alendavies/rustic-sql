use crate::errors::SqlError;
use std::collections::HashMap;

/// Register struct
///
/// The `Register` struct represents a table row.
///
/// # Fields
///
/// * A `HashMap` containing the column name and the column value.
///
/// # Examples
///
/// ```
/// let register = Register(HashMap::new());
///
/// ```
///
#[derive(Clone, Debug, PartialEq)]
pub struct Register(pub HashMap<String, String>);

impl Register {
    /// Converts a register to a csv format.
    /// The column order is given by the columns parameter.
    ///
    /// Returns a string with the values of the register separated by commas.
    ///
    /// If a column is not found in the register, returns an error.
    ///
    /// # Examples
    ///
    /// ```
    /// let register = Register(HashMap::new());
    /// let columns = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    /// let result = register.to_csv(&columns);
    ///
    /// assert_eq!(result, Err(SqlError::Error));
    ///
    ///
    /// let mut table = HashMap::new();
    /// table.insert("id".to_string(), "1".to_string());
    /// table.insert("name".to_string(), "Alen".to_string());
    /// table.insert("age".to_string(), "25".to_string());
    ///
    /// let register = Register(table);
    /// let columns = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    /// let result = register.to_csv(&columns);
    ///
    /// assert_eq!(result, Ok("1,Alen,25".to_string()));
    /// ```
    ///
    pub fn to_csv(&self, columns: &Vec<String>) -> Result<String, SqlError> {
        let mut values = Vec::new();

        for col in columns {
            let value = self.0.get(col).ok_or(SqlError::Error)?;
            values.push(value.to_string());
        }

        let csv = values.join(",");

        Ok(csv)
    }
}
