use super::where_sql::Where;
use crate::utils::{is_delete, is_from, is_where};
use crate::{errors::SqlError, register::Register, table::Table, utils::find_file_in_folder};
use std::io::Write;
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufRead, BufReader},
};

/// Struct that represents the `DELETE` SQL clause.
/// The `DELETE` clause is used to delete records from a table.
///
/// # Fields
///
/// - `table_name`: a `String` that holds the name of the table from which the records will be deleted.
/// - `where_clause`: an `Option<Where>` that holds the condition that the records must meet to be deleted. If it is `None`, all records will be deleted.
///
#[derive(PartialEq, Debug)]
pub struct Delete {
    pub table_name: String,
    pub where_clause: Option<Where>,
}

impl Delete {
    /// Creates and returns a new `Delete` instance from tokens.
    ///
    /// # Arguments
    ///
    /// - `tokens`: a `Vec<String>` that holds the tokens that form the `DELETE` clause.
    ///
    /// The tokens must be in the following order: `DELETE`, `FROM`, `table_name`, `WHERE`, `condition`.
    ///
    /// If the `WHERE` clause is not present, the `where_clause` field will be `None`.
    ///
    /// # Examples
    ///
    /// ```
    /// let tokens = vec![
    ///     String::from("DELETE"),
    ///     String::from("FROM"),
    ///     String::from("table"),
    /// ];
    /// let delete = Delete::new_from_tokens(tokens).unwrap();
    ///
    /// assert_eq!(
    ///    delete,
    ///     Delete {
    ///         table_name: String::from("table"),
    ///         where_clause: None
    ///     }
    /// );
    /// ```
    ///
    pub fn new_from_tokens(tokens: Vec<String>) -> Result<Self, SqlError> {
        if tokens.len() < 3 {
            return Err(SqlError::InvalidSyntax);
        }
        let mut where_tokens: Vec<&str> = Vec::new();

        let mut i = 0;
        let mut table_name = String::new();

        while i < tokens.len() {
            if i == 0 && !is_delete(&tokens[i]) || i == 1 && !is_from(&tokens[i]) {
                return Err(SqlError::InvalidSyntax);
            }
            if i == 1 && is_from(&tokens[i]) && i + 1 < tokens.len() {
                table_name = tokens[i + 1].to_string();
            }

            if i == 3 && is_where(&tokens[i]) {
                while i < tokens.len() {
                    where_tokens.push(tokens[i].as_str());
                    i += 1;
                }
            }
            i += 1;
        }

        if table_name.is_empty() {
            return Err(SqlError::InvalidSyntax);
        }

        let mut where_clause = None;

        if !where_tokens.is_empty() {
            where_clause = Some(Where::new_from_tokens(where_tokens)?);
        }

        Ok(Self {
            table_name,
            where_clause,
        })
    }

    /// Applies the `DELETE` clause to the given table.
    ///
    /// Returns a new table with the records that do not meet the condition.
    /// The ones that meet the condition will be deleted.
    ///
    /// If the `WHERE` clause is not present, all records will be deleted.
    ///
    /// # Arguments
    ///
    /// - `table`: a `BufReader<File>` that holds the table to which the `DELETE` clause will be applied.
    ///
    pub fn apply_to_table(&self, table: BufReader<File>) -> Result<Table, SqlError> {
        let mut result = Table::new();

        for (idx, line) in table.lines().enumerate() {
            let line = line.map_err(|_| SqlError::Error)?;

            if idx == 0 {
                result.columns = line.split(',').map(|s| s.to_string()).collect();
                if self.where_clause.is_none() {
                    return Ok(result);
                }
                continue;
            }
            let register = self.execute(line, &result.columns)?;

            if !register.0.is_empty() {
                result.registers.push(register);
            }
        }
        Ok(result)
    }

    fn execute(&self, line: String, columns: &Vec<String>) -> Result<Register, SqlError> {
        let atributes: Vec<String> = line.split(',').map(|s| s.to_string()).collect();

        let mut register = Register(HashMap::new());

        for (idx, col) in columns.iter().enumerate() {
            register
                .0
                .insert(col.to_string(), atributes[idx].to_string());
        }

        let mut result = Register(HashMap::new());

        if let Some(where_clause) = &self.where_clause {
            let op_result = where_clause.execute(&register)?;

            if !op_result {
                for col in columns {
                    result.0.insert(
                        col.to_string(),
                        register.0.get(col).unwrap_or(&String::new()).to_string(),
                    );
                }
            }
        }
        Ok(result)
    }

    /// Updates the table file with the new data after the `DELETE` clause is applied.
    ///
    /// # Arguments
    ///
    /// - `csv`: a `Vec<String>` that holds the new data to be written to the table file.
    /// - `folder_path`: a `&str` that holds the path to the folder where the table file is located.
    ///
    pub fn write_table(&self, csv: Vec<String>, folder_path: &str) -> Result<(), SqlError> {
        let temp_file_path = folder_path.to_string() + "/" + "temp.csv";
        let mut temp_file = File::create(&temp_file_path).map_err(|_| SqlError::Error)?;

        for line in csv {
            writeln!(temp_file, "{}", line).map_err(|_| SqlError::Error)?;
        }

        let path = folder_path.to_string() + "/" + &self.table_name + ".csv";

        fs::rename(&temp_file_path, path).map_err(|_| SqlError::Error)?;

        Ok(())
    }

    /// Opens the table file to which the `DELETE` clause will be applied.
    ///
    /// # Arguments
    ///
    /// - `folder_path`: a `&str` that holds the path to the folder where the table file is located.
    ///
    pub fn open_table(&self, folder_path: &str) -> Result<BufReader<File>, SqlError> {
        let table_name = self.table_name.to_string() + ".csv";
        if !find_file_in_folder(folder_path, &table_name) {
            return Err(SqlError::InvalidTable);
        }

        let table_path = folder_path.to_string() + "/" + &table_name;
        let file = File::open(&table_path).map_err(|_| SqlError::InvalidTable)?;

        let reader = BufReader::new(file);

        Ok(reader)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::Delete;
    use crate::{
        clauses::{condition::Condition, where_sql::Where},
        errors::SqlError,
        operator::Operator,
        register::Register,
        table::Table,
    };

    #[test]
    fn new_1_token() {
        let tokens = vec![String::from("DELETE")];
        let delete = Delete::new_from_tokens(tokens);
        assert_eq!(delete, Err(SqlError::InvalidSyntax));
    }

    #[test]
    fn new_2_token() {
        let tokens = vec![String::from("DELETE"), String::from("FROM")];
        let delete = Delete::new_from_tokens(tokens);
        assert_eq!(delete, Err(SqlError::InvalidSyntax));
    }

    #[test]
    fn new_without_where() {
        let tokens = vec![
            String::from("DELETE"),
            String::from("FROM"),
            String::from("table"),
        ];
        let delete = Delete::new_from_tokens(tokens).unwrap();
        assert_eq!(
            delete,
            Delete {
                table_name: String::from("table"),
                where_clause: None
            }
        );
    }

    #[test]
    fn new_4_tokens() {
        let tokens = vec![
            String::from("DELETE"),
            String::from("FROM"),
            String::from("table"),
            String::from("WHERE"),
        ];
        let delete = Delete::new_from_tokens(tokens);
        assert_eq!(delete, Err(SqlError::InvalidSyntax));
    }

    #[test]
    fn new_with_where() {
        let tokens = vec![
            String::from("DELETE"),
            String::from("FROM"),
            String::from("table"),
            String::from("WHERE"),
            String::from("cantidad"),
            String::from(">"),
            String::from("1"),
        ];
        let delete = Delete::new_from_tokens(tokens).unwrap();
        assert_eq!(
            delete,
            Delete {
                table_name: String::from("table"),
                where_clause: Some(Where {
                    condition: Condition::Simple {
                        field: String::from("cantidad"),
                        operator: Operator::Greater,
                        value: String::from("1")
                    }
                }),
            }
        );
    }

    #[test]
    fn delete_without_where_should_delete_all() {
        let delete = Delete {
            table_name: String::from("testing"),
            where_clause: None,
        };
        let folder_path = String::from("tablas");
        let reader = delete.open_table(&folder_path).unwrap();

        let table = delete.apply_to_table(reader).unwrap();
        let expected = Table {
            columns: vec![
                String::from("nombre"),
                String::from("apellido"),
                String::from("edad"),
            ],
            registers: vec![],
        };

        assert_eq!(table.registers, expected.registers);
        assert_eq!(table.columns, expected.columns);
    }

    #[test]
    fn delete_with_where() {
        let delete = Delete {
            table_name: String::from("testing"),
            where_clause: Some(Where {
                condition: Condition::Simple {
                    field: String::from("edad"),
                    operator: Operator::Greater,
                    value: String::from("18"),
                },
            }),
        };
        let folder_path = String::from("tablas");
        let reader = delete.open_table(&folder_path).unwrap();

        let table = delete.apply_to_table(reader).unwrap();
        let expected = Table {
            columns: vec![
                String::from("nombre"),
                String::from("apellido"),
                String::from("edad"),
            ],
            registers: vec![Register(HashMap::from([
                (String::from("nombre"), String::from("Ana")),
                (String::from("apellido"), String::from("López")),
                (String::from("edad"), String::from("18")),
            ]))],
        };

        assert_eq!(table.registers, expected.registers);
        assert_eq!(table.columns, expected.columns);
    }
}
