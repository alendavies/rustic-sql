use super::set_sql::Set;
use super::where_sql::Where;
use crate::utils::{is_set, is_update, is_where};
use crate::{errors::SqlError, register::Register, table::Table, utils::find_file_in_folder};
use std::io::Write;
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufRead, BufReader},
};

/// Struct representing the `UPDATE` SQL clause.
/// The `UPDATE` clause is used to modify records in a table.
///
/// # Fields
///
/// * `table_name` - The name of the table to be updated.
/// * `set_clause` - The set clause to be applied.
/// * `where_clause` - The where clause to be applied.
///
#[derive(PartialEq, Debug)]
pub struct Update {
    pub table_name: String,
    pub set_clause: Set,
    pub where_clause: Option<Where>,
}

impl Update {
    /// Creates and returns a new `Update` instance from a vector of tokens.
    ///
    /// # Arguments
    ///
    /// * `tokens` - A vector of tokens that can be used to build a `Update` instance.
    ///
    /// The tokens should be in the following order: `UPDATE`, `table`, `SET`, `column`, `=`, `value`.
    ///
    /// # Examples
    ///
    /// ```
    /// let tokens = vec!["UPDATE", "table", "SET", "nombre", "=", "Alen"];
    /// let update_from_tokens = Update::new_from_tokens(tokens).unwrap();
    /// let update = Update {
    ///     table_name: "table".to_string(),
    ///     set_clause: Set(vec![("nombre".to_string(), "Alen".to_string())]),
    ///     where_clause: None,
    /// };
    ///
    /// assert_eq!(update_from_tokens, update);
    /// ```
    pub fn new_from_tokens(tokens: Vec<String>) -> Result<Self, SqlError> {
        if tokens.len() < 6 {
            return Err(SqlError::InvalidSyntax);
        }
        let mut where_tokens = Vec::new();
        let mut set_tokens = Vec::new();
        let mut table_name = String::new();

        let mut i = 0;

        while i < tokens.len() {
            if i == 0 && !is_update(&tokens[i]) || i == 2 && !is_set(&tokens[i]) {
                return Err(SqlError::InvalidSyntax);
            }

            if i == 0 && is_update(&tokens[i]) && i + 1 < tokens.len() {
                table_name = tokens[i + 1].to_string();
            }

            if i == 2 && is_set(&tokens[i]) {
                while i < tokens.len() && !is_where(&tokens[i]) {
                    set_tokens.push(tokens[i].as_str());
                    i += 1;
                }
                if i < tokens.len() && is_where(&tokens[i]) {
                    while i < tokens.len() {
                        where_tokens.push(tokens[i].as_str());
                        i += 1;
                    }
                }
            }
            i += 1;
        }

        if table_name.is_empty() || set_tokens.is_empty() {
            return Err(SqlError::InvalidSyntax);
        }

        let mut where_clause = None;

        if !where_tokens.is_empty() {
            where_clause = Some(Where::new_from_tokens(where_tokens)?);
        }

        let set_clause = Set::new_from_tokens(set_tokens)?;

        Ok(Self {
            table_name,
            where_clause,
            set_clause,
        })
    }

    /// Applies the `UPDATE` clause to a given table.
    ///
    /// Reads the table and applies the set clause to the registers that meet the where clause if it exist or to all the registers if it doesn't.
    /// Returns a new table with the updated registers.
    ///
    /// # Arguments
    ///
    /// * `table` - A `BufReader<File>` that contains a reader for the table to be updated.
    ///
    pub fn apply_to_table(&self, table: BufReader<File>) -> Result<Table, SqlError> {
        let mut result = Table::new();

        for (idx, line) in table.lines().enumerate() {
            let line = line.map_err(|_| SqlError::Error)?;
            if idx == 0 {
                result.columns = line.split(',').map(|s| s.to_string()).collect();
                continue;
            }
            let register = self.execute(line, &result.columns)?;

            if !register.0.is_empty() {
                result.registers.push(register);
            }
        }
        Ok(result)
    }

    fn execute(&self, line: String, columns: &[String]) -> Result<Register, SqlError> {
        let atributes: Vec<String> = line.split(',').map(|s| s.to_string()).collect();

        let mut register = Register(HashMap::new());

        for (idx, col) in columns.iter().enumerate() {
            register
                .0
                .insert(col.to_string(), atributes[idx].to_string());
        }

        if let Some(where_clause) = &self.where_clause {
            let op_result = where_clause.execute(&register)?;

            if op_result {
                for (col, val) in &self.set_clause.0 {
                    register.0.insert(col.to_string(), val.to_string());
                }
            }
        } else {
            for (col, val) in &self.set_clause.0 {
                register.0.insert(col.to_string(), val.to_string());
            }
        }

        Ok(register)
    }

    /// Writes the updated table in csv format to the file that contains the table in the given folder path.
    ///
    /// # Arguments
    ///
    /// * `csv` - A vector of strings that contains the updated table in csv format.
    /// * `folder_path` - A string slice that contains the path to the folder where the table is located.
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

    /// Opens the table file in the given folder path.
    /// Returns a `BufReader<File>` that contains a reader for the table file.
    ///
    /// # Arguments
    ///
    /// * `folder_path` - A string slice that contains the path to the folder where the table is located.
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

    use crate::{
        clauses::{condition::Condition, set_sql::Set, update_sql::Update, where_sql::Where},
        errors::SqlError,
        operator::Operator,
        register::Register,
        table::Table,
    };

    #[test]
    fn new_1_token() {
        let tokens = vec![String::from("UPDATE")];
        let update = Update::new_from_tokens(tokens);
        assert_eq!(update, Err(SqlError::InvalidSyntax));
    }

    #[test]
    fn new_3_tokens() {
        let tokens = vec![
            String::from("UPDATE"),
            String::from("table"),
            String::from("SET"),
        ];
        let update = Update::new_from_tokens(tokens);
        assert_eq!(update, Err(SqlError::InvalidSyntax));
    }

    #[test]
    fn new_without_where() {
        let tokens = vec![
            String::from("UPDATE"),
            String::from("table"),
            String::from("SET"),
            String::from("nombre"),
            String::from("="),
            String::from("Alen"),
        ];
        let update = Update::new_from_tokens(tokens).unwrap();
        assert_eq!(
            update,
            Update {
                table_name: String::from("table"),
                set_clause: Set(vec![(String::from("nombre"), String::from("Alen"))]),
                where_clause: None
            }
        );
    }

    #[test]
    fn new_with_where() {
        let tokens = vec![
            String::from("UPDATE"),
            String::from("table"),
            String::from("SET"),
            String::from("nombre"),
            String::from("="),
            String::from("Alen"),
            String::from("WHERE"),
            String::from("edad"),
            String::from("<"),
            String::from("30"),
        ];
        let update = Update::new_from_tokens(tokens).unwrap();
        assert_eq!(
            update,
            Update {
                table_name: String::from("table"),
                set_clause: Set(vec![(String::from("nombre"), String::from("Alen"))]),
                where_clause: Some(Where {
                    condition: Condition::Simple {
                        field: String::from("edad"),
                        operator: Operator::Lesser,
                        value: String::from("30"),
                    },
                }),
            }
        );
    }

    #[test]
    fn update_without_where() {
        let update = Update {
            table_name: String::from("testing"),
            set_clause: Set(vec![(String::from("nombre"), String::from("Alen"))]),
            where_clause: None,
        };

        let folder_path = String::from("tablas");
        let reader = update.open_table(&folder_path).unwrap();

        let table = update.apply_to_table(reader).unwrap();

        let expected = Table {
            columns: vec![
                String::from("nombre"),
                String::from("apellido"),
                String::from("edad"),
            ],
            registers: vec![
                Register(HashMap::from([
                    (String::from("nombre"), String::from("Alen")),
                    (String::from("apellido"), String::from("Pérez")),
                    (String::from("edad"), String::from("30")),
                ])),
                Register(HashMap::from([
                    (String::from("nombre"), String::from("Alen")),
                    (String::from("apellido"), String::from("López")),
                    (String::from("edad"), String::from("18")),
                ])),
                Register(HashMap::from([
                    (String::from("nombre"), String::from("Alen")),
                    (String::from("apellido"), String::from("Gómez")),
                    (String::from("edad"), String::from("40")),
                ])),
            ],
        };

        assert_eq!(table.registers, expected.registers);
        assert_eq!(table.columns, expected.columns);
    }

    #[test]
    fn delete_with_where() {
        let update = Update {
            table_name: String::from("testing"),
            set_clause: Set(vec![(String::from("nombre"), String::from("Alen"))]),
            where_clause: Some(Where {
                condition: Condition::Simple {
                    field: String::from("edad"),
                    operator: Operator::Greater,
                    value: String::from("20"),
                },
            }),
        };
        let folder_path = String::from("tablas");
        let reader = update.open_table(&folder_path).unwrap();

        let table = update.apply_to_table(reader).unwrap();
        let expected = Table {
            columns: vec![
                String::from("nombre"),
                String::from("apellido"),
                String::from("edad"),
            ],
            registers: vec![
                Register(HashMap::from([
                    (String::from("nombre"), String::from("Alen")),
                    (String::from("apellido"), String::from("Pérez")),
                    (String::from("edad"), String::from("30")),
                ])),
                Register(HashMap::from([
                    (String::from("nombre"), String::from("Ana")),
                    (String::from("apellido"), String::from("López")),
                    (String::from("edad"), String::from("18")),
                ])),
                Register(HashMap::from([
                    (String::from("nombre"), String::from("Alen")),
                    (String::from("apellido"), String::from("Gómez")),
                    (String::from("edad"), String::from("40")),
                ])),
            ],
        };

        assert_eq!(table.registers, expected.registers);
        assert_eq!(table.columns, expected.columns);
    }
}
