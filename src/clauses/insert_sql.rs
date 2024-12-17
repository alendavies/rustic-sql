use super::into_sql::Into;
use crate::errors::SqlError;
use crate::utils::{find_file_in_folder, is_insert, is_values};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};

/// Struct that represents the `INSERT` SQL clause.
/// The `INSERT` clause is used to insert new records into a table.
///
/// # Fields
///
/// * `values` - A vector of strings that contains the values to be inserted.
/// * `into_clause` - An `Into` struct that contains the table name and columns.
///
#[derive(Debug, PartialEq)]
pub struct Insert {
    pub values: Vec<String>,
    pub into_clause: Into,
}

impl Insert {
    /// Creates and returns a new `Insert` instance from a vector of tokens.
    ///
    /// # Arguments
    ///
    /// * `tokens` - A vector of strings that contains the tokens to be parsed.
    ///
    /// The tokens should be in the following order: `INSERT`, `INTO`, `table_name`, `column_names`, `VALUES`, `values`.
    ///
    /// The `column_names` and `values` should be comma-separated and between parentheses.
    ///
    /// If a pair of col, value is missing for a column in the table, the value will be an empty string for that column.
    ///
    /// # Examples
    ///
    /// ```
    /// let tokens = vec![
    ///     String::from("INSERT"),
    ///     String::from("INTO"),
    ///     String::from("table"),
    ///     String::from("name, age"),
    ///     String::from("VALUES"),
    ///     String::from("Alen, 25"),
    /// ];
    ///
    /// let insert = Insert::new_from_tokens(tokens).unwrap();
    ///
    /// assert_eq!(
    ///     insert,
    ///     Insert {
    ///         values: vec![String::from("Alen"), String::from("25")],
    ///         into_clause: Into {
    ///             table_name: String::from("table"),
    ///             columns: vec![String::from("name"), String::from("age")]
    ///         }
    ///     }
    /// );
    /// ```
    ///
    pub fn new_from_tokens(tokens: Vec<String>) -> Result<Self, SqlError> {
        if tokens.len() < 6 {
            return Err(SqlError::InvalidSyntax);
        }
        let mut into_tokens: Vec<&str> = Vec::new();
        let mut values: Vec<String> = Vec::new();

        let mut i = 0;

        if is_insert(&tokens[i]) {
            i += 1;
            while !is_values(&tokens[i]) && i < tokens.len() {
                into_tokens.push(tokens[i].as_str());
                i += 1;
            }
        }
        if is_values(&tokens[i]) {
            i += 1;

            let vals: Vec<String> = tokens[i]
                .replace("\'", "")
                .split(",")
                .map(|c| c.trim().to_string())
                .collect();

            for val in vals {
                values.push(val);
            }
        }

        if into_tokens.is_empty() || values.is_empty() {
            return Err(SqlError::InvalidSyntax);
        }

        let into_clause = Into::new_from_tokens(into_tokens)?;

        Ok(Self {
            values,
            into_clause,
        })
    }

    /// Applies the `INSERT` clause to a table.
    ///
    /// # Arguments
    ///
    /// * `file` - A mutable reference to a `File` instance that represents the table file.
    ///
    pub fn apply_to_table(&mut self, file: &mut File) -> Result<(), SqlError> {
        let mut reader = BufReader::new(file.by_ref());

        let mut first_line = String::new();

        reader
            .read_line(&mut first_line)
            .map_err(|_| SqlError::InvalidTable)?;

        let columns: Vec<String> = first_line
            .trim()
            .split(',')
            .map(|col| col.to_string())
            .collect();

        self.reorder_values(columns);

        let line = self.values.join(",");

        file.seek(SeekFrom::End(0)).map_err(|_| SqlError::Error)?;

        writeln!(file, "{}", line).map_err(|_| SqlError::Error)?;

        Ok(())
    }

    fn reorder_values(&mut self, columns: Vec<String>) {
        let mut reordered_values: Vec<&str> = Vec::new();
        let mut reordered_cols: Vec<&str> = Vec::new();

        for col in &columns {
            if self.into_clause.columns.contains(col) {
                if let Some(index) = self.into_clause.columns.iter().position(|x| x == col) {
                    reordered_values.push(self.values[index].as_str());
                }

                reordered_cols.push(col);
            } else {
                reordered_values.push("");
                reordered_cols.push(col);
            }
        }

        self.into_clause.columns = reordered_cols.iter().map(|c| c.to_string()).collect();
        self.values = reordered_values.iter().map(|c| c.to_string()).collect();
    }

    /// Opens the table file and returns a `File` instance.
    ///
    /// # Arguments
    ///
    /// * `folder_path` - A string slice that contains the path to the folder where the table file is located.
    ///
    pub fn open_table(&self, folder_path: &str) -> Result<File, SqlError> {
        let table_name = self.into_clause.table_name.to_string() + ".csv";
        if !find_file_in_folder(folder_path, &table_name) {
            return Err(SqlError::InvalidTable);
        }
        let table_path = folder_path.to_string() + "/" + &table_name;

        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&table_path)
            .map_err(|_| SqlError::InvalidTable)?;

        Ok(file)
    }
}

#[cfg(test)]
mod test {
    use crate::errors::SqlError;
    use std::io::BufRead;

    #[test]
    fn new_1_token() {
        let tokens = vec![String::from("INSERT")];
        let result = super::Insert::new_from_tokens(tokens);
        assert_eq!(result, Err(SqlError::InvalidSyntax));
    }

    #[test]
    fn new_3_tokens() {
        let tokens = vec![
            String::from("INSERT"),
            String::from("INTO"),
            String::from("table"),
        ];

        let result = super::Insert::new_from_tokens(tokens);
        assert_eq!(result, Err(SqlError::InvalidSyntax));
    }

    #[test]
    fn new_6_tokens() {
        let tokens = vec![
            String::from("INSERT"),
            String::from("INTO"),
            String::from("table"),
            String::from("name"),
            String::from("VALUES"),
            String::from("Alen"),
        ];
        let result = super::Insert::new_from_tokens(tokens).unwrap();
        assert_eq!(
            result,
            super::Insert {
                values: vec![String::from("Alen")],
                into_clause: super::Into {
                    table_name: String::from("table"),
                    columns: vec![String::from("name")]
                }
            }
        );
    }

    #[test]
    fn new_more_values() {
        let tokens = vec![
            String::from("INSERT"),
            String::from("INTO"),
            String::from("table"),
            String::from("name, age"),
            String::from("VALUES"),
            String::from("Alen, 25"),
        ];
        let result = super::Insert::new_from_tokens(tokens).unwrap();
        assert_eq!(
            result,
            super::Insert {
                values: vec![String::from("Alen"), String::from("25")],
                into_clause: super::Into {
                    table_name: String::from("table"),
                    columns: vec![String::from("name"), String::from("age")]
                }
            }
        );
    }

    #[test]
    fn insert_with_missing_values() {
        let mut insert = super::Insert {
            values: vec![String::from("Alen")],
            into_clause: super::Into {
                table_name: String::from("testing_values"),
                columns: vec![String::from("nombre")],
            },
        };

        let mut file = insert.open_table("tablas").unwrap();

        assert_eq!(insert.apply_to_table(&mut file), Ok(()));

        let expected = vec![
            "nombre,apellido,edad",
            "Juan,Pérez,30",
            "Ana,López,18",
            "Carlos,Gómez,40",
            "Alen,,",
        ];

        let file = std::fs::File::open("tablas/testing_values.csv").unwrap();
        let reader = std::io::BufReader::new(file);
        let mut result = Vec::new();

        for line in reader.lines() {
            result.push(line.unwrap());
        }

        assert_eq!(result, expected);
    }

    #[test]
    fn insert_all_values() {
        let mut insert = super::Insert {
            values: vec![
                String::from("Alen"),
                String::from("Davies"),
                String::from("25"),
            ],
            into_clause: super::Into {
                table_name: String::from("testing_all"),
                columns: vec![
                    String::from("nombre"),
                    String::from("apellido"),
                    String::from("edad"),
                ],
            },
        };

        let mut file = insert.open_table("tablas").unwrap();

        assert_eq!(insert.apply_to_table(&mut file), Ok(()));

        let expected = vec![
            "nombre,apellido,edad",
            "Juan,Pérez,30",
            "Ana,López,18",
            "Carlos,Gómez,40",
            "Alen,Davies,25",
        ];

        let file = std::fs::File::open("tablas/testing_all.csv").unwrap();
        let reader = std::io::BufReader::new(file);
        let mut result = Vec::new();

        for line in reader.lines() {
            result.push(line.unwrap());
        }

        assert_eq!(result, expected);
    }

    #[test]
    fn insert_in_desorder() {
        let mut insert = super::Insert {
            values: vec![
                String::from("Davies"),
                String::from("25"),
                String::from("Alen"),
            ],
            into_clause: super::Into {
                table_name: String::from("testing_desorder"),
                columns: vec![
                    String::from("apellido"),
                    String::from("edad"),
                    String::from("nombre"),
                ],
            },
        };

        let mut file = insert.open_table("tablas").unwrap();

        assert_eq!(insert.apply_to_table(&mut file), Ok(()));

        let expected = vec![
            "nombre,apellido,edad",
            "Juan,Pérez,30",
            "Ana,López,18",
            "Carlos,Gómez,40",
            "Alen,Davies,25",
        ];

        let file = std::fs::File::open("tablas/testing_desorder.csv").unwrap();
        let reader = std::io::BufReader::new(file);
        let mut result = Vec::new();

        for line in reader.lines() {
            result.push(line.unwrap());
        }

        assert_eq!(result, expected);
    }
}
