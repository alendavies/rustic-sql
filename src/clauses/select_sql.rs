use super::{orderby_sql::OrderBy, where_sql::Where};
use crate::{
    errors::SqlError,
    register::Register,
    table::Table,
    utils::{find_file_in_folder, is_by, is_from, is_order, is_select, is_where},
};
use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
};

/// Struct that represents the `SELECT` SQL clause.
/// The `SELECT` clause is used to select data from a table.
///
/// # Fields
///
/// * `table_name` - The name of the table to select data from.
/// * `columns` - The columns to select from the table.
/// * `where_clause` - The `WHERE` clause to filter the result set.
/// * `orderby_clause` - The `ORDER BY` clause to sort the result set.
///
#[derive(Debug, PartialEq)]
pub struct Select {
    pub table_name: String,
    pub columns: Vec<String>,
    pub where_clause: Option<Where>,
    pub orderby_clause: Option<OrderBy>,
}

fn parse_columns<'a>(tokens: &'a [String], i: &mut usize) -> Result<Vec<&'a String>, SqlError> {
    let mut columns = Vec::new();
    if is_select(&tokens[*i]) {
        if *i < tokens.len() {
            *i += 1;
            while !is_from(&tokens[*i]) && *i < tokens.len() {
                columns.push(&tokens[*i]);
                *i += 1;
            }
        }
    } else {
        return Err(SqlError::InvalidSyntax);
    }
    Ok(columns)
}

fn parse_table_name(tokens: &[String], i: &mut usize) -> Result<String, SqlError> {
    if *i < tokens.len() && is_from(&tokens[*i]) {
        *i += 1;
        let table_name = tokens[*i].to_string();
        *i += 1;
        Ok(table_name)
    } else {
        Err(SqlError::InvalidSyntax)
    }
}

fn parse_where_and_orderby<'a>(
    tokens: &'a [String],
    i: &mut usize,
) -> Result<(Vec<&'a str>, Vec<&'a str>), SqlError> {
    let mut where_tokens = Vec::new();
    let mut orderby_tokens = Vec::new();

    if *i < tokens.len() {
        if is_where(&tokens[*i]) {
            while *i < tokens.len() && !is_order(&tokens[*i]) {
                where_tokens.push(tokens[*i].as_str());
                *i += 1;
            }
        }
        if *i < tokens.len() && is_order(&tokens[*i]) {
            orderby_tokens.push(tokens[*i].as_str());
            *i += 1;
            if *i < tokens.len() && is_by(&tokens[*i]) {
                while *i < tokens.len() {
                    orderby_tokens.push(tokens[*i].as_str());
                    *i += 1;
                }
            }
        }
    }
    Ok((where_tokens, orderby_tokens))
}

fn convert_line_to_register(line: String, columns: &[String]) -> Register {
    let attributes: Vec<String> = line.split(',').map(|s| s.to_string()).collect();
    let mut original = Register(HashMap::new());
    for (idx, col) in columns.iter().enumerate() {
        original
            .0
            .insert(col.to_string(), attributes[idx].to_string());
    }

    original
}

impl Select {
    /// Creates and returns a new `Select` instance from a vector of `String` tokens.
    ///
    /// # Arguments
    ///
    /// * `tokens` - A vector of `String` tokens that represent the `SELECT` clause.
    ///
    /// The tokens should be in the following order: `SELECT`, `columns`, `FROM`, `table_name`, `WHERE`, `condition`, `ORDER`, `BY`, `columns`, `order`.
    ///
    /// The `columns` should be comma-separated.
    ///
    pub fn new_from_tokens(tokens: Vec<String>) -> Result<Self, SqlError> {
        if tokens.len() < 4 {
            return Err(SqlError::InvalidSyntax);
        }

        let mut i = 0;

        let columns = parse_columns(&tokens, &mut i)?;
        let table_name = parse_table_name(&tokens, &mut i)?;

        if columns.is_empty() || table_name.is_empty() {
            return Err(SqlError::InvalidSyntax);
        }

        let (where_tokens, orderby_tokens) = parse_where_and_orderby(&tokens, &mut i)?;

        let where_clause = if !where_tokens.is_empty() {
            Some(Where::new_from_tokens(where_tokens)?)
        } else {
            None
        };

        let orderby_clause = if !orderby_tokens.is_empty() {
            Some(OrderBy::new_from_tokens(orderby_tokens)?)
        } else {
            None
        };

        Ok(Self {
            table_name,
            columns: columns.iter().map(|c| c.to_string()).collect(),
            where_clause,
            orderby_clause,
        })
    }

    fn filter_columns(&self, columns: &Vec<String>, registers: Vec<Register>) -> Vec<Register> {
        let mut cols_selected = Vec::new();
        if self.columns[0] == "*" {
            for col in columns {
                cols_selected.push(col.to_string());
            }
        } else {
            for col in &self.columns {
                cols_selected.push(col.to_string());
            }
        }

        let mut filtered_registers = Vec::new();
        for register in registers {
            let filtered: HashMap<String, String> = register
                .0
                .into_iter()
                .filter(|(key, _)| cols_selected.contains(key))
                .collect();

            filtered_registers.push(Register(filtered));
        }

        filtered_registers
    }

    /// Applies the `SELECT` clause to a table and returns the resulting `Table`.
    ///
    /// # Arguments
    ///
    /// * `table` - A `BufReader<File>` that represents the table to apply the `SELECT` clause to.
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

        if let Some(orderby) = &self.orderby_clause {
            let ordered_registers = orderby.execute(&mut result.registers).to_vec();
            result.registers = self.filter_columns(&result.columns, ordered_registers);
        } else {
            result.registers = self.filter_columns(&result.columns, result.registers);
        }

        Ok(result)
    }

    fn execute(&self, line: String, columns: &Vec<String>) -> Result<Register, SqlError> {
        if !self.columns.iter().all(|col| columns.contains(col)) && self.columns[0] != "*" {
            return Err(SqlError::InvalidColumn);
        }

        let original = convert_line_to_register(line, columns);
        let mut result = Register(HashMap::new());

        if let Some(where_clause) = &self.where_clause {
            let op_result = where_clause.execute(&original)?;
            if op_result {
                for col in columns {
                    result.0.insert(
                        col.to_string(),
                        original.0.get(col).unwrap_or(&String::new()).to_string(),
                    );
                }
            }
        } else {
            for col in columns {
                result.0.insert(
                    col.to_string(),
                    original.0.get(col).unwrap_or(&String::new()).to_string(),
                );
            }
        }
        Ok(result)
    }

    /// Opens the table file and returns a `BufReader<File>`.
    ///
    /// # Arguments
    ///
    /// * `folder_path` - A `&str` that represents the path to the folder where the table file is located.
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

    use super::Select;
    use crate::{
        clauses::{condition::Condition, orderby_sql::OrderBy, where_sql::Where},
        errors::SqlError,
        logical_operator::LogicalOperator,
        operator::Operator,
        register::Register,
        table::Table,
    };

    #[test]
    fn new_1_tokens() {
        let tokens = vec![String::from("SELECT")];
        let select = Select::new_from_tokens(tokens);
        assert_eq!(select, Err(SqlError::InvalidSyntax));
    }

    #[test]
    fn new_2_tokens() {
        let tokens = vec![String::from("SELECT"), String::from("col")];
        let select = Select::new_from_tokens(tokens);
        assert_eq!(select, Err(SqlError::InvalidSyntax));
    }
    #[test]
    fn new_3_tokens() {
        let tokens = vec![
            String::from("SELECT"),
            String::from("col"),
            String::from("FROM"),
        ];
        let select = Select::new_from_tokens(tokens);
        assert_eq!(select, Err(SqlError::InvalidSyntax));
    }

    #[test]
    fn new_4_tokens() {
        let tokens = vec![
            String::from("SELECT"),
            String::from("col"),
            String::from("FROM"),
            String::from("table"),
        ];
        let select = Select::new_from_tokens(tokens).unwrap();
        assert_eq!(select.columns, ["col"]);
        assert_eq!(select.table_name, "table");
        assert_eq!(select.where_clause, None);
        assert_eq!(select.orderby_clause, None);
    }

    #[test]
    fn new_with_where() {
        let tokens = vec![
            String::from("SELECT"),
            String::from("col"),
            String::from("FROM"),
            String::from("table"),
            String::from("WHERE"),
            String::from("cantidad"),
            String::from(">"),
            String::from("1"),
        ];
        let select = Select::new_from_tokens(tokens).unwrap();
        assert_eq!(select.columns, ["col"]);
        assert_eq!(select.table_name, "table");
        let where_clause = select.where_clause.unwrap();
        assert_eq!(
            where_clause.condition,
            Condition::Simple {
                field: String::from("cantidad"),
                operator: Operator::Greater,
                value: String::from("1"),
            }
        );
        assert_eq!(select.orderby_clause, None);
    }

    #[test]
    fn new_with_orderby() {
        let tokens = vec![
            String::from("SELECT"),
            String::from("col"),
            String::from("FROM"),
            String::from("table"),
            String::from("ORDER"),
            String::from("BY"),
            String::from("cantidad"),
            String::from("DESC"),
        ];
        let select = Select::new_from_tokens(tokens).unwrap();
        assert_eq!(select.columns, ["col"]);
        assert_eq!(select.table_name, "table");
        let orderby_clause = select.orderby_clause.unwrap();
        assert_eq!(
            orderby_clause,
            OrderBy {
                columns: vec![String::from("cantidad")],
                order: String::from("DESC")
            }
        );
        assert_eq!(select.where_clause, None);
    }

    #[test]
    fn new_with_where_orderby() {
        let tokens = vec![
            String::from("SELECT"),
            String::from("col"),
            String::from("FROM"),
            String::from("table"),
            String::from("WHERE"),
            String::from("cantidad"),
            String::from(">"),
            String::from("1"),
            String::from("ORDER"),
            String::from("BY"),
            String::from("email"),
        ];
        let select = Select::new_from_tokens(tokens).unwrap();
        assert_eq!(select.columns, ["col"]);
        assert_eq!(select.table_name, "table");
        let where_clause = select.where_clause.unwrap();
        assert_eq!(
            where_clause.condition,
            Condition::Simple {
                field: String::from("cantidad"),
                operator: Operator::Greater,
                value: String::from("1"),
            }
        );
        let orderby_clause = select.orderby_clause.unwrap();
        let mut columns = Vec::new();
        columns.push(String::from("email"));
        assert_eq!(
            orderby_clause,
            OrderBy {
                columns,
                order: String::new()
            }
        );
    }

    #[test]
    fn select_all_without_where() {
        let select = Select {
            table_name: String::from("testing"),
            columns: vec![String::from("*")],
            where_clause: None,
            orderby_clause: None,
        };
        let folder_path = String::from("tablas");
        let reader = select.open_table(&folder_path).unwrap();

        let table = select.apply_to_table(reader).unwrap();
        let expected = Table {
            columns: vec![
                String::from("nombre"),
                String::from("apellido"),
                String::from("edad"),
            ],
            registers: vec![
                Register(HashMap::from([
                    (String::from("nombre"), String::from("Juan")),
                    (String::from("apellido"), String::from("Pérez")),
                    (String::from("edad"), String::from("30")),
                ])),
                Register(HashMap::from([
                    (String::from("nombre"), String::from("Ana")),
                    (String::from("apellido"), String::from("López")),
                    (String::from("edad"), String::from("18")),
                ])),
                Register(HashMap::from([
                    (String::from("nombre"), String::from("Carlos")),
                    (String::from("apellido"), String::from("Gómez")),
                    (String::from("edad"), String::from("40")),
                ])),
            ],
        };

        assert_eq!(table.registers, expected.registers);
        assert_eq!(table.columns, expected.columns);
    }

    #[test]
    fn select_all_without_where_orderby() {
        let select = Select {
            table_name: String::from("testing"),
            columns: vec![String::from("*")],
            where_clause: None,
            orderby_clause: Some(OrderBy {
                columns: vec![String::from("edad")],
                order: String::new(),
            }),
        };
        let folder_path = String::from("tablas");
        let reader = select.open_table(&folder_path).unwrap();

        let table = select.apply_to_table(reader).unwrap();
        let expected = Table {
            columns: vec![
                String::from("nombre"),
                String::from("apellido"),
                String::from("edad"),
            ],
            registers: vec![
                Register(HashMap::from([
                    (String::from("nombre"), String::from("Ana")),
                    (String::from("apellido"), String::from("López")),
                    (String::from("edad"), String::from("18")),
                ])),
                Register(HashMap::from([
                    (String::from("nombre"), String::from("Juan")),
                    (String::from("apellido"), String::from("Pérez")),
                    (String::from("edad"), String::from("30")),
                ])),
                Register(HashMap::from([
                    (String::from("nombre"), String::from("Carlos")),
                    (String::from("apellido"), String::from("Gómez")),
                    (String::from("edad"), String::from("40")),
                ])),
            ],
        };

        assert_eq!(table.registers, expected.registers);
        assert_eq!(table.columns, expected.columns);
    }

    #[test]
    fn select_all_with_where() {
        let select = Select {
            table_name: String::from("testing"),
            columns: vec![String::from("*")],
            where_clause: Some(Where {
                condition: Condition::Simple {
                    field: String::from("edad"),
                    operator: Operator::Greater,
                    value: String::from("18"),
                },
            }),
            orderby_clause: None,
        };
        let folder_path = String::from("tablas");
        let reader = select.open_table(&folder_path).unwrap();

        let table = select.apply_to_table(reader).unwrap();
        let expected = Table {
            columns: vec![
                String::from("nombre"),
                String::from("apellido"),
                String::from("edad"),
            ],
            registers: vec![
                Register(HashMap::from([
                    (String::from("nombre"), String::from("Juan")),
                    (String::from("apellido"), String::from("Pérez")),
                    (String::from("edad"), String::from("30")),
                ])),
                Register(HashMap::from([
                    (String::from("nombre"), String::from("Carlos")),
                    (String::from("apellido"), String::from("Gómez")),
                    (String::from("edad"), String::from("40")),
                ])),
            ],
        };

        assert_eq!(table.registers, expected.registers);
        assert_eq!(table.columns, expected.columns);
    }

    #[test]
    fn select_all_with_where_orderby() {
        let select = Select {
            table_name: String::from("testing"),
            columns: vec![String::from("*")],
            where_clause: Some(Where {
                condition: Condition::Simple {
                    field: String::from("edad"),
                    operator: Operator::Greater,
                    value: String::from("18"),
                },
            }),
            orderby_clause: Some(OrderBy {
                columns: vec![String::from("edad")],
                order: String::from("DESC"),
            }),
        };
        let folder_path = String::from("tablas");
        let reader = select.open_table(&folder_path).unwrap();

        let table = select.apply_to_table(reader).unwrap();
        let expected = Table {
            columns: vec![
                String::from("nombre"),
                String::from("apellido"),
                String::from("edad"),
            ],
            registers: vec![
                Register(HashMap::from([
                    (String::from("nombre"), String::from("Carlos")),
                    (String::from("apellido"), String::from("Gómez")),
                    (String::from("edad"), String::from("40")),
                ])),
                Register(HashMap::from([
                    (String::from("nombre"), String::from("Juan")),
                    (String::from("apellido"), String::from("Pérez")),
                    (String::from("edad"), String::from("30")),
                ])),
            ],
        };

        assert_eq!(table.registers, expected.registers);
        assert_eq!(table.columns, expected.columns);
    }

    #[test]
    fn select_with_where_complex_orderby() {
        let select = Select {
            table_name: String::from("testing"),
            columns: vec![String::from("nombre"), String::from("apellido")],
            where_clause: Some(Where {
                condition: Condition::Complex {
                    left: Some(Box::new(Condition::Simple {
                        field: String::from("edad"),
                        operator: Operator::Greater,
                        value: String::from("18"),
                    })),
                    operator: LogicalOperator::And,
                    right: Box::new(Condition::Simple {
                        field: String::from("nombre"),
                        operator: Operator::Equal,
                        value: String::from("Carlos"),
                    }),
                },
            }),
            orderby_clause: Some(OrderBy {
                columns: vec![String::from("edad")],
                order: String::from("DESC"),
            }),
        };
        let folder_path = String::from("tablas");
        let reader = select.open_table(&folder_path).unwrap();

        let table = select.apply_to_table(reader).unwrap();
        let expected = Table {
            columns: vec![
                String::from("nombre"),
                String::from("apellido"),
                String::from("edad"),
            ],
            registers: vec![Register(HashMap::from([
                (String::from("nombre"), String::from("Carlos")),
                (String::from("apellido"), String::from("Gómez")),
            ]))],
        };

        assert_eq!(table.registers, expected.registers);
        assert_eq!(table.columns, expected.columns);
    }
}
