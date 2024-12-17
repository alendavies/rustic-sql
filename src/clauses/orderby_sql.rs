use crate::{
    errors::SqlError,
    register::Register,
    utils::{is_by, is_order},
};
use std::cmp::Ordering;

/// Struct that epresents the `ORDER BY` SQL clause.
/// The `ORDER BY` clause is used to sort the result set in ascending or descending order in a `SELECT` clause.
///
/// # Fields
///
/// * `columns` - The columns to sort the result set by.
/// * `order` - The order to sort the result set by. It can be either `ASC` or `DESC`.
///
#[derive(Debug, PartialEq)]
pub struct OrderBy {
    pub columns: Vec<String>,
    pub order: String,
}

impl OrderBy {
    /// Creates and returns a new `OrderBy` instance from a vector of `&str` tokens.
    ///
    /// # Arguments
    ///
    /// * `tokens` - A vector of `&str` tokens that represent the `ORDER BY` clause.
    ///
    /// The tokens should be in the following order: `ORDER`, `BY`, `columns`, `order`.
    ///
    /// The `columns` should be comma-separated.
    ///
    /// The `order` can be `ASC` or `DESC`.
    /// If the `order` is not specified, the result set will be sorted in ascending order.
    ///
    /// # Examples
    ///
    /// ```
    /// let tokens = vec!["ORDER", "BY", "name", "DESC"];
    /// let order_by = OrderBy::new_from_tokens(tokens).unwrap();
    /// assert_eq!(order_by., OrderBy { columns: vec!["name".to_string()], order: "DESC".to_string() });
    /// ```
    ///
    pub fn new_from_tokens(tokens: Vec<&str>) -> Result<Self, SqlError> {
        if tokens.len() < 3 {
            return Err(SqlError::InvalidSyntax);
        }

        let mut columns = Vec::new();
        let mut order = String::new();
        let mut i = 0;

        if !is_order(tokens[i]) && !is_by(tokens[i + 1]) {
            return Err(SqlError::InvalidSyntax);
        }

        i += 2;

        while i < tokens.len() && tokens[i] != "DESC" && tokens[i] != "ASC" {
            columns.push(tokens[i].to_string());
            i += 1;
        }

        if i < tokens.len() {
            order = tokens[i].to_string();
        }

        Ok(Self { columns, order })
    }

    /// Sorts the registers by the columns and order specified in the `ORDER BY` clause.
    ///
    /// # Arguments
    ///
    /// * `registers` - A mutable reference to a vector of `Register`.
    ///
    pub fn execute<'a>(&self, registers: &'a mut Vec<Register>) -> &'a Vec<Register> {
        registers.sort_by(|val_a, val_b| {
            let mut result = Ordering::Equal;
            for column in &self.columns {
                if let Some(val_a) = val_a.0.get(column) {
                    if let Some(val_b) = val_b.0.get(column) {
                        result = if self.order == "DESC" {
                            val_b.cmp(val_a)
                        } else {
                            val_a.cmp(val_b)
                        };
                        if result != Ordering::Equal {
                            break;
                        }
                    }
                }
            }
            result
        });
        registers
    }
}
