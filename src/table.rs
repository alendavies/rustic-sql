use crate::register::Register;

/// Table struct
///
/// # Fields
///
/// * `columns` - Vector of columns
/// * `registers` - Vector of registers
///
/// # Examples
/// ```
/// let table = Table::new();
/// ```
#[derive(Debug)]
pub struct Table {
    pub columns: Vec<String>,
    pub registers: Vec<Register>,
}

impl Table {
    /// Creates a new Table, with empty columns and registers.
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
            registers: Vec::new(),
        }
    }
}
