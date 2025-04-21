use odbc_api::{Environment, Connection};
use std::sync::Mutex;
use std::cell::RefCell;
use simple_error::SimpleError;

use crate::report::*;

pub struct ReportMSSQL<'env> {
    instance: String, // Connection string for the MSSQL instance
    environment: &'env Environment, // Reference to the Environment object
    connection: Mutex<Option<Connection<'env>>>, // Mutex to manage the connection
    report_suffix: Option<ReportSuffix>,
    values: RefCell<Vec<(String /*field*/, String /*value*/)>>, // Stores the values to be inserted
}

impl<'env> ReportMSSQL<'env> {
    pub fn new(
        environment: &'env Environment,
        instance: &str,
        report_suffix: Option<ReportSuffix>,
    ) -> Result<Self, SimpleError> {
        Ok(ReportMSSQL {
            instance: instance.to_string(),
            environment, // Store a reference to the Environment object
            connection: Mutex::new(None), // Connection will be established in `start_file`
            report_suffix,
            values: RefCell::new(Vec::new()),
        })
    }

    fn connect_to_database(&self) -> Result<(), SimpleError> {
        let connection_string = format!(
            "Driver={{ODBC Driver 17 for SQL Server}};Server={};Trusted_Connection=yes;",
            self.instance
        );

        let connection = self
            .environment
            .connect_with_connection_string(&connection_string)
            .map_err(|e| SimpleError::new(format!("Failed to connect to MSSQL instance: {e}")))?;

        // Store the connection in the Mutex
        *self.connection.lock().unwrap() = Some(connection);

        println!("Successfully connected to MSSQL instance: {}", self.instance);
        Ok(())
    }

    pub fn write_values_to_db(&self) -> Result<(), SimpleError> {
        let guard = self.connection.lock().unwrap(); // Get the MutexGuard
        let connection = guard
            .as_ref()
            .ok_or_else(|| SimpleError::new("No active database connection"))?;

        let values = self.values.borrow();
        for (field, value) in values.iter() {
            // Example SQL query to insert data
            let query = format!(
                "INSERT INTO ReportTable (Field, Value) VALUES ('{}', '{}')",
                field, value
            );

            connection
                .execute(&query, ())
                .map_err(|e| SimpleError::new(format!("Failed to execute query: {e}")))?;
        }

        Ok(())
    }
}

impl<'env> Report for ReportMSSQL<'env> {
    fn start_file(&mut self) {
        if let Err(e) = self.connect_to_database() {
            panic!("Failed to connect to database: {}", e);
        }
    }

    fn create_new_row(&mut self, _f: bool) {
        if !self.values.borrow().is_empty() {
            if let Err(e) = self.write_values_to_db() {
                panic!("Failed to write values to database: {}", e);
            }
            self.values.borrow_mut().clear();
        }
    }

    fn insert_str_val(&self, f: &str, s: String) {
        self.values.borrow_mut().push((f.to_string(), s));
    }

    fn insert_int_val(&self, f: &str, n: u64) {
        self.values.borrow_mut().push((f.to_string(), n.to_string()));
    }

    fn is_some_val_in_record(&self) -> bool {
        !self.values.borrow().is_empty()
    }
}

