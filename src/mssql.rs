use odbc_api::{Environment, Connection};
use std::sync::Mutex;
use std::cell::RefCell;
use simple_error::SimpleError;

use crate::report::*;

pub struct ReportMSSQL<'env> {
    instance: String, // Connection string for the MSSQL instance
    database: String, // Database name
    table_name: String,
    report_suffix: Option<ReportSuffix>,
    environment: Environment, // Reference to the Environment object
    connection: Mutex<Option<Connection<'env>>>, // Mutex to manage the connection
    values: RefCell<Vec<(String /*field*/, String /*value*/)>>, // Stores the values to be inserted
}

impl<'env> ReportMSSQL<'env> {
    pub fn new(
        table_name: &str,
        instance: &str,
        database: &str,
        report_suffix: Option<ReportSuffix>,
    ) -> Result<Self, SimpleError> {

    // Create a new ODBC environment
    let environment = Environment::new().map_err(|e| SimpleError::new(format!("ODBC Error: {e}")))?;
        Ok(ReportMSSQL {
            instance: instance.to_string(),
            database: database.to_string(),
            table_name: table_name.to_string(),
            report_suffix: report_suffix,
            environment, // Store a reference to the Environment object
            connection: Mutex::new(None), // Connection will be established in `start_file`
            values: RefCell::new(Vec::new()),
        })
    }

    fn connect_to_database(&self) -> Result<(), SimpleError> {
        let connection_string = format!(
            "Driver={{ODBC Driver 17 for SQL Server}};Server={};Database={};Trusted_Connection=yes;",
            self.instance, self.database
        );

        // Create a connection and extend its lifetime to 'static
        let connection = unsafe {
            std::mem::transmute::<Connection, Connection<'static>>(
                self.environment
                    .connect_with_connection_string(&connection_string)
                    .map_err(|e| SimpleError::new(format!("Failed to connect to MSSQL instance: {e}")))?,
            )
        };

        // Store the connection in the Mutex
        *self.connection.lock().unwrap() = Some(connection);

        println!(
            "Successfully connected to MSSQL instance: {} and database: {}",
            self.instance, self.database
        );
        Ok(())
    }

    pub fn create_table(&self) -> Result<(), SimpleError> {
        let guard = self.connection.lock().unwrap(); // Get the MutexGuard
        let connection = guard
            .as_ref()
            .ok_or_else(|| SimpleError::new("No active database connection"))?;

        // Define the SQL query to create the table
        let query = match self.report_suffix{
            Some(ReportSuffix::FileReport) => {
                format!(
                    "CREATE TABLE [{}] (
                        [WorkId] BIGINT NULL,
                        [System_ComputerName] NVARCHAR(MAX) NULL,
                        [System_ItemPathDisplay] NVARCHAR(MAX) NULL,
                        [System_DateModified] DATETIME2 NULL,
                        [System_DateCreated] DATETIME2 NULL,
                        [System_DateAccessed] DATETIME2 NULL,
                        [System_Size] BIGINT NULL,
                        [System_FileOwner] NVARCHAR(MAX) NULL,
                        [System_Search_AutoSummary] NVARCHAR(MAX) NULL,
                        [System_Search_GatherTime] DATETIME2 NULL,
                        [System_ItemType] NVARCHAR(MAX) NULL
                    )",
                    self.table_name
                )
            }
            Some(ReportSuffix::ActivityHistory) => {
            format!(
                "CREATE TABLE [{}] (
                    [WorkId] BIGINT NULL,
                    [System_ComputerName] NVARCHAR(MAX) NULL,
                    [System_ItemPathDisplay] NVARCHAR(MAX) NULL,
                    [System_DateModified] DATETIME2 NULL,
                    [System_DateCreated] DATETIME2 NULL,
                    [System_DateAccessed] DATETIME2 NULL,
                    [System_Size] BIGINT NULL,
                    [System_FileOwner] NVARCHAR(MAX) NULL,
                    [System_Search_AutoSummary] NVARCHAR(MAX) NULL,
                    [System_Search_GatherTime] DATETIME2 NULL,
                    [System_ItemType] NVARCHAR(MAX) NULL
                )",
                self.table_name
            )
            }
            Some(ReportSuffix::InternetHistory) => {
                format!(
                "CREATE TABLE [{}] (
                    [WorkId] BIGINT NULL,
                    [System_ItemUrl] NVARCHAR(MAX) NULL,
                    [System_ItemDate] DATETIME2 NULL,
                    [System_Link_TargetUrl] NVARCHAR(MAX) NULL,
                    [System_Search_GatherTime] DATETIME2 NULL,
                    [System_Title] NVARCHAR(MAX) NULL,
                    [System_Link_DateVisited] DATETIME2 NULL
                )",
                self.table_name
            )
            }
            _ => {
                return Err(SimpleError::new("Invalid report suffix"));
            }
        };
        
   
        /*format!(
            "CREATE TABLE [{}] (
                [WorkId] BIGINT NULL,
                [System_ComputerName] NVARCHAR(MAX) NULL,
                [System_ItemPathDisplay] NVARCHAR(MAX) NULL,
                [System_DateModified] DATETIME NULL,
                [System_DateCreated] DATETIME NULL,
                [System_DateAccessed] DATETIME NULL,
                [System_Size] BIGINT NULL,
                [System_FileOwner] NVARCHAR(MAX) NULL,
                [System_Search_AutoSummary] NVARCHAR(MAX) NULL,
                [System_Search_GatherTime] DATETIME NULL,
                [System_ItemType] NVARCHAR(MAX) NULL
            )",
            self.table_name
        );*/

        // Execute the query
        connection
            .execute(&query, ())
            .map_err(|e| SimpleError::new(format!("Failed to create table: {e}")))?;
            
        println!("Executing SQL: {}", query);
        Ok(())
    }

    pub fn write_values_to_db(&self) -> Result<(), SimpleError> {
        let guard = self.connection.lock().unwrap(); // Get the MutexGuard
        let connection = guard
            .as_ref()
            .ok_or_else(|| SimpleError::new("No active database connection"))?;

        let values = self.values.borrow();

        // Prepare the column names and values
        let mut columns = Vec::new();
        let mut values_list = Vec::new();

        for (field, value) in values.iter() {
            columns.push(format!("[{}]", field)); // Add column name with brackets
            values_list.push(format!("'{}'", value.replace("'", "''"))); // Escape single quotes in values
        }

        // Combine columns and values into a single INSERT statement
        let query = format!(
            "INSERT INTO {} ({}) VALUES ({});",
            self.table_name,
            columns.join(", "),
            values_list.join(", ")
        );

        //println!("Executing query: {}", query);
        // Execute the query
        connection
            .execute(&query, ())
            .map_err(|e| SimpleError::new(format!("Failed to execute query: {e}")))?;

        

        Ok(())
    }

}

impl<'env> Report for ReportMSSQL<'env> {
    fn start_file(&mut self) {
        if let Err(e) = self.connect_to_database() {
            panic!("Failed to connect to database: {}", e);
        }

        if let Err(e) = self.create_table() {
            panic!("Failed to create table: {}", e);
        }
    }

    fn footer(&mut self) {
        self.create_new_row(true);
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

impl<'env> Drop for ReportMSSQL<'env> {
    fn drop(&mut self) {
        self.footer();
        // Lock the connection mutex
        let mut guard = self.connection.lock().unwrap();

        if let Some(connection) = guard.take() {
            // Commit the transaction
            if let Err(e) = connection.commit() {
                eprintln!("Failed to commit transaction: {}", e);
            } else {
                println!("Transaction committed successfully.");
            }

            // Closing the connection is handled automatically when the `Connection` object is dropped.
            println!("Connection to database '{}' closed.", self.database);
        } else {
            eprintln!("No active connection to close.");
        }
    }
}
