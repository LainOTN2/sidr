use odbc_api::{Environment, Connection};
use std::sync::Mutex;
use std::cell::RefCell;
use std::cell::Cell;
use simple_error::SimpleError;
use regex::Regex;


use crate::report::*;

pub struct ReportMSSQL<'env> {
    instance: String, // Connection string for the MSSQL instance
    database: String, // Database name
    table_name: String,
    report_suffix: Option<ReportSuffix>,
    environment: Environment, // Reference to the Environment object
    connection: Mutex<Option<Connection<'env>>>, // Mutex to manage the connection
    values: RefCell<Vec<(String /*field*/, String /*value*/)>>, // Stores the values to be inserted
    query_builder: RefCell<String>, // Accumulates the INSERT statements
    row_counter: Cell<usize>, // Tracks the number of rows since the last flush
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
            query_builder: RefCell::new(String::new()),
            row_counter: Cell::new(0),
        })
    }

     pub fn get_latest_odbc_driver() -> Result<String, SimpleError> {
        // Create a new ODBC environment
        let environment = Environment::new().map_err(|e| SimpleError::new(format!("ODBC Error: {e}")))?;

        // Regex to match ODBC driver names and extract the version number
        let driver_regex = Regex::new(r"ODBC Driver (\d+) for SQL Server")
            .map_err(|e| SimpleError::new(format!("Failed to compile regex: {e}")))?;

        // Find the latest driver version
        let mut latest_version = 0;
        let mut latest_driver = None;
        
       for driver_info in environment
            .drivers()
            .map_err(|e| SimpleError::new(format!("Failed to retrieve ODBC drivers: {e}")))? {
            let driver_name = driver_info.description; // Use the name() method to get the driver name
            if let Some(captures) = driver_regex.captures(&driver_name) {
                if let Some(version_match) = captures.get(1) {
                    if let Ok(version) = version_match.as_str().parse::<u32>() {
                        if version > latest_version {
                            latest_version = version;
                            latest_driver = Some(driver_name.to_string());
                        }
                    }
                }
            }
        }

        // Return the latest driver or print an error if none is found
        if let Some(driver) = latest_driver {
            println!("Found latest ODBC driver: {}", driver);
            Ok(format!("Driver={{{}}};", driver))
        } else {
            eprintln!(
                "No compatible ODBC Driver for SQL Server found. Please download the latest driver from: \
                https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server"
            );
            Err(SimpleError::new("No compatible ODBC Driver for SQL Server found"))
        }
    }
    fn connect_to_database(&self) -> Result<(), SimpleError> {

        let driver = Self::get_latest_odbc_driver()?;
        let connection_string = format!(
            "{}Server={};Database={};Trusted_Connection=yes;TrustServerCertificate=yes;",
            driver, self.instance, self.database
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
        let values = self.values.borrow();

        // Define the column order based on the report suffix
        let column_order = match self.report_suffix {
            Some(ReportSuffix::FileReport) => vec![
                "WorkId",
                "System_ComputerName",
                "System_ItemPathDisplay",
                "System_DateModified",
                "System_DateCreated",
                "System_DateAccessed",
                "System_Size",
                "System_FileOwner",
                "System_Search_AutoSummary",
                "System_Search_GatherTime",
                "System_ItemType",
            ],
            Some(ReportSuffix::ActivityHistory) => vec![
                "WorkId",
                "System_ComputerName",
                "System_ItemPathDisplay",
                "System_DateModified",
                "System_DateCreated",
                "System_DateAccessed",
                "System_Size",
                "System_FileOwner",
                "System_Search_AutoSummary",
                "System_Search_GatherTime",
                "System_ItemType",
            ],
            Some(ReportSuffix::InternetHistory) => vec![
                "WorkId",
                "System_ItemUrl",
                "System_ItemDate",
                "System_Link_TargetUrl",
                "System_Search_GatherTime",
                "System_Title",
                "System_Link_DateVisited",
            ],
            _ => return Err(SimpleError::new("Invalid report suffix")),
        };

        // Map the provided values to their corresponding columns
        let mut column_values = vec!["NULL".to_string(); column_order.len()];
        for (field, value) in values.iter() {
            if let Some(index) = column_order.iter().position(|&col| col == field) {
                column_values[index] = format!("'{}'", value.replace("'", "''")); // Escape single quotes
            }
        }

        // Combine the column values into a single VALUES clause
        let values_clause = format!("({})", column_values.join(", "));

        // Append the VALUES clause to the query builder
        self.query_builder.borrow_mut().push_str(&values_clause);
        self.query_builder.borrow_mut().push_str(", "); // Add a comma for the next VALUES clause

        // Increment the row counter
        let current_count = self.row_counter.get() + 1;
        self.row_counter.set(current_count);

        // Flush the query builder if the row counter reaches 25
        if current_count >= 25 {
            self.flush_query_builder()?;
            self.row_counter.set(0); // Reset the counter
        }

        Ok(())
    }

    pub fn flush_query_builder(&self) -> Result<(), SimpleError> {
        let mut query_builder = self.query_builder.borrow_mut();

        // Remove the trailing comma and space
        if query_builder.ends_with(", ") {
            let len = query_builder.len(); // Get the length first
            query_builder.truncate(len - 2); // Then truncate
        }

        // Execute the accumulated query
        if !query_builder.is_empty() {
            // Define the column list based on the report suffix
            let column_list = match self.report_suffix {
                Some(ReportSuffix::FileReport) => vec![
                    "[WorkId]",
                    "[System_ComputerName]",
                    "[System_ItemPathDisplay]",
                    "[System_DateModified]",
                    "[System_DateCreated]",
                    "[System_DateAccessed]",
                    "[System_Size]",
                    "[System_FileOwner]",
                    "[System_Search_AutoSummary]",
                    "[System_Search_GatherTime]",
                    "[System_ItemType]",
                ],
                Some(ReportSuffix::ActivityHistory) => vec![
                    "[WorkId]",
                    "[System_ComputerName]",
                    "[System_ItemPathDisplay]",
                    "[System_DateModified]",
                    "[System_DateCreated]",
                    "[System_DateAccessed]",
                    "[System_Size]",
                    "[System_FileOwner]",
                    "[System_Search_AutoSummary]",
                    "[System_Search_GatherTime]",
                    "[System_ItemType]",
                ],
                Some(ReportSuffix::InternetHistory) => vec![
                    "[WorkId]",
                    "[System_ItemUrl]",
                    "[System_ItemDate]",
                    "[System_Link_TargetUrl]",
                    "[System_Search_GatherTime]",
                    "[System_Title]",
                    "[System_Link_DateVisited]",
                ],
                _ => return Err(SimpleError::new("Invalid report suffix")),
            };

            // Construct the full query
            let query = format!(
                "INSERT INTO [{}] ({}) VALUES {}",
                self.table_name,
                column_list.join(", "),
                query_builder
            );

            //println!("Executing batch query: {}", query);

            let guard = self.connection.lock().unwrap(); // Get the MutexGuard
            let connection = guard
                .as_ref()
                .ok_or_else(|| SimpleError::new("No active database connection"))?;
            connection
                .execute(&query, ())
                .map_err(|e| SimpleError::new(format!("Failed to execute query: {e}")))?;

            // Clear the query builder
            query_builder.clear();
        }

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
        // Perform a final flush
        if let Err(e) = self.flush_query_builder() {
            eprintln!("Failed to flush query builder during drop: {}", e);
        }

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
