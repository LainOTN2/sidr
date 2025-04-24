use chrono::prelude::*;
use clap::ValueEnum;
use ese_parser_lib::parser::jet::DbState;
use serde_json;
use simple_error::SimpleError;
use std::cell::{Cell, RefCell};
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::ops::IndexMut;
use std::path::{Path, PathBuf};

use crate::utils::*;
use crate::mssql::*;

#[derive(Clone, Debug, ValueEnum)]
pub enum ReportFormat {
    Json,
    Csv,
    NoFormat,
}

#[derive(Clone, Copy, Debug, PartialEq, ValueEnum)]
pub enum ReportOutput {
    ToFile,
    ToStdout,
    ToDatabase,
}

#[derive(Debug, PartialEq)]
pub enum ReportSuffix {
    FileReport,
    ActivityHistory,
    InternetHistory,
    Unknown,
}

impl ReportSuffix {
    pub fn get_match(output_type: &str) -> Option<ReportSuffix> {
        match output_type {
            "File_Report" => Some(ReportSuffix::FileReport),
            "Activity_History_Report" => Some(ReportSuffix::ActivityHistory),
            "Internet_History_Report" => Some(ReportSuffix::InternetHistory),
            &_ => Some(ReportSuffix::Unknown),
        }
    }

    // Autogenerating the names from the enum values by deriving Debug is another option.
    // However, if someone decided to change the name of one of these enums,
    // it could break downstream processing.
    pub fn message(&self) -> String {
        match self {
            Self::FileReport => serde_json::to_string("file_report").unwrap(),
            Self::ActivityHistory => serde_json::to_string("activity_history").unwrap(),
            Self::InternetHistory => serde_json::to_string("internet_history").unwrap(),
            Self::Unknown => serde_json::to_string("").unwrap(),
        }
    }
}

impl Display for ReportSuffix {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.message())
    }
}

pub struct ReportProducer {
    dir: PathBuf,
    format: ReportFormat,
    report_type: ReportOutput,
    instance: Option<String>,
    database: Option<String>,
}

impl ReportProducer {
    pub fn new(dir: &Path, format: ReportFormat, report_type: ReportOutput, instance: Option<String>, database: Option<String>) -> Self {
        if !dir.exists() {
            std::fs::create_dir(dir)
                .unwrap_or_else(|_| panic!("Can't create directory \"{}\"", dir.to_string_lossy()));
        }
        ReportProducer {
            dir: dir.to_path_buf(),
            format,
            report_type,
            instance: instance,
            database: database,
        }
    }

    pub fn get_report_type(&self) -> ReportOutput {
        self.report_type
    }

    pub fn is_db_dirty(&self, db_state: Option<DbState>) -> bool {
        match db_state {
            Some(state) => state != DbState::CleanShutdown,
            None => false,
        }
    }

    pub fn get_path_db_status(
        &self,
        recovered_hostname: &str,
        report_suffix: &str,
        date_time_now: DateTime<Utc>,
        ext: &str,
        edb_database_state: Option<DbState>,
    ) -> PathBuf {
        let status = if self.is_db_dirty(edb_database_state) {
            "_dirty"
        } else {
            ""
        };
        self.dir.join(format!(
            "{}_{}_{}{}.{}",
            recovered_hostname,
            report_suffix,
            date_time_now.format("%Y%m%d_%H%M%S%.f"),
            status,
            ext
        ))
    }

    pub fn new_report(
        &self,
        _dbpath: &Path,
        recovered_hostname: &str,
        report_suffix: &str,
        edb_database_state: Option<DbState>,
    ) -> Result<(PathBuf, Box<dyn Report>), SimpleError> {
        let ext = match self.format {
            ReportFormat::Json => "json",
            ReportFormat::Csv => "csv",
            ReportFormat::NoFormat => "",
        };
        let date_time_now: DateTime<Utc> = Utc::now();
        let mut path = self.get_path_db_status(
            recovered_hostname,
            report_suffix,
            date_time_now,
            ext,
            edb_database_state,
        );
        let table_name = format!("{}_{}_{}", recovered_hostname, report_suffix,date_time_now.format("%Y%m%d_%H%M%S"));
        let report_suffix = ReportSuffix::get_match(report_suffix);
       
        let rep: Box<dyn Report> = if ReportOutput::ToDatabase == self.report_type {
            path = PathBuf::new();
            ReportMSSQL::new(
                        table_name.as_str(),
                        self.instance.as_ref().unwrap(),
                        self.database.as_ref().unwrap(),
                        report_suffix,
                    )
                    .map(Box::new)?
        } else {
            match self.format{
                ReportFormat::Json => {
                    ReportJson::new(&path, self.report_type, report_suffix).map(Box::new)?
                }
                ReportFormat::Csv => {
                    ReportCsv::new(&path, self.report_type, report_suffix).map(Box::new)?
                }
                ReportFormat::NoFormat => {
                    return Err(SimpleError::new("NoFormat is not supported"));
                }
            }
        };
        /*
        let rep: Box<dyn Report> = match self.format {
            ReportFormat::Json => {
                ReportJson::new(&path, self.report_type, report_suffix).map(Box::new)?
            }
            ReportFormat::Csv => {
                ReportCsv::new(&path, self.report_type, report_suffix).map(Box::new)?
            }
            ReportFormat::NoFormat => {
                if ReportOutput::ToDatabase == self.report_type {
                    ReportMSSQL::new(
                        table_name.as_str(),
                        self.instance.as_ref().unwrap(),
                        self.database.as_ref().unwrap(),
                    )
                    .map(Box::new)?
                } else {
                    return Err(SimpleError::new("NoFormat is not supported"));
                }

            }
        };*/
        Ok((path, rep))
    }
}

pub trait Report {
    fn footer(&mut self) {}
    fn create_new_row(&mut self, f: bool);
    fn insert_str_val(&self, f: &str, s: String);
    fn insert_int_val(&self, f: &str, n: u64);
    fn set_field(&self, _: &str) {} // used in csv to generate header
    fn start_file(&mut self) {}
    fn end_file(&mut self) {}
    fn is_some_val_in_record(&self) -> bool;
}

// report json
pub struct ReportJson {
    f: Box<dyn Write + 'static>,
    report_output: ReportOutput,
    report_suffix: Option<ReportSuffix>,
    values: RefCell<Vec<String>>,
}

impl ReportJson {
    pub fn new(
        path: &Path,
        report_output: ReportOutput,
        report_suffix: Option<ReportSuffix>,
    ) -> Result<Self, SimpleError> {
        let mut report = match report_output {
                                ReportOutput::ToFile => {
                                    let output: Box<dyn Write> =
                                        Box::new(File::create(path).map_err(|e| SimpleError::new(format!("{e}")))?);
                                    ReportJson {
                                        f: output,
                                        report_output,
                                        report_suffix: None,
                                        values: RefCell::new(Vec::new()),
                                    }
                                },
                                ReportOutput::ToStdout => ReportJson {
                                    f: Box::new(BufWriter::new(io::stdout())),
                                    report_output,
                                    report_suffix,
                                    values: RefCell::new(Vec::new()),
                                },
                                ReportOutput::ToDatabase => Err(SimpleError::new(
                                    "ReportOutput::ToDatabase is not supported for JSON format",
                                ))?,
         };

         report.start_file();
         Ok(report)
    }

    fn start_file(&mut self)
    {
        let handle = self.f.as_mut();
        handle.write_all(b"[\n").unwrap();
        handle.flush().unwrap();
    }

    fn end_file(&mut self)
    {
        let handle = self.f.as_mut();
        handle.write_all(b"]").unwrap();
        handle.flush().unwrap();
    }

    fn escape(s: String) -> String {
        json_escape(&s)
    }

    pub fn write_values(&mut self, f: bool) {
        let mut values = self.values.borrow_mut();
        let len = values.len();
        let handle = self.f.as_mut();
        if len > 0 {
            handle.write_all(b"{").unwrap();
        }
        if self.report_output == ReportOutput::ToStdout {
            handle
                .write_all(
                    format!(
                        "{}:{},",
                        serde_json::to_string("report_suffix").unwrap(),
                        self.report_suffix.as_ref().unwrap()
                    )
                    .as_bytes(),
                )
                .ok();
        }
        for i in 0..len {
            let v = values.index_mut(i);
            if !v.is_empty() {
                let last = if i == len - 1 { "" } else { "," };
                handle.write_all(format!("{v}{last}").as_bytes()).unwrap();
            }
        }
        if len > 0 {
            if f{
                handle.write_all(b"}\n").unwrap();
            }
            else {
                handle.write_all(b"},\n").unwrap();
            }
            values.clear();
        }
        handle.flush().unwrap();
    }
}

impl Report for ReportJson {
    fn footer(&mut self) {
        self.create_new_row(true);
    }

    fn create_new_row(&mut self, f: bool) {
        if !self.values.borrow().is_empty() {
            self.write_values(f);
        }
    }

    fn insert_str_val(&self, f: &str, s: String) {
        self.values
            .borrow_mut()
            .push(format!("\"{}\":{}", f, ReportJson::escape(s)));
    }

    fn insert_int_val(&self, f: &str, n: u64) {
        self.values.borrow_mut().push(format!("\"{f}\":{n}"));
    }

    fn is_some_val_in_record(&self) -> bool {
        !self.values.borrow().is_empty()
    }

    fn start_file(&mut self) {
        self.start_file();
    }

    fn end_file(&mut self) {
        self.end_file();
    }
}

impl Drop for ReportJson {
    fn drop(&mut self) {
        self.footer();
        self.end_file();
    }
}

// report csv
pub struct ReportCsv {
    f: Box<dyn Write + 'static>,
    report_output: ReportOutput,
    report_suffix: Option<ReportSuffix>,
    first_record: Cell<bool>,
    values: RefCell<Vec<(String /*field*/, String /*value*/)>>,
}

impl ReportCsv {
    pub fn new(
        f: &Path,
        report_output: ReportOutput,
        report_suffix: Option<ReportSuffix>,
    ) -> Result<Self, SimpleError> {
        let mut report = match report_output {
                        ReportOutput::ToFile => {
                            let output: Box<dyn Write> =
                                Box::new(File::create(f).map_err(|e| SimpleError::new(format!("{e}")))?);
                            ReportCsv {
                                f: output,
                                report_output,
                                report_suffix: None,
                                first_record: Cell::new(true),
                                values: RefCell::new(Vec::new()),
                            }
                        }
                        ReportOutput::ToStdout => ReportCsv {
                            f: Box::new(BufWriter::new(io::stdout())),
                            report_output,
                            report_suffix,
                            first_record: Cell::new(true),
                            values: RefCell::new(Vec::new()),
                        },
                        ReportOutput::ToDatabase => Err(SimpleError::new(
                            "ReportOutput::ToDatabase is not supported for CSV format",
                        ))?,
        };

        report.start_file();

        Ok(report)
    }

    fn escape(s: String) -> String {
        s.replace('\"', "\"\"")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
    }

    pub fn write_header(&mut self) {
        let handle = self.f.as_mut();
        if self.report_output == ReportOutput::ToStdout {
            handle.write_all(b"\nReportSuffix,").ok();
        }
        let values = self.values.borrow();
        for i in 0..values.len() {
            let v = &values[i];
            if i == values.len() - 1 {
                handle.write_all(v.0.as_bytes()).unwrap();
            } else {
                handle.write_all(format!("{},", v.0).as_bytes()).unwrap();
            }
        }
    }

    pub fn write_values(&mut self) {
        let handle = self.f.as_mut();
        handle.write_all(b"\n").unwrap();

        let mut values = self.values.borrow_mut();
        let len = values.len();
        if self.report_output == ReportOutput::ToStdout {
            handle
                .write_all(format!("{},", self.report_suffix.as_ref().unwrap()).as_bytes())
                .ok();
        }
        for i in 0..len {
            let v = values.index_mut(i);
            let last = if i == len - 1 { "" } else { "," };
            if v.1.is_empty() {
                handle.write_all(last.to_string().as_bytes()).unwrap();
            } else {
                handle
                    .write_all(format!("{}{}", v.1, last).as_bytes())
                    .unwrap();
                v.1.clear();
            }
        }
        handle.flush().unwrap();
    }

    pub fn update_field_with_value(&self, f: &str, v: String) {
        let mut values = self.values.borrow_mut();
        if let Some(found) = values.iter_mut().find(|i| i.0 == f) {
            found.1 = v;
        } else {
            values.push((f.into(), v));
        }
    }
}

impl Report for ReportCsv {
    fn footer(&mut self) {
        self.create_new_row(true);
        self.end_file();
    }

    fn create_new_row(&mut self, _f: bool) {
        // at least 1 value was recorded?
        if self.is_some_val_in_record() {
            if self.first_record.get() {
                self.write_header();
                self.first_record.set(false);
            }
            self.write_values();
        }
    }

    fn insert_str_val(&self, f: &str, s: String) {
        self.update_field_with_value(f, format!("\"{}\"", ReportCsv::escape(s)));
    }

    fn insert_int_val(&self, f: &str, n: u64) {
        self.update_field_with_value(f, n.to_string());
    }

    fn set_field(&self, f: &str) {
        // set field with empty value to record field name
        self.update_field_with_value(f, "".to_string());
    }

    fn is_some_val_in_record(&self) -> bool {
        self.values.borrow().iter().any(|i| !i.1.is_empty())
    }
}

impl Drop for ReportCsv {
    fn drop(&mut self) {
        self.footer();
    }
}

#[cfg(test)]
mod tests {
    use crate::report::{
        Report, ReportCsv, ReportFormat, ReportJson, ReportOutput, ReportProducer, ReportSuffix,
    };
    use chrono::{DateTime, NaiveDate, Utc};
    use ese_parser_lib::parser::jet::DbState;
    use std::path::Path;

    #[test]
    pub fn test_report_csv() {
        let p = Path::new("test.csv");
        let report_type = ReportOutput::ToFile;
        let report_suffix = None;
        {
            let mut r = ReportCsv::new(p, report_type, report_suffix).unwrap();
            r.set_field("int_field");
            r.set_field("str_field");
            r.insert_int_val("int_field", 0);
            r.insert_str_val("str_field", "string0".into());
            for i in 1..10 {
                r.create_new_row(false);
                if i % 2 == 0 {
                    r.insert_str_val("str_field", format!("string{}", i));
                } else {
                    r.insert_int_val("int_field", i);
                }
            }
        }
        let data = std::fs::read_to_string(p).unwrap();
        let expected = r#"int_field,str_field
0,"string0"
1,
,"string2"
3,
,"string4"
5,
,"string6"
7,
,"string8"
9,"#;
        assert_eq!(data, expected);
        std::fs::remove_file(p).unwrap();
    }

    #[test]
    pub fn test_report_jsonl() {
        let p = Path::new("test.json");
        let report_type = ReportOutput::ToFile;
        let report_suffix = Some(ReportSuffix::FileReport);
        {
            let mut r = ReportJson::new(p, report_type, report_suffix).unwrap();
            r.insert_int_val("int_field", 0);
            r.insert_str_val("str_field", "string0_with_escapes_here1\"here2\\".into());
            for i in 1..10 {
                r.create_new_row(false);
                if i % 2 == 0 {
                    r.insert_str_val("str_field", format!("string{}", i));
                } else {
                    r.insert_int_val("int_field", i);
                }
            }
        }
        let data = std::fs::read_to_string(p).unwrap();
        let expected = r#"{"int_field":0,"str_field":"string0_with_escapes_here1\"here2\\"},
{"int_field":1},
{"str_field":"string2"},
{"int_field":3},
{"str_field":"string4"},
{"int_field":5},
{"str_field":"string6"},
{"int_field":7},
{"str_field":"string8"},
{"int_field":9}
]"#;
        assert_eq!(data, expected);
        std::fs::remove_file(p).unwrap();
    }

    #[test]
    fn test_report_suffix() {
        let report_suffix = Some(ReportSuffix::FileReport);
        assert_eq!(ReportSuffix::get_match("File_Report"), report_suffix);
        assert_ne!(ReportSuffix::get_match("Activity"), report_suffix);

        assert_eq!(
            ReportSuffix::message(report_suffix.as_ref().unwrap()),
            serde_json::to_string("file_report").unwrap()
        );
        assert_eq!(
            ReportSuffix::message(&ReportSuffix::ActivityHistory),
            serde_json::to_string("activity_history").unwrap()
        );
        assert_eq!(
            ReportSuffix::message(&ReportSuffix::InternetHistory),
            serde_json::to_string("internet_history").unwrap()
        );
        assert_eq!(
            ReportSuffix::message(&ReportSuffix::Unknown),
            serde_json::to_string("").unwrap()
        );
    }

    #[test]
    fn test_get_path_db_status() {
        let path = Path::new("./tests");
        let rp = ReportProducer::new(path, ReportFormat::Json, ReportOutput::ToStdout);
        let naivedatetime_utc = NaiveDate::from_ymd_opt(2000, 1, 12)
            .unwrap()
            .and_hms_opt(2, 0, 0)
            .unwrap();
        let dt = DateTime::<Utc>::from_utc(naivedatetime_utc, Utc);
        assert_eq!(
            rp.get_path_db_status(
                "test_hostname",
                "activity",
                dt,
                "edb.test",
                Some(DbState::CleanShutdown)
            )
            .to_string_lossy(),
            Path::new("./tests")
                .join("test_hostname_activity_20000112_020000.edb.test")
                .to_string_lossy()
        );
        assert_eq!(
            rp.get_path_db_status(
                "test_hostname",
                "activity",
                dt,
                "edb.test",
                Some(DbState::DirtyShutdown)
            )
            .to_string_lossy(),
            Path::new("./tests")
                .join("test_hostname_activity_20000112_020000_dirty.edb.test")
                .to_string_lossy()
        );
    }

    #[test]
    fn test_is_db_dirty() {
        let path = Path::new("./tests");
        let rp = ReportProducer::new(path, ReportFormat::Json, ReportOutput::ToStdout);
        assert_eq!(rp.is_db_dirty(Some(DbState::CleanShutdown)), false);
        assert_eq!(rp.is_db_dirty(Some(DbState::DirtyShutdown)), true);
        assert_eq!(rp.is_db_dirty(Some(DbState::BeingConverted)), true);
    }
}
