use clap::Parser;
use env_logger::{self, Target};
use ese_parser_lib::parser::jet::DbState;

use std::path::PathBuf;
use walkdir::WalkDir;
use wsa_lib::report::{ReportFormat, ReportOutput};
use wsa_lib::{do_reports, ReportsCfg};

#[derive(Parser)]
struct Cli {
    /// Path to <config.yaml>
    #[arg(short, long)]
    cfg_path: String,

    /// Path to input directory (which will be recursively scanned for Windows.edb and Windows.db).
    input: String,

    /// Output format: json (default) or csv
    #[arg(short, long, value_enum, default_value_t = ReportFormat::Json)]
    format: ReportFormat,

    /// Report type: file or stdout
    #[arg(short, long, value_enum, default_value_t = ReportOutput::ToFile)]
    report_type: ReportOutput,

    /// Path to the directory where reports will be created (will be created if not present). Default is the current directory.
    #[arg(short, long, value_name = "OUTPUT DIRECTORY")]
    outdir: Option<PathBuf>,
}

fn do_sql_report(db_path: &str, cfg: &ReportsCfg) {
    let mut sql_reader = wsa_lib::SqlReader::new(db_path);
    do_reports(cfg, &mut sql_reader);
}

fn do_edb_report(db_path: &str, cfg: &ReportsCfg) {
    let mut edb_reader = wsa_lib::EseReader::new(db_path, &cfg.table_edb);

    if edb_reader.jdb.get_database_state() != DbState::CleanShutdown {
        eprintln!("WARNING: The database state is not clean.");
        eprintln!("Please use EseUtil which helps check the status (/MH) of a database and perform a soft (/R) or hard (/P) recovery");
        eprintln!("or system32/esentutl for repair (/p).");
        eprintln!("Results could be inaccurate and unstable work (even crash) is possible.\n");
    }

    do_reports(cfg, &mut edb_reader);
}

fn main() {
    env_logger::builder()
        .format_timestamp(None)
        .target(Target::Stderr)
        .init();

    let cli = Cli::parse();
    let s = std::fs::read_to_string(&cli.cfg_path).unwrap();
    let mut cfg: ReportsCfg = match serde_yaml::from_str(s.as_str()) {
        Ok(cfg) => cfg,
        Err(_e) => panic!("Bad config '{}': {_e}", cli.cfg_path),
    };

    if let Some(output_dir) = &cli.outdir {
        cfg.output_dir = output_dir.to_str().unwrap().to_string();
    }

    cfg.output_format = match cli.format {
        ReportFormat::Json => wsa_lib::OutputFormat::Json,
        ReportFormat::Csv => wsa_lib::OutputFormat::Csv,
    };

    static DB_NAMES: [&str; 2] = ["Windows.edb", "Windows.db"];

    for entry in WalkDir::new(&cli.input)
        .into_iter()
        .filter_entry(|e| {
            e.file_type().is_dir() || DB_NAMES.contains(&e.file_name().to_str().unwrap())
        })
        .flatten()
    {
        if !entry.file_type().is_dir() {
            let db_path = entry.path().to_str().unwrap().to_string();

            println!("{db_path}");
            if db_path.ends_with(".edb") {
                do_edb_report(&db_path, &cfg);
            } else if db_path.ends_with(".db") {
                do_sql_report(&db_path, &cfg);
            }
        }
    }
}
