use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader};
#[cfg(test)]
use std::{
    env, fs,
    process::{Command, Stdio},
};

use camino::Utf8PathBuf as PathBuf;
use csv::{Reader, StringRecordIter};
use env_logger::{self, Target};
use function_name::named;
use log::info;
use simple_error::SimpleError;
use std::path::Path as StdPath;
use tempdir::TempDir;
use walkdir::{DirEntry, Error, WalkDir};

macro_rules! function_path {
    () => {
        concat!(module_path!(), "::", function_name!())
    };
}

fn get_dir<P: AsRef<StdPath>>(path: P, ext: &str) -> Vec<PathBuf> {
    fn get_filename(f: &Result<DirEntry, Error>) -> &str {
        f.as_ref().unwrap().file_name().to_str().unwrap()
    }

    WalkDir::new(path)
        .sort_by(|a, b| a.file_name().cmp(b.file_name()))
        .same_file_system(true)
        .into_iter()
        .filter_map(|ref f| {
            if get_filename(f).ends_with(ext) {
                Some(PathBuf::from(get_filename(f)))
            } else {
                None
            }
        })
        .collect()
}

fn full_path(path: &str, file: &str) -> PathBuf {
    PathBuf::from_iter([path, file].iter())
}

#[named]
fn do_invoke(cmd: &mut Command) {
    info!("{}", function_path!());
    let args: Vec<&str> = cmd.get_args().map(|a| a.to_str().unwrap()).collect();
    println!(
        "cmd '{} {}'",
        cmd.get_program().to_str().unwrap(),
        args.join(" ")
    );

    let mut child = cmd
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap_or_else(|_| panic!("'{cmd:?}' command failed to start"));

    if !child.wait().unwrap().success() {
        if let Some(stderr) = child.stderr {
            panic!("stderr: {stderr:?}");
        }
        panic!("Failed '{cmd:?}'");
    }
}

#[named]
fn generate_reports(reporter_bin: &str, db_path: &str, common_args: &Vec<&str>) {
    info!("{}", function_path!());
    let mut cmd = Command::new(reporter_bin);
    let cmd = cmd.args(common_args).args(["csv", db_path]);

    do_invoke(cmd);

    let mut cmd = Command::new(reporter_bin);
    let cmd = cmd.args(common_args).args(["json", db_path]);

    do_invoke(cmd);
}

fn do_generate(reporter_bin: &str, db_path: &str, rep_dir: &str, specific_args: &Vec<&str>) {
    let mut common_args = vec!["--outdir", rep_dir];
    common_args.extend(specific_args);
    common_args.push("--format");
    generate_reports(reporter_bin, db_path, &common_args);
}

fn error(filename: &PathBuf, msg: &str) -> Box<dyn std::error::Error> {
    let a = filename.as_str().replace(".csv", "").replace(".json", "");
    let i = a.rfind(|c: char| c.is_alphabetic()).unwrap();
    let s = &a[..(i + 1)];

    Box::new(SimpleError::new(format!("{s}: {msg}")))
}

fn do_compare_json(sidr_path: &str, ext_cfg_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    fn read_lines(filename: &str) -> io::Lines<BufReader<File>> {
        let file = File::open(filename).unwrap();
        BufReader::new(file).lines()
    }

    let dir_sidr = get_dir(sidr_path, ".json");
    let dir_ext_cfg = get_dir(ext_cfg_path, ".json");
    for (sidr, ext_cfg) in itertools::zip_eq(dir_sidr.iter(), dir_ext_cfg.iter()) {
        println!("{sidr} == {ext_cfg}");

        let sidr_lines = read_lines(full_path(sidr_path, sidr.as_str()).as_str()).count();
        let ext_lines = read_lines(full_path(ext_cfg_path, ext_cfg.as_str()).as_str()).count();

        if sidr_lines != ext_lines {
            return Err(error(
                sidr,
                &format!("sidr_lines {} != ext_lines {}", sidr_lines, ext_lines),
            ));
        }

        let mut errors = "".to_string();
        let sidr_lines = read_lines(full_path(sidr_path, sidr.as_str()).as_str());
        let ext_lines = read_lines(full_path(ext_cfg_path, ext_cfg.as_str()).as_str());

        itertools::zip_eq(sidr_lines, ext_lines).for_each(|(s_l, e_l)| {
            let s_i = json::parse(s_l.unwrap().as_str());
            let e_i = json::parse(e_l.unwrap().as_str());

            s_i.unwrap().entries().for_each(|(s_k, s_v)| {
                if let Some((e_k, e_v)) = e_i.as_ref().unwrap().entries().find(|(k, _)| *k == s_k) {
                    if s_v != e_v {
                        errors.push_str(&format!("{}={} not equal to {}={}\n", s_k, s_v, e_k, e_v));
                    }
                } else {
                    errors.push_str(&format!("could not find {}\n", s_k));
                }
            });
        });

        if !errors.is_empty() {
            return Err(error(sidr, &format!("\n{}", errors)));
        }
    }

    Ok(())
}

fn do_compare_csv(sidr_path: &str, ext_cfg_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let dir_sidr = get_dir(sidr_path, ".csv");
    let dir_ext_cfg = get_dir(ext_cfg_path, ".csv");

    for (sidr, ext_cfg) in dir_sidr.iter().zip(dir_ext_cfg.iter()) {
        println!("{sidr} == {ext_cfg}");

        let sidr = PathBuf::from_iter([sidr_path, sidr.as_str()].iter());
        let mut sidr_reader = Reader::from_path(&sidr)?;
        let ext_cfg = PathBuf::from_iter([ext_cfg_path, ext_cfg.as_str()].iter());
        let mut ext_cfg_reader = Reader::from_path(ext_cfg)?;
        let mut sidr_iter = sidr_reader.headers()?.iter();
        let mut ext_iter = ext_cfg_reader.headers()?.iter();

        compare_iters(&mut sidr_iter, &mut ext_iter, &sidr)?;

        let mut sidr_reader = sidr_reader.into_records();
        let mut ext_cfg_reader = ext_cfg_reader.into_records();

        loop {
            match (sidr_reader.next(), ext_cfg_reader.next()) {
                (None, None) => break,
                (Some(sid_rec), Some(ext_rec)) => {
                    let sid_rec = sid_rec?;
                    let mut sid_fld = sid_rec.iter();
                    let ext_rec = ext_rec?;
                    let mut ext_fld = ext_rec.iter();

                    compare_iters(&mut sid_fld, &mut ext_fld, &sidr)?
                }
                (Some(sid_rec), None) => {
                    let errors = format!("sidr has more records:\n{:?}", sid_rec);
                    return Err(error(&sidr, &errors));
                }
                (None, Some(ext_rec)) => {
                    let errors = format!("ext_cfg has more records:\n{:?}", ext_rec);
                    return Err(error(&sidr, &errors));
                }
            }
        }
    }

    Ok(())
}

fn compare_iters(
    sidr_iter: &mut StringRecordIter,
    ext_iter: &mut StringRecordIter,
    filename: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    if !itertools::equal(sidr_iter.clone(), ext_iter.clone()) {
        let mut errors = "".to_string();
        let mut i = 0;
        for (s, e) in sidr_iter.zip(ext_iter) {
            i += 1;
            if s != e {
                errors.push_str(&format!("{i}. '{s}' != '{e}'"));
            }
        }
        return Err(error(filename, &errors));
    }

    Ok(())
}

fn do_compare(sidr_path: &str, ext_cfg_path: &str) {
    let mut errors = Vec::<Box<dyn std::error::Error>>::new();

    if let Err(e) = do_compare_csv(sidr_path, ext_cfg_path) {
        errors.push(e);
    }
    if let Err(e) = do_compare_json(sidr_path, ext_cfg_path) {
        errors.push(e);
    }

    if !errors.is_empty() {
        for e in errors {
            println!("{e}");
        }
        panic!("failed");
    }
}

// #[test]
// #[named]
// fn compare_generated_reports() {
//     env_logger::builder().target(Target::Stderr).init();

//     info!("{}", function_path!());

//     let bin_root = PathBuf::from("target").join(if cfg!(debug_assertions) {
//         "debug"
//     } else {
//         "release"
//     });
//     let sidr_bin = bin_root.join("sidr");
//     let ext_cfg_bin = bin_root.join("external_cfg");
//     let db_path = env::var("WSA_TEST_DB_PATH").unwrap_or("tests/testdata".to_string());
//     let cfg_path = env::var("WSA_TEST_CONFIGURATION_PATH")
//         .unwrap_or("src/bin/test_reports_cfg.yaml".to_string());
//     let work_dir_name = format!("{}_testing", ext_cfg_bin.file_name().unwrap());
//     let work_temp_dir = TempDir::new(work_dir_name.as_str()).expect("{work_dir_name} creation");
//     let _work_dir_keeper;
//     let work_dir = if env::var("KEEP_TEMP_WORK_DIR").is_ok() {
//         _work_dir_keeper = work_temp_dir.into_path();
//         _work_dir_keeper.as_path()
//     } else {
//         work_temp_dir.path()
//     };
//     let sidr_dir = PathBuf::from_path_buf(work_dir.join("sidr")).unwrap();
//     let ext_cfg_dir: PathBuf = PathBuf::from_path_buf(work_dir.join("ext_cfg")).unwrap();

//     info!("db_path: {db_path}");
//     info!("cfg_path: {cfg_path}");
//     info!("sidr_dir: {sidr_dir}");
//     info!("ext_cfg_dir: {ext_cfg_dir}");

//     fs::create_dir(&sidr_dir).unwrap_or_else(|_| panic!("could not create '{}'", sidr_dir));
//     fs::create_dir(&ext_cfg_dir).unwrap_or_else(|_| panic!("could not create '{}'", ext_cfg_dir));

//     do_generate(
//         sidr_bin.as_str(),
//         db_path.as_str(),
//         sidr_dir.as_str(),
//         &vec![],
//     );
//     do_generate(
//         ext_cfg_bin.as_str(),
//         db_path.as_str(),
//         ext_cfg_dir.as_str(),
//         &vec!["--cfg-path", &cfg_path],
//     );

//     do_compare(sidr_dir.as_str(), ext_cfg_dir.as_str());
// }
