/***************************************************************************
 * Module uses clap crate to read command line arguments. These include 
 * possible A, S, T and C flags, and possible strings for the data folder and 
 * source file name. If no flags 'S' (= import data) is returned by default.
 * Folder and file names return an empty string ("") rather than null if not 
 * present. 
 ***************************************************************************/

use clap::{command, Arg, ArgMatches};
use crate::error_defs::AppError;
use std::ffi::OsString;
use std::path::PathBuf;

#[derive(Debug)]
pub struct CliPars {
    pub data_folder: PathBuf,
    pub source_file: String,
    pub data_version: String,
    pub data_date: String,
    pub import_ror: bool,
    pub process_data: bool,
    pub report_data: bool,
    pub create_lup: bool,
    pub create_smm: bool,
    pub test_run: bool,
}

pub fn fetch_valid_arguments(args: Vec<OsString>) -> Result<CliPars, AppError>
{ 
    let parse_result = parse_args(args)?;

    // These parameters guaranteed to unwrap OK as all have a default value of "".

    let data_folder_as_string = parse_result.get_one::<String>("data_folder").unwrap();
    let data_folder = PathBuf::from(data_folder_as_string.replace("\\", "/"));

    let source_file = parse_result.get_one::<String>("src_file").unwrap();
    let data_version = parse_result.get_one::<String>("data_version").unwrap();
    let data_date = parse_result.get_one::<String>("data_date").unwrap();

    // Flag values are false if not present, true if present.

    let a_flag = parse_result.get_flag("a_flag");
    let i_flag = parse_result.get_flag("i_flag");

    let r_flag = parse_result.get_flag("r_flag");
    let p_flag = parse_result.get_flag("p_flag");
    let x_flag = parse_result.get_flag("x_flag");
    let c_flag = parse_result.get_flag("c_flag");
    let m_flag = parse_result.get_flag("m_flag");
    let t_flag = parse_result.get_flag("t_flag");

    let mut import = true;
    let mut process = false;
    let mut report_data = false;

    if a_flag == true  // 'a' (do all) flag set
    {
        process = true;  // import already true
        report_data = true;
    }
    else 
    {
        // none, one, two or all r, p and t flags set
        // if none set, use initial default values,
        // otherwise use values as provided

        if !(r_flag == false && p_flag == false && x_flag == false) {
            import = r_flag;
            process = p_flag;
            report_data = x_flag;
        }
    }

    // If both c and m flags set (may be by using 'i' (initialise) flag)
    {
        if i_flag || (c_flag && m_flag) {
            Ok(CliPars {
                data_folder: PathBuf::new(),
                source_file: "".to_string(),
                data_version: "".to_string(),
                data_date: "".to_string(),
                import_ror: false,
                process_data: false,
                report_data: false,
                create_lup: true,
                create_smm: true,
                test_run: false,
            })
        }
    
        else {
            Ok(CliPars {
                data_folder: data_folder.clone(),
                source_file: source_file.clone(),
                data_version: data_version.clone(),
                data_date: data_date.clone(),
                import_ror: import,
                process_data: process,
                report_data: report_data,
                create_lup: c_flag,
                create_smm: m_flag,
                test_run: t_flag,
            })
        }
    }
}


fn parse_args(args: Vec<OsString>) -> Result<ArgMatches, clap::Error> {

    command!()
        .about("Imports data from ROR json file (v2) and imports it into a database")
        .arg(
            Arg::new("data_folder")
           .short('f')
           .long("folder")
           .visible_short_aliases(['F'])
           .visible_aliases(["data folder"])
           .help("A string with the data folder path (over-rides environment setting")
           .default_value("")
       )
        .arg(
             Arg::new("src_file")
            .short('s')
            .long("source")
            .visible_short_aliases(['S'])
            .visible_aliases(["source file"])
            .help("A string with the source file name (over-rides environment setting")
            .default_value("")
        )
        .arg(
            Arg::new("data_version")
           .short('v')
           .long("data_version")
           .required(false)
           .help("A string with the version ascribed to the data by ror, in a semver format")
           .default_value("")
        )
        .arg(
            Arg::new("data_date")
           .short('d')
           .long("date")
           .visible_short_aliases(['D'])
           .required(false)
           .help("A string with a date in ISO format that gives the date of the data")
           .default_value("")
        )
        .arg(
            Arg::new("a_flag")
           .short('a')
           .long("all")
           .visible_short_aliases(['A'])
           .required(false)
           .help("A flag signifying run the entire program, equivalent to R and P")
           .action(clap::ArgAction::SetTrue)
         )
        .arg(
            Arg::new("r_flag")
           .short('r')
           .long("import")
           .visible_short_aliases(['R'])
           .required(false)
           .help("A flag signifying import from ror file to ror schema tables only")
           .action(clap::ArgAction::SetTrue)
       )
        .arg(
             Arg::new("p_flag")
            .short('p')
            .long("process")
            .visible_short_aliases(['P'])
            .required(false)
            .help("A flag signifying process ror data to src data and analyse and store results")
            .action(clap::ArgAction::SetTrue)
        )
        .arg(
            Arg::new("x_flag")
           .short('x')
           .long("export")
           .visible_short_aliases(['X'])
           .required(false)
           .help("A flag signifying output a summary of the current data into a text file")
           .action(clap::ArgAction::SetTrue)
       )
       .arg(
            Arg::new("i_flag")
           .short('i')
           .long("install")
           .visible_short_aliases(['I'])
           .required(false)
           .help("A flag signifying initial run, creates summary and context tables only")
           .action(clap::ArgAction::SetTrue)
       )
       .arg(
            Arg::new("c_flag")
            .short('c')
            .long("context")
            .visible_short_aliases(['C'])
            .required(false)
            .help("A flag signifying that context tables need to be rebuilt")
            .action(clap::ArgAction::SetTrue)
       )
       .arg(
            Arg::new("m_flag")
            .short('m')
            .long("summsetup")
            .visible_short_aliases(['M'])
            .required(false)
            .help("A flag signifying that summary tables should be recreated")
            .action(clap::ArgAction::SetTrue)
       )
       .arg(
            Arg::new("t_flag")
            .short('t')
            .long("test")
            .visible_short_aliases(['T'])
            .required(false)
            .help("A flag signifying that this is part of an integration test run")
            .action(clap::ArgAction::SetTrue)
       )
    .try_get_matches_from(args)

}


#[cfg(test)]
mod tests {
    use super::*;
    
    // Ensure the parameters are being correctly extracted from the CLI arguments

    #[test]
    fn check_cli_no_explicit_params() {
        let target = &"target\\debug\\ror1.exe".replace("\\", "/");
        let args : Vec<&str> = vec![target];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
        let res = fetch_valid_arguments(test_args).unwrap();
        assert_eq!(res.data_folder, PathBuf::new());
        assert_eq!(res.source_file, "");
        assert_eq!(res.import_ror, true);
        assert_eq!(res.process_data, false);
        assert_eq!(res.report_data, false);
        assert_eq!(res.create_lup, false);
        assert_eq!(res.create_smm, false);
        assert_eq!(res.test_run, false);
        assert_eq!(res.data_date, "");
        assert_eq!(res.data_version, "");
    }
  
    #[test]
    fn check_cli_with_a_flag() {
        let target = &"target\\debug\\ror1.exe".replace("\\", "/");
        let args : Vec<&str> = vec![target, "-A"];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();

        let res = fetch_valid_arguments(test_args).unwrap();
        assert_eq!(res.data_folder, PathBuf::new());
        assert_eq!(res.source_file, "");
        assert_eq!(res.import_ror, true);
        assert_eq!(res.process_data, true);
        assert_eq!(res.report_data, true);
        assert_eq!(res.create_lup, false);
        assert_eq!(res.create_smm, false);
        assert_eq!(res.test_run, false);
        assert_eq!(res.data_date, "");
        assert_eq!(res.data_version, "");
    }

    #[test]
    fn check_cli_with_i_flag() {
        let target = &"target\\debug\\ror1.exe".replace("\\", "/");
        let args : Vec<&str> = vec![target, "-I"];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();

        let res = fetch_valid_arguments(test_args).unwrap();
        assert_eq!(res.data_folder, PathBuf::new());
        assert_eq!(res.source_file, "");
        assert_eq!(res.import_ror, false);
        assert_eq!(res.process_data, false);
        assert_eq!(res.report_data, false);
        assert_eq!(res.create_lup, true);
        assert_eq!(res.create_smm, true);
        assert_eq!(res.test_run, false);
        assert_eq!(res.data_date, "");
        assert_eq!(res.data_version, "");
    }

    #[test]
    fn check_cli_with_c_and_m_flags() {
        let target = &"target\\debug\\ror1.exe".replace("\\", "/");
        let args : Vec<&str> = vec![target, "-C", "-M"];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();

        let res = fetch_valid_arguments(test_args).unwrap();
        assert_eq!(res.data_folder, PathBuf::new());
        assert_eq!(res.source_file, "");
        assert_eq!(res.import_ror, false);
        assert_eq!(res.process_data, false);
        assert_eq!(res.report_data, false);
        assert_eq!(res.create_lup, true);
        assert_eq!(res.create_smm, true);
        assert_eq!(res.test_run, false);
        assert_eq!(res.data_date, "");
        assert_eq!(res.data_version, "");
    }


    #[test]
    fn check_cli_with_c_and_p_flag() {
        let target = &"target\\debug\\ror1.exe".replace("\\", "/");
        let args : Vec<&str> = vec![target, "-C", "-P"];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();

        let res = fetch_valid_arguments(test_args).unwrap();
        assert_eq!(res.data_folder, PathBuf::new());
        assert_eq!(res.source_file, "");
        assert_eq!(res.import_ror, false);
        assert_eq!(res.process_data, true);
        assert_eq!(res.report_data, false);
        assert_eq!(res.create_lup, true);
        assert_eq!(res.create_smm, false);
        assert_eq!(res.test_run, false);
        assert_eq!(res.data_date, "");
        assert_eq!(res.data_version, "");
    }

    #[test]
    fn check_cli_with_explicit_string_pars() {
        let target = &"target\\debug\\ror1.exe".replace("\\", "/");
        let args : Vec<&str> = vec![target, "-f", "E:\\ROR\\some data folder", 
                                    "-s", "schema2 data.json", "-d", "2025-12-25", "-v", "1.62"];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();

        let res = fetch_valid_arguments(test_args).unwrap();
        assert_eq!(res.data_folder, PathBuf::from("E:/ROR/some data folder"));
        assert_eq!(res.source_file, "schema2 data.json");
        assert_eq!(res.import_ror, true);
        assert_eq!(res.process_data, false);
        assert_eq!(res.report_data, false);
        assert_eq!(res.create_lup, false);
        assert_eq!(res.create_smm, false);
        assert_eq!(res.test_run, false);
        assert_eq!(res.data_date, "2025-12-25");
        assert_eq!(res.data_version, "1.62");
    }

    #[test]
    fn check_cli_with_most_params_explicit() {
        let target = &"target\\debug\\ror1.exe".replace("\\", "/");
        let args : Vec<&str> = vec![target, "-f", "E:\\ROR\\some other data folder", 
        "-s", "schema2.1 data.json", "-d", "2026-12-25", "-v", "1.63", "-R", "-P", "-X", "-C", "-T"];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();

        let res = fetch_valid_arguments(test_args).unwrap();
        assert_eq!(res.data_folder, PathBuf::from("E:/ROR/some other data folder"));
        assert_eq!(res.source_file, "schema2.1 data.json");
        assert_eq!(res.import_ror, true);
        assert_eq!(res.process_data, true);
        assert_eq!(res.report_data, true);
        assert_eq!(res.create_lup, true);
        assert_eq!(res.create_smm, false);
        assert_eq!(res.test_run, true);
        assert_eq!(res.data_date, "2026-12-25");
        assert_eq!(res.data_version, "1.63");
    }
}

