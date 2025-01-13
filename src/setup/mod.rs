// The setup module. Referenced in main by 'mod setup'.
// It makes use of the other modules in the folder, each corresponding to a file of the same name.
// The folder modules do not need to be public - they are referenced only within this module.
// The log established by log_helper, however, seems to be available throughout the program
// via a suitable 'use' statement.

mod env_reader;
mod cli_reader;
mod lup_tables_create;
mod lup_tables_insert;
mod smm_tables_create;
pub mod log_helper;

use crate::error_defs::{AppError, CustomError};
use chrono::NaiveDate;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Postgres, Pool};
use log::{info, error};
use chrono::Local;
use std::path::PathBuf;
use std::ffi::OsString;
use std::fs;
use regex::Regex;

pub struct InitParams {
    pub import_ror: bool,
    pub process_data: bool,
    pub report_data: bool,
    pub create_context: bool,
    pub create_summary: bool,
    pub test_run: bool,
    pub data_folder : PathBuf,
    pub log_folder: PathBuf,
    pub output_folder: PathBuf,
    pub source_file_name : String,
    pub output_file_name : String,
    pub data_version: String,
    pub data_date : NaiveDate,
}

pub async fn get_params(args: Vec<OsString>) -> Result<InitParams, AppError> {

    // Called from main as the initial task of the program.
    // Returns a struct that conbtains the program's parameters.
    // Start by obtaining CLI arguments and reading parameters from .env file.
    
    let cli_pars = cli_reader::fetch_valid_arguments(args)?;
    env_reader::populate_env_vars()?; 
    
    let base_date = NaiveDate::from_ymd_opt(1900,1,1).unwrap();

    if cli_pars.create_lup && cli_pars.create_smm {

       // Returned ONLY if this is the 'install' situation - 
       // which sets up the lookup and summary tables but does not process 
       // any ror data (and when any other flags or arguments are ignored).

        Ok(InitParams {
            import_ror : false,
            process_data : false,
            report_data : false,
            create_context: true,
            create_summary: true,
            test_run: false,
            data_folder: PathBuf::new(),
            log_folder: PathBuf::new(),
            output_folder: PathBuf::new(),
            source_file_name: "".to_string(),
            output_file_name: "".to_string(),
            data_version: "".to_string(),
            data_date : base_date,
        })
    }
    else {

        // Normal import and / or processing and / or outputting
        // If folder name also given in CL args the CL version takes precedence

        let empty_pb = PathBuf::from("");
        let mut data_folder_good = true;

        let mut data_folder = cli_pars.data_folder;
        if data_folder == empty_pb {
            data_folder =  env_reader::fetch_data_folder();
        }
             
        // Does this folder exist and is it accessible? - If not and the 
        // 'R' (import ror) option is active, raise error and exit program.
                
        if !folder_exists (&data_folder) 
        {   
            data_folder_good = false;
        }
        if !data_folder_good && cli_pars.import_ror { 
            let msg = "Required data folder does not exists or is not accessible";
            let cf_err = CustomError::new(msg);
            return Result::Err(AppError::CsErr(cf_err));
        }

        let mut log_folder = env_reader::fetch_log_folder();
        if log_folder == empty_pb && data_folder_good {
            log_folder = data_folder.clone();
        }
        else {
            if !folder_exists (&log_folder) { 
                fs::create_dir_all(&log_folder)?;
            }
        }

        let mut output_folder = env_reader::fetch_output_folder();
        if output_folder == empty_pb && data_folder_good {
            output_folder = data_folder.clone();
        }
        else {
            if !folder_exists (&output_folder) { 
                fs::create_dir_all(&output_folder)?;
            }
        }
               

        // If source file name given in CL args the CL version takes precedence.
    
        let mut source_file_name= cli_pars.source_file;
        if source_file_name == "" {
            source_file_name =  env_reader::fetch_source_file_name();
            if source_file_name == "" {   // Required data is missing - Raise error and exit program.
                let msg = "Source file name not provided in either command line or environment file";
                let cf_err = CustomError::new(msg);
                return Result::Err(AppError::CsErr(cf_err));
             }
        }

        
        let mut data_version = "".to_string();
        let mut data_date = base_date;
       
        // If file name conforms to the correct pattern data version and data date can be derived!
        
        if is_valid_file_name(&source_file_name) {
            
            let version_pattern = r#"^v[0-9]+(\.[0-9]+){0,2}(-| )"#;
            let re = Regex::new(version_pattern).unwrap();
            if re.is_match(&source_file_name) {
                let caps = re.captures(&source_file_name).unwrap();
                data_version = caps[0].trim().to_string();
            }

            let date_pattern = r#"(-| )[0-9]{4}-?[01][0-9]-?[0-3][0-9]"#;
            let re = Regex::new(date_pattern).unwrap();
            if re.is_match(&source_file_name) {
                let caps = re.captures(&source_file_name).unwrap();
                let putative_date = caps[0].trim();
                if putative_date.len() == 8 {
                    data_date = match NaiveDate::parse_from_str(putative_date, "%Y%m%d")
                    {
                        Ok(d) => d,
                        Err(_e) => base_date,
                    }
                } 
                else {
                    data_date = match NaiveDate::parse_from_str(putative_date, "%Y-%m-%d")
                    {
                        Ok(d) => d,
                        Err(_e) => base_date,
                    }
                }
            }
        }
        else {

            // Otherwise, get the version of the data from the CLI, or failing that the config file
            data_version= cli_pars.data_version;
            if data_version == "" {
                data_version =  env_reader::fetch_data_version();
                if data_version == "" {   // Required data is missing - Raise error and exit program.
                    let msg = "Data version not provided in either command line or environment file";
                    let cf_err = CustomError::new(msg);
                    return Result::Err(AppError::CsErr(cf_err));
                }
            }

            // Get the date of the data. First check cli date, then .env date.  
            // Use the default base date to indicate missing data or an error somewhere.
        
            data_date = match NaiveDate::parse_from_str(&cli_pars.data_date, "%Y-%m-%d")
            {
                Ok(d) => d,
                Err(_e) => base_date,
            };
            if data_date == base_date {  
                    data_date = match NaiveDate::parse_from_str(&env_reader::fetch_data_date(), 
                                            "%Y-%m-%d") {
                    Ok(d) => d,
                    Err(_e) => base_date,
                };
                if data_date == base_date {   // Raise an AppError...required data is missing.
                    let msg = "Data date not provided in either command line or environment file";
                    let cf_err = CustomError::new(msg);
                    return Result::Err(AppError::CsErr(cf_err));
                }
            }
        }

        // get the output file name - if anywhere it is in the .env variables
        
        let mut output_file_name =  env_reader::fetch_output_file_name();
        if output_file_name == "" {
            output_file_name = format!("{} summary", data_version).to_string()
        }
        let datetime_string = Local::now().format("%m-%d %H%M%S").to_string();
        output_file_name = format!("{} at {}.txt", output_file_name, datetime_string);


  
        // For execution flags read from the environment variables
       
        Ok(InitParams {
            import_ror : cli_pars.import_ror,
            process_data : cli_pars.process_data,
            report_data: cli_pars.report_data,
            create_context: cli_pars.create_lup,
            create_summary: cli_pars.create_smm,
            test_run: cli_pars.test_run,
            data_folder,
            log_folder,
            output_folder,
            source_file_name,
            output_file_name,
            data_version,
            data_date,
        })

    }

}


pub async fn get_db_pool() -> Result<Pool<Postgres>, AppError> {  

    let db_name = env_reader::fetch_db_name().unwrap();  // default value of ror
    let db_conn_string = env_reader::fetch_db_conn_string(db_name)?;  
    let try_pool = PgPoolOptions::new()
              .max_connections(5).connect(&db_conn_string).await;
    let pool = match try_pool {
        Ok(p) => Ok(p),
        Err(e) => {
            error!("An error occured while creating the DB pool: {}", e);
            error!("Check the DB credentials and confirm the database is available");
            return Err(AppError::SqErr(e))
        }, 
    };
    pool
}

fn folder_exists(folder_name: &PathBuf) -> bool {
    let xres = folder_name.try_exists();
    let res = match xres {
        Ok(true) => true,
        Ok(false) => false, 
        Err(_e) => false,           
    };
    res
}

pub async fn create_lup_tables(pool : &Pool<Postgres>) -> Result<(), AppError>
{
    match lup_tables_create::create_tables(pool).await {
        Ok(()) => info!("Tables created for lup schema"),
        Err(e) => {
            error!("An error occured while creating the lup schema tables: {}", e);
            return Err(e)
            },
    };
    match lup_tables_insert::fill_tables(pool).await {
        Ok(()) => info!("Data added to lup tables"),
        Err(e) => {
            error!("An error occured while inserting data into the lup schema tables: {}", e);
            return Err(e)
            },
    };
    Ok(())
}

pub async fn create_smm_tables(pool : &Pool<Postgres>) -> Result<(), AppError>
{
    match smm_tables_create::create_tables(pool).await {
        Ok(()) => info!("Tables created for smm schema"),
        Err(e) => {
            error!("An error occured while creating the smm schema tables: {}", e);
            return Err(e)
            },
    };
    Ok(())
}

fn is_valid_file_name(input: &str) -> bool {
    let file_name_pattern = r#"^v[0-9]+(\.[0-9]+){0,2}(-| )[0-9]{4}-?[01][0-9]-?[0-3][0-9]"#;
    let re = Regex::new(file_name_pattern).unwrap();
    re.is_match(input)
}

// Tests
#[cfg(test)]

mod tests {
    use super::*;
   
   // regex tests
   #[test]
   fn check_pattern_matching_works_1 () {
      let test_file_name = "v1.50 2024-12-11.json".to_string();
      assert_eq!(is_valid_file_name(&test_file_name), true);

      let test_file_name = "v1.50-2024-12-11.json".to_string();
      assert_eq!(is_valid_file_name(&test_file_name), true);

      let test_file_name = "v1.50 20241211.json".to_string();
      assert_eq!(is_valid_file_name(&test_file_name), true);

      let test_file_name = "v1.50-20241211.json".to_string();
      assert_eq!(is_valid_file_name(&test_file_name), true);

      let test_file_name = "v1.50-2024-1211.json".to_string();
      assert_eq!(is_valid_file_name(&test_file_name), true);
   }

   #[test]
    fn check_pattern_matching_works_2 () {
        let test_file_name = "1.50 2024-12-11.json".to_string();
        assert_eq!(is_valid_file_name(&test_file_name), false);

        let test_file_name = "v1.50--2024-12-11.json".to_string();
        assert_eq!(is_valid_file_name(&test_file_name), false);

        let test_file_name = "v1.50  20241211.json".to_string();
        assert_eq!(is_valid_file_name(&test_file_name), false);

        let test_file_name = "v1.50 20242211.json".to_string();
        assert_eq!(is_valid_file_name(&test_file_name), false);
    }

    // Ensure the parameters are being correctly extracted from the CLI arguments
    // The testing functions need to be async because of the call to get_params.
    // the test therefore uses the async version of the temp_env::with_vars function.
    // This function needs to be awaited to execute.
    // The closure is replaced by an explicitly async block rather than
    // a normal closure. Inserting '||' before or after the 'async' results
    // in multiple complaints from the compiler. The async block can also
    // be replaced by a separate async function and called explicitly.
 
    #[tokio::test]
    async fn check_env_vars_overwrite_blank_cli_values() {

        // Note that in most cases the folder path given must exist, and be 
        // accessible, or get_params will panic and an error will be thrown. 

        temp_env::async_with_vars(
        [
            ("data_folder_path", Some("E:/ROR/data")),
            ("src_file_name", Some("v1.58 20241211.json")),
            ("output_file_name", Some("results 25.json")),
            ("data_version", Some("v1.60")),
            ("data_date", Some("2025-12-11")),

        ],
        async { 
            let args : Vec<&str> = vec!["target/debug/ror1.exe"];
            let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
            let res = get_params(test_args).await.unwrap();
    
            assert_eq!(res.import_ror, true);
            assert_eq!(res.process_data, false);
            assert_eq!(res.report_data, false);
            assert_eq!(res.create_context, false);
            assert_eq!(res.create_summary, false);
            assert_eq!(res.data_folder, PathBuf::from("E:/ROR/data"));
            assert_eq!(res.log_folder, PathBuf::from("E:/ROR/logs"));
            assert_eq!(res.output_folder, PathBuf::from("E:/ROR/outputs"));
            assert_eq!(res.source_file_name, "v1.58 20241211.json");
            let lt = Local::now().format("%m-%d %H%M%S").to_string();
            assert_eq!(res.output_file_name, format!("results 25.json at {}.txt", lt));
            assert_eq!(res.data_version, "v1.58");
            let nd = NaiveDate::parse_from_str("2024-12-11", "%Y-%m-%d").unwrap();
            assert_eq!(res.data_date, nd);
        }
       ).await;

    }


    #[tokio::test]
    async fn check_cli_vars_overwrite_env_values() {

        // Note that the folder path given must exist, 
        // and be accessible, or get_params will panic
        // and an error will be thrown. 

        temp_env::async_with_vars(
        [
            ("data_folder_path", Some("E:/ROR/20241211 1.58 data")),
            ("src_file_name", Some("v1.58 20241211.json")),
            ("data_version", Some("v1.59")),
            ("data_date", Some("2025-12-11")),
            ("output_file_name", Some("results 27.json")),
        ],
        async { 
            let args : Vec<&str> = vec!["target/debug/ror1.exe", "-R", "-P", "-X", 
                                     "-f", "E:/ROR/data", "-d", "2026-12-25", "-s", "schema2 data.json", "-v", "v1.60"];
            let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
            let res = get_params(test_args).await.unwrap();
    
            assert_eq!(res.import_ror, true);
            assert_eq!(res.process_data, true);
            assert_eq!(res.report_data, true);
            assert_eq!(res.create_context, false);
            assert_eq!(res.create_summary, false);
            assert_eq!(res.data_folder, PathBuf::from("E:/ROR/data"));
            assert_eq!(res.log_folder, PathBuf::from("E:/ROR/logs"));
            assert_eq!(res.output_folder, PathBuf::from("E:/ROR/outputs"));
            assert_eq!(res.source_file_name, "schema2 data.json");
            let lt = Local::now().format("%m-%d %H%M%S").to_string();
            assert_eq!(res.output_file_name, format!("results 27.json at {}.txt", lt));
            assert_eq!(res.data_version, "v1.60");
            let nd = NaiveDate::parse_from_str("2026-12-25", "%Y-%m-%d").unwrap();
            assert_eq!(res.data_date, nd);
        }
       ).await;

    }


    #[tokio::test]
    async fn check_cli_vars_with_i_flag() {

        // Note that the folder path given must exist, 
        // and be accessible, or get_params will panic
        // and an error will be thrown. 

        temp_env::async_with_vars(
        [
            ("data_folder_path", Some("E:/ROR/20241211 1.58 data")),
            ("src_file_name", Some("v1.58 20241211.json")),
            ("data_date", Some("2025-12-11")),
            ("output_file_name", Some("results 27.json")),
        ],
        async { 
            let args : Vec<&str> = vec!["target/debug/ror1.exe", "-R", "-P", "-I", "-f", "E:/ROR/data", "-d", "2026-12-25", "-s", "schema2 data.json"];
            let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
            let res = get_params(test_args).await.unwrap();
    
            assert_eq!(res.import_ror, false);
            assert_eq!(res.process_data, false);
            assert_eq!(res.report_data, false);
            assert_eq!(res.create_context,true);
            assert_eq!(res.create_summary, true);
            assert_eq!(res.data_folder, PathBuf::new());
            assert_eq!(res.log_folder, PathBuf::new());
            assert_eq!(res.output_folder, PathBuf::new());
            assert_eq!(res.source_file_name, "".to_string());
            assert_eq!(res.output_file_name, "".to_string());
            assert_eq!(res.data_version, "".to_string());
            let base_date = NaiveDate::parse_from_str("1900-01-01", "%Y-%m-%d").unwrap();
            assert_eq!(res.data_date, base_date);
        }
       ).await;

    }


    #[tokio::test]
    async fn check_cli_vars_with_a_flag_and_new_win_folders() {

        // Note that the folder path given must exist, 
        // and be accessible, or get_params will panic
        // and an error will be thrown. 

        temp_env::async_with_vars(
        [
            ("data_folder_path", Some("E:\\ROR\\20241211 1.58 data")),
            ("log_folder_path", Some("E:\\ROR\\some logs")),
            ("output_folder_path", Some("E:\\ROR\\dummy\\some outputs")),
            ("src_file_name", Some("v1.58 20241211.json")),
            ("data_date", Some("2025-12-11")),
            ("output_file_name", Some("results 28.json")),
        ],
        async { 
            let args : Vec<&str> = vec!["target/debug/ror1.exe", "-A", "-f", "E:\\ROR\\data", 
                                       "-d", "2026-12-25", "-s", "schema2 data.json", "-v", "v1.60"];
            let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
            let res = get_params(test_args).await.unwrap();
    
            assert_eq!(res.import_ror, true);
            assert_eq!(res.process_data, true);
            assert_eq!(res.report_data, true);
            assert_eq!(res.create_context, false);
            assert_eq!(res.create_summary, false);
            assert_eq!(res.data_folder, PathBuf::from("E:/ROR/data"));
            assert_eq!(res.log_folder, PathBuf::from("E:/ROR/some logs"));
            assert_eq!(res.output_folder, PathBuf::from("E:/ROR/dummy/some outputs"));
            assert_eq!(res.source_file_name, "schema2 data.json");
            let lt = Local::now().format("%m-%d %H%M%S").to_string();
            assert_eq!(res.output_file_name, format!("results 28.json at {}.txt", lt));
            assert_eq!(res.data_version, "v1.60");
            let nd = NaiveDate::parse_from_str("2026-12-25", "%Y-%m-%d").unwrap();
            assert_eq!(res.data_date, nd);
        }
      ).await;

    }
    
    #[tokio::test]
    async fn check_cli_vars_with_a_flag_and_new_posix_folders() {

        // Note that the folder path given must exist, 
        // and be accessible, or get_params will panic
        // and an error will be thrown. 

        temp_env::async_with_vars(
        [
            ("data_folder_path", Some("E:/ROR/data")),
            ("log_folder_path", Some("E:/ROR/some logs 2")),
            ("output_folder_path", Some("E:/ROR/dummy 2/some outputs")),
            ("src_file_name", Some("v1.58 20241211.json")),
            ("data_date", Some("2025-12-11")),
            ("output_file_name", Some("results 28.json")),
        ],
        async { 
            let args : Vec<&str> = vec!["target/debug/ror1.exe", "-A", "-f", "E:/ROR/data", 
                                       "-d", "2026-12-25", "-s", "schema2 data.json", "-v", "v1.60"];
            let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
            let res = get_params(test_args).await.unwrap();
    
            assert_eq!(res.import_ror, true);
            assert_eq!(res.process_data, true);
            assert_eq!(res.report_data, true);
            assert_eq!(res.create_context, false);
            assert_eq!(res.create_summary, false);
            assert_eq!(res.data_folder, PathBuf::from("E:/ROR/data"));
            assert_eq!(res.log_folder, PathBuf::from("E:/ROR/some logs 2"));
            assert_eq!(res.output_folder, PathBuf::from("E:/ROR/dummy 2/some outputs"));
            assert_eq!(res.source_file_name, "schema2 data.json");
            let lt = Local::now().format("%m-%d %H%M%S").to_string();
            assert_eq!(res.output_file_name, format!("results 28.json at {}.txt", lt));
            assert_eq!(res.data_version, "v1.60");
            let nd = NaiveDate::parse_from_str("2026-12-25", "%Y-%m-%d").unwrap();
            assert_eq!(res.data_date, nd);
        }
      ).await;

    }


    #[tokio::test]
    #[should_panic]
    async fn check_wrong_data_folder_panics_if_r() {
    
    temp_env::async_with_vars(
    [
        ("data_folder_path", Some("E:/ROR/20240607 1.47 data")),
        ("log_folder_path", Some("E:/ROR/some logs")),
        ("output_folder_path", Some("E:/ROR/dummy/some outputs")),
        ("src_file_name", Some("v1.58 20241211.json")),
        ("data_date", Some("2025-12-11")),
        ("output_file_name", Some("results 28.json")),
    ],
    async { 
        let args : Vec<&str> = vec!["target/debug/ror1.exe", "-A", "-f", "E:/silly folder name", 
                                    "-d", "2026-12-25", "-s", "schema2 data.json", "-v", "v1.60"];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
        let _res = get_params(test_args).await.unwrap();
        }
      ).await;
    }

    #[tokio::test]
    async fn check_wrong_data_folder_does_not_panic_if_not_r() {
    
        temp_env::async_with_vars(
        [
            ("data_folder_path", Some("E:/ROR/daft data")),
            ("log_folder_path", Some("E:/ROR/some logs")),
            ("output_folder_path", Some("E:/ROR/dummy/some outputs")),
            ("src_file_name", Some("v1.58 20241211.json")),
            ("data_date", Some("2025-12-11")),
            ("output_file_name", Some("results 28.json")),
        ],
        async { 
            let args : Vec<&str> = vec!["target/debug/ror1.exe", "-P", "-f", "E:/ROR/silly folder name", 
                                        "-d", "2026-12-25", "-s", "schema2 data.json", "-v", "v1.60"];
            let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
            let res = get_params(test_args).await.unwrap();
            assert_eq!(res.import_ror, false);
            assert_eq!(res.process_data, true);
            assert_eq!(res.report_data, false);
            assert_eq!(res.create_context, false);
            assert_eq!(res.create_summary, false);
            assert_eq!(res.data_folder, PathBuf::from("E:/ROR/silly folder name"));
            assert_eq!(res.log_folder, PathBuf::from("E:/ROR/some logs"));
            assert_eq!(res.output_folder, PathBuf::from("E:/ROR/dummy/some outputs"));
            assert_eq!(res.source_file_name, "schema2 data.json");
            let lt = Local::now().format("%m-%d %H%M%S").to_string();
            assert_eq!(res.output_file_name, format!("results 28.json at {}.txt", lt));
            assert_eq!(res.data_version, "v1.60");
            let nd = NaiveDate::parse_from_str("2026-12-25", "%Y-%m-%d").unwrap();
            assert_eq!(res.data_date, nd);

            }
        ).await;

        
    }

}


