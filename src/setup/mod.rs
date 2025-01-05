// The setup module. Referenced in main by 'mod setup'.
// It makes use of the other modules in the folder, each corresponding to a file of the same name.
// The folder modules do not need to be public - they are referenced only within this module.
// The log established by log_helper, however, seems to be available throughout the program
// via a suitable 'use' statement.

mod env_reader;
mod cli_reader;
pub mod log_helper;

use crate::error_defs::{AppError, CustomError};
use chrono::NaiveDate;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Postgres, Pool};
use log::error;
use std::path::PathBuf;
use std::path::Path;
use std::ffi::OsString;

pub struct InitParams {
    pub import_source: bool,
    pub process_source: bool,
    pub create_context: bool,
    pub data_folder : String,
    pub source_file_name : String,
    pub source_file_path : PathBuf,
    pub res_file_path : PathBuf,
    pub data_date : NaiveDate,
}

pub async fn get_params(args: Vec<OsString>) -> Result<InitParams, AppError> {

    // Called from main as the initial task of the program.
    // Returns a struct that conbtains the program's parameters.
    // Start by obtaining CLI arguments and reading parameters from .env file.
    
    let cli_pars = cli_reader::fetch_valid_arguments(args)?;
    env_reader::populate_env_vars()?; 
    
    
    // If folder name also given in CL args the CL version takes precedence

    let mut data_folder =  env_reader::fetch_folder_path();
    if cli_pars.data_folder != "" {
        data_folder = cli_pars.data_folder;
    }
    else {
        if data_folder == "" {
            // raise an AppError...both are missing
            let msg = "Data folder name not provided in either command line or environment file";
            let cf_err = CustomError::new(msg);
            return Result::Err(AppError::CsErr(cf_err));
        }
    }
        
    // Does this folder exist and is it accessible? - If not end the program...

    let xres = Path::new(&data_folder).try_exists();
    let x = match xres {
        Ok(true) => true,
        Ok(false) => false,    // need specific error here 
        Err(_e) => false,           
    };
    if x == false {
        // raise an AppError...
        let msg = "Stipulated data folder does not exists or is not accessible";
        let cf_err = CustomError::new(msg);
        return Result::Err(AppError::CsErr(cf_err));
    }
 
    // If source file name given in CL args the CL version takes precedence.
    
    let mut source_file_name =  env_reader::fetch_source_file_name();
    if cli_pars.source_file != "" {
        source_file_name = cli_pars.source_file;
    }
    else {
            if source_file_name == "" {
             // raise an AppError...both are missing
            let msg = "Source file name not provided in either command line or environment file";
            let cf_err = CustomError::new(msg);
            return Result::Err(AppError::CsErr(cf_err));
         }
    }
    
    let source_file_path : PathBuf = [&data_folder, &source_file_name].iter().collect();

    // Checking the source file's existence will take place on initial read...
    // For results file name simply read from the environment variables and add to data_folder

    let res_file_name =  env_reader::fetch_results_file_name();
    let res_file_path : PathBuf = [&data_folder, &res_file_name].iter().collect();

    // get the date of the data
    // first check CLI date, if any...  start with a default base date.
    
    let base_date = NaiveDate::from_ymd_opt(1900,1,1).unwrap();
    let mut date_of_data = base_date.clone();
        
    if cli_pars.data_date != "" {
        // check if date
        date_of_data = match NaiveDate::parse_from_str(&cli_pars.data_date, "%Y-%m-%d")
        {
            Ok(d) => d,
            Err(_e) => base_date,
        }
    }

    if date_of_data == base_date  // No data date in CLI or was not valid format
    {
        // Need to check env. data date.

        date_of_data = match NaiveDate::parse_from_str(&env_reader::fetch_data_date_string(), "%Y-%m-%d")
        {
            Ok(d) => d,
            Err(_e) => base_date,
        }
    }

    if date_of_data == base_date {

        // No date anywhere - we have a problem - raise an AppError and end program.

        let msg = "Data date not provided in either command line or environment file";
        let cf_err = CustomError::new(msg);
        return Result::Err(AppError::CsErr(cf_err));
    }

    // For execution flags simply read from the environment variables
       
    Ok(InitParams {
        import_source : cli_pars.import_source,
        process_source : cli_pars.process_source,
        create_context: cli_pars.create_context,
        data_folder: data_folder,
        source_file_name : source_file_name,
        source_file_path : source_file_path.clone(),
        res_file_path : res_file_path.clone(),
        data_date : date_of_data.clone(),
    })

}





pub async fn get_db_pool(db_name :&str) -> Result<Pool<Postgres>, AppError> {  
    
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


// Tests
#[cfg(test)]
mod tests {
    use super::*;
   
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

        // Note that the folder path given must exist, 
        // and be accessible, or get_params will panic
        // and an error will be thrown. 

        temp_env::async_with_vars(
        [
            ("folder_path", Some("E:\\ROR\\20241211 1.58 data")),
            ("src_file_name", Some("v1.58 20241211.json")),
            ("data_date", Some("2025-12-11")),
            ("res_file_name", Some("results 25.json")),
        ],
        async { 
            let args : Vec<&str> = vec!["target\\debug\\ror1.exe"];
            let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
            let res = get_params(test_args).await.unwrap();
    
            assert_eq!(res.import_source, true);
            assert_eq!(res.process_source, false);
            assert_eq!(res.create_context, false);
            let sf_path: PathBuf = ["E:\\ROR\\20241211 1.58 data", "v1.58 20241211.json"].iter().collect();
            assert_eq!(res.source_file_path, sf_path);
            let rf_path: PathBuf = ["E:\\ROR\\20241211 1.58 data", "results 25.json"].iter().collect();
            assert_eq!(res.res_file_path, rf_path);
            let nd = NaiveDate::parse_from_str("2025-12-11", "%Y-%m-%d").unwrap();
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
            ("folder_path", Some("E:\\ROR\\20241211 1.58 data")),
            ("src_file_name", Some("v1.58 20241211.json")),
            ("data_date", Some("2025-12-11")),
            ("res_file_name", Some("results 27.json")),
        ],
        async { 
            let args : Vec<&str> = vec!["target\\debug\\ror1.exe", "-S", "-C", "-T", "-f", "E:\\ROR\\20240607 1.47 data", "-d", "2026-12-25", "-s", "schema2 data.json"];
            let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
            let res = get_params(test_args).await.unwrap();
    
            assert_eq!(res.import_source, true);
            assert_eq!(res.process_source, true);
            assert_eq!(res.create_context, true);
            let sf_path: PathBuf = ["E:\\ROR\\20240607 1.47 data", "schema2 data.json"].iter().collect();
            assert_eq!(res.source_file_path, sf_path);
            let rf_path: PathBuf = ["E:\\ROR\\20240607 1.47 data", "results 27.json"].iter().collect();
            assert_eq!(res.res_file_path, rf_path);
            let nd = NaiveDate::parse_from_str("2026-12-25", "%Y-%m-%d").unwrap();
            assert_eq!(res.data_date, nd);
        }
       ).await;

    }
}

