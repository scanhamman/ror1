/***************************************************************************
 * Module uses std::env to read environmental values from an .env file 
 * (must be in same folder as src). These include the database parameters, 
 * which are loaded into a static singleton value, to allow it to be 
 * interrogated later in the program if necessary. During setup, however, 
 * the DB parameters are only used to provide a database connection string 
 * for any specified database.
 * The .env file also contains the default names for the overall folder path 
 * (used for both source data and logs) and for the source and results file 
 * name. 
 * 
 * Database parameters MUST be provided and be valid or the program can not
 * continue. 
 * The folder path and the source file name have defaults but these should 
 * NOT normally be used. They are there only as placeholders, to be overwritten by 
 * values provided as string arguments in the command line or the .env file. 
 * In other words the folder path and the source file name MUST be present 
 * EITHER in the .env file OR in the CLI arguments. 
 * If both, the CLI arguments take precedence.
 * The results file name has a timestamped default name that will be used if 
 * none is provided explicitly.
 ***************************************************************************/

use std::sync::OnceLock;
use std::env;
use std::path::PathBuf;
use dotenv;
use crate::error_defs::{AppError, CustomError};

#[derive(Debug)]
pub struct DbPars {
    pub host: String,
    pub user: String,
    pub password: String,
    pub port: String,
    pub dbname: String,
}

pub static DB_PARS: OnceLock<DbPars> = OnceLock::new();

pub fn populate_env_vars() -> Result< (), AppError> {

    // Use the dotenv from_filename function to load the variables into std::env.

    let _env_res  = match dotenv::from_filename(".env")
    {
        Ok(pb) => pb,
        Err(err) => return Err(AppError::DeErr(err)),
    };
       
    // Extract the DB connection variables - N.B. user (name) and password have 
    // no meaningful defaults
    
    let host: String = env::var("db_host").unwrap_or("localhost".to_string());
    let user: String = env::var("db_user").unwrap_or("no user".to_string());
    let password: String = env::var("db_password").unwrap_or("no password".to_string());
    let port: String = env::var("db_port").unwrap_or("5432".to_string());
    let dbname: String = env::var("db_name").unwrap_or("ror".to_string());
       
    let db_pars = DbPars {
        host, 
        user,
        password,
        port,
        dbname,
    };
    let _ = DB_PARS.set(db_pars);  // should always work in this environment

    Ok(())

}
 
pub fn fetch_db_name() -> Result<String, AppError> {
    let db_pars = match DB_PARS.get() {
         Some(dbp) => dbp,
         None => {
            let msg = "Unable to obtain DB parameters when building connection string";
            let cf_err = CustomError::new(msg);
            return Result::Err(AppError::CsErr(cf_err));
        },
    };
    Ok(db_pars.dbname.clone())
}

pub fn fetch_db_conn_string(db_name: String) -> Result<String, AppError> {
    let db_pars = match DB_PARS.get() {
         Some(dbp) => dbp,
         None => {
            let msg = "Unable to obtain DB parameters when building connection string";
            let cf_err = CustomError::new(msg);
            return Result::Err(AppError::CsErr(cf_err));
        },
    };
    
    if db_pars.user == "no user" ||  db_pars.password == "no password"{  
        let msg = "No user or password present in environment file";
        let cf_err = CustomError::new(msg);
        return Result::Err(AppError::CsErr(cf_err));
    } 
    else {
        Ok(format!("postgres://{}:{}@{}:{}/{}", 
        db_pars.user, db_pars.password, db_pars.host, db_pars.port, db_name))
    }
}

pub fn fetch_data_folder() -> PathBuf {
    let path_as_string = env::var("data_folder_path").unwrap_or("".to_string());
    PathBuf::from(path_as_string.replace("\\", "/"))
}

pub fn fetch_log_folder() -> PathBuf {
    let path_as_string = env::var("log_folder_path").unwrap_or("".to_string());
    PathBuf::from(path_as_string.replace("\\", "/"))
}

pub fn fetch_output_folder() -> PathBuf {
    let path_as_string = env::var("output_folder_path").unwrap_or("".to_string());
    PathBuf::from(path_as_string.replace("\\", "/"))
}

pub fn fetch_source_file_name() -> String {
    env::var("src_file_name").unwrap_or("".to_string())
}

pub fn fetch_output_file_name() -> String {
    env::var("output_file_name").unwrap_or("".to_string())
}

pub fn fetch_data_version() -> String {
    env::var("data_version").unwrap_or("".to_string())
}

pub fn fetch_data_date() -> String {
    env::var("data_date").unwrap_or("".to_string())
}




