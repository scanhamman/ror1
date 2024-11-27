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
 * NOT be used. They are there only as placeholders, to be overwritten by 
 * values provided as string arguments in the command line. In other words the 
 * folder path and the source file name MUST be present EITHER in the .env 
 * file OR in the CLI arguments. If both, the CLI arguments take precedence.
 * The results file name has a timestamped default name that will be used if 
 * none is provided explicitly.
 ***************************************************************************/

use std::sync::OnceLock;
use std::env;
use dotenv;
use chrono::Local;

#[derive(Debug)]
pub struct DbPars {
    pub host: String,
    pub user: String,
    pub password: String,
    pub port: String,
}

pub static DB_PARS: OnceLock<DbPars> = OnceLock::new();

pub fn populate_env_vars() -> () {

    // Use the dotenv from_filename function to load the variables into std::env.
    
    dotenv::from_filename("ror.env").expect("Problem in reading parameters from env file");
    
    // Extract the DB connection variables - N.B. user (name) and password have 
    // no meaningful defaults
    
    let host: String = env::var("db_host").unwrap_or("localhost".to_string());
    let user: String = env::var("db_user").unwrap_or("no user".to_string());
    let password: String = env::var("db_password").unwrap_or("no password".to_string());
    let port: String = env::var("db_port").unwrap_or("5432".to_string());
       
    let db_pars = DbPars {
        host, 
        user,
        password,
        port,
    };
    DB_PARS.set(db_pars).expect("");

}
 
pub fn fetch_db_conn_string(db_name: &str) -> Result<String, &str> {
    let db_pars = DB_PARS.get().unwrap();
    if db_pars.user == "no user" ||  db_pars.password == "no password"{  
        Err("No user or password present in environment file")
    } 
    else {
        Ok(format!("postgres://{}:{}@{}:{}/{}", 
        db_pars.user, db_pars.password, db_pars.host, db_pars.port, db_name))
    }
}

pub fn fetch_folder_path() -> String {
    env::var("folder_path").unwrap_or("C:\\ROR".to_string())
}

pub fn fetch_source_file_name() -> String {
    env::var("src_file_name").unwrap_or("source-data.json".to_string())
}

pub fn fetch_results_file_name() -> String {
    let datetime_string = Local::now().format("%m-%d-%H-%M-%S").to_string();
    env::var("res_file_name").unwrap_or(format!("analysis-results - {}.json", datetime_string))
}









