use std::sync::OnceLock;
use std::env;
use dotenv;

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
    env::var("folder_path").unwrap_or("E:\\ROR".to_string())
}

pub fn fetch_source_file_name() -> String {
    env::var("src_file_name").unwrap_or("source-data.json".to_string())
}

pub fn fetch_results_file_name() -> String {
    env::var("res_file_name").unwrap_or("analysis-results.json".to_string().to_string())
}









