pub mod env_reader;
pub mod cli_reader;
pub mod log_helper;

use std::path::PathBuf;
use std::path::Path;

pub struct InitParams{
    pub import_source: bool,
    pub process_source: bool,
    pub source_file_path : PathBuf,
    pub res_file_path : PathBuf,
    pub db_conn_string : String,
}

pub async fn get_params() -> Result<InitParams, sqlx::Error> {

    // Called from main as the initial task of the program.
    // Returns a struct that conbtains the program's parameters.
    // Start by obtaining CLI arguments and reading 
    // parameters from .env file.

    let cli_pars = cli_reader::fetch_valid_arguments();
    env_reader::populate_env_vars();    

    // If folder name also given in CL args the CL version takes precedence

    let mut source_folder =  env_reader::fetch_folder_path();
    if cli_pars.data_folder != "" {
        source_folder =  cli_pars.data_folder;
    }
    
    // does this folder exist? - If not end the program...

    let x = Path::new(&source_folder).try_exists();
    match x {
        Ok(true) => println!("Yep, it exists"),
        Ok(false) => println!("Nope, it doesn't exists"),
        Err(e) => println!("Cannot proceed!!! - stipulated data folder does not exist ({})", e),           
    } 

    // If source file name given in CL args the CL version takes precedence.
    
    let mut source_file_name =  env_reader::fetch_source_file_name();
    if cli_pars.source_file != "" {
        source_file_name = cli_pars.source_file;
    }
    let source_file_path : PathBuf = [&source_folder, &source_file_name].iter().collect();

    // Checking the file's existence will take place on initieal read...
    // For results file name simply read from the environment variables

    let res_file_name =  env_reader::fetch_results_file_name();
    let res_file_path : PathBuf = [&source_folder, &res_file_name].iter().collect();

    // Create and open a log file. Construct log file name, then full log_path, 
    // and then set up logging mechanism using log4rs by call to logging helper.
    // Finally log the initial parameters.

    let log_file_name = log_helper::get_log_file_name(&source_file_name);
    let log_file_path: PathBuf = [&source_folder, &log_file_name].iter().collect();
    log_helper::setup_log(&log_file_path);

    log_helper::log_startup_params(&source_folder, &source_file_name, &res_file_name, 
                           cli_pars.import_source, cli_pars.process_source );
    
    let db_conn_string = env_reader::fetch_db_conn_string("ror").unwrap();  // need to handle error

    Ok(InitParams {
        import_source : cli_pars.import_source,
        process_source : cli_pars.process_source,
        source_file_path : source_file_path.clone(),
        res_file_path : res_file_path.clone(),
        db_conn_string,
    })

    }
