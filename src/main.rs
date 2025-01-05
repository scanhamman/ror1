mod import;
mod setup;
mod transform;
mod error_defs;

use error_defs::AppError;
use setup::log_helper;
use std::env;
use std::path::PathBuf;

/// A small program to process the ROR organisation data, as made
/// available in a JSON file download by ROR, and load that data
/// into a series of tables in a Postgres database. 

#[tokio::main(flavor = "current_thread")]

async fn main() -> Result<(), AppError> {
    
    // Important that there are no errors in these intial three steps.
    // If one does occur the program exits.

    // First, collect program arguments and fetch start parameters 
    // such as file names and CLI flags. Arguments are collected explicitly 
    // to allow unit testing of the process (using injected arguments).
      
    let args: Vec<_> = env::args_os().collect();
    let try_params = setup::get_params(args).await;
    let initial_params = match try_params {
        Ok(p) => p,
        Err(e) => {
            return Err(e)
        }, 
    };


    // Secondly, a log file is also established in the specified data folder.
    // It's name references the source data file, as well as the date-time.
    // The startup parameters are recorded as the initial part of the log.

    let log_file_name = log_helper::get_log_file_name(&initial_params.source_file_name);
    let log_file_path: PathBuf = [&initial_params.data_folder, &log_file_name].iter().collect();
    log_helper::setup_log(&log_file_path)?;
    log_helper::log_startup_params(&initial_params);
    
    // Third, The database connection pool is established for the database "ror"
    
    let try_pool = setup::get_db_pool("ror").await;
    let pool = match try_pool {
        Ok(p) => p,
        Err(e) => {
            return Err(e)
        }, 
    };


    // Next two stages dependent on the presence of the relevant flag(s)
    // In each, initial step is to recreate the DB tables, before doing
    // the processing and, where necessary, the summarising.

    if initial_params.import_source == true
    {
        import::create_src_tables(&pool).await?;
        
        import::import_data(&initial_params.source_file_path, &pool).await?;

        import::summarise_import(&pool).await?;
    }
    

    if initial_params.process_source == true
    {
        transform::create_org_tables(&pool).await?;

        if initial_params.create_context == true
        {  
            transform::create_lup_tables(&pool).await?;
        }

        transform::process_data(&pool).await?;

        transform::store_results(&initial_params.data_date, &pool).await?;

        transform::output_results(&initial_params.res_file_path, &pool).await?;
    }

    Ok(())  

}

