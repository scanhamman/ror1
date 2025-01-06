mod import;
mod setup;
mod transform;
mod error_defs;

use error_defs::AppError;
use setup::log_helper;
use std::env;

/// A small program to process the ROR organisation data, as made
/// available in a JSON file download by ROR, and load that data
/// into a series of tables in a Postgres database. 

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), AppError> {
    
    // Important that there are no errors in the intial three steps.
    // If one does occur the program exits.
    // 1) Collect initial parameters such as file names and CLI flags. 
    // CLI arguments are collected explicitly to facilitate unit testing. 
    // of 'get_params'. Relevant environmental variables are also read.
    // 2) Establish a log file, in the specified data folder.
    // The initial parameters are recorded as the initial part of the log.
    // 3) The database connection pool is established for the database "ror".

    let args: Vec<_> = env::args_os().collect();
    let params = setup::get_params(args).await?;
  
    log_helper::setup_log(&params.log_folder, &params.source_file_name)?;
    log_helper::log_startup_params(&params);
            
    let pool = setup::get_db_pool("ror").await?;

    // Processing of the remaining stages depends on the 
    // presence of the relevant CLI flag(s).

    // The first two routines normally run only as an initial 
    // 'setup' of the program's DB, but can be repeated later if required.
    // If combined with data import / processing they will obviously 
    // occur before that import / processing.

    if params.create_context
    {  
        transform::create_lup_tables(&pool).await?;
    }

    if params.create_summary
    {  
        transform::create_smm_tables(&pool).await?;
    }
    
    // In each of the following stages, the initial step is to recreate 
    // the relevant DB tables, before doing the processing and summarising.
    // These steps are not considered if both create_context and create_summary 
    // are true (as in initial database installation).

    if !(params.create_context && params.create_summary) {

        if params.import_ror    // import ror from json file and store in ror schema tables
        {
            import::create_ror_tables(&pool).await?;
            import::import_data(&params.data_folder, &params.source_file_name, &pool).await?;
            import::summarise_import(&pool).await?;
        }
    
        if params.process_data  // transfer data to src tables, and summarise in smm tables
        {
            transform::create_src_tables(&pool).await?;
            transform::process_data(&params.data_version, &params.data_date, &pool).await?;
        }

        if params.report_data  // write out summary data from data in src tables
        { 
            transform::report_results(&params.output_folder, &params.output_file_name, &pool).await?;
        }
    }

    Ok(())  

}

