/**********************************************************
 * A small library to import ror data from the ror data 
 * dump, maintained on Zenodo, process that data (lightly),
 * and summarise key features of it. The summaries for each 
 * version are retained, whilst the base data is replaced 
 * each time the program is run. The system can also output 
 * a report summarising the key features of the data set. 
 * See the read.me file for further details.
 **********************************************************/

pub mod setup;
mod import;
mod transform;
pub mod error_defs;

use error_defs::AppError;
use setup::log_helper;
use std::ffi::OsString;

pub async fn run(args: Vec<OsString>) -> Result<(), AppError> {
    
    // Important that there are no errors in the intial three steps.
    // If one does occur the program exits.
    // 1) Collect initial parameters such as file names and CLI flags. 
    // CLI arguments are collected explicitly to facilitate unit testing. 
    // of 'get_params'. Relevant environmental variables are also read.
    // 2) Establish a log file, in the specified data folder.
    // The initial parameters are recorded as the initial part of the log.
    // 3) The database connection pool is established for the database "ror".

    let params = setup::get_params(args).await?;
  
    if !params.test_run {
       log_helper::setup_log(&params.log_folder, &params.source_file_name)?;
       log_helper::log_startup_params(&params);
    }
            
    let pool = setup::get_db_pool().await?;

    // Processing of the remaining stages depends on the 
    // presence of the relevant CLI flag(s).

    // The first two routines normally run only as an initial 
    // 'setup' of the program's DB, but can be repeated later if required.
    // If combined with data import / processing they will obviously 
    // occur before that import / processing.

    if params.create_context
    {  
        setup::create_lup_tables(&pool).await?;
    }

    if params.create_summary
    {  
        setup::create_smm_tables(&pool).await?;
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
            if !params.test_run {
                import::summarise_import(&pool).await?;
            }
        }
    
        if params.process_data  // transfer data to src tables, and summarise in smm tables
        {
            transform::create_src_tables(&pool).await?;
            transform::process_data(&params.data_version, &params.data_date, &pool).await?;
        }

        if params.report_data  // write out summary data from data in smm tables
        { 
            if !params.test_run {
                transform::report_results(&params.output_folder, &params.output_file_name, &pool).await?;
            }
        }
    }

    Ok(())  
}
