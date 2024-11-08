mod json_models;
mod startup;
mod data;
use log::info;
use std::path::PathBuf;
use std::path::Path;
use std::fs;

use sqlx::postgres::PgPoolOptions;
use json_models::RorRecord;
use data::vecs::{CoreDataVecs, RequiredDataVecs, NonRequiredDataVecs};
use startup::env_reader;
use startup::param_checker;
use startup::log_helper;

/// A small program to process the ROR organisation data, as made
/// available in a JSON file download by ROR, and load that data
/// into a series of tables in a Postgres database. 
/// 
/// The program is designed to illustrate various aspects of 'real'
/// data oriented systems, including logging, use of environment 
/// variables, use of command line parameters, and database access.
/// 
/// There are two phases to the system. In phase 1 the data is downloaded,
/// transformed to matching structs with very little additional processing, 
/// which are then stored in the db. The resulting tables therefore mirror 
/// the source ROR data very closely. In phase 2 the data is processed to 
/// form a new set of tables, incorporating additional information, 
/// e.g. summary records for each organisation. The second set are designed 
/// to be used as the basis for use of the organisation data, including its
/// display, editing and incorporation into other systems.

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), sqlx::Error> {

    
    // ******************************************************************************** 
    // Phase 0 - Set up
    // ******************************************************************************** 

    // Obtain CLI arguments, includes possible folder, source file, and processing flags,
    // and read parameters with file and DB parameters from .env file

    let cli_pars = param_checker::fetch_valid_arguments();
    env_reader::populate_env_vars();    

    // If folder name also given in CL args the CL version takes precedence
    // check folder path exists - post error if it does not and stop execution

    let mut source_folder =  env_reader::fetch_folder_path();
    if cli_pars.data_folder != "" {
        source_folder =  cli_pars.data_folder;
    }

    if !Path::new(&source_folder).exists() {
        println!("Cannot proceed!!! - stipulated data folder does not exist ('{}')", source_folder);               
    } 

    // If source file name given in CL args the CL version takes precedence.
    // Results file name simply read from the environment variables

    let mut source_file_name =  env_reader::fetch_source_file_name();
    if cli_pars.source_file != "" {
        source_file_name = cli_pars.source_file;
    }

    let res_file_name =  env_reader::fetch_results_file_name();

    // Create and open a log file. Construct log file name, then full log_path, 
    // and then set up logging mechanism using log4rs by call to logging helper.
    // Finally log the initial parameters.

    let log_file_name = log_helper::get_log_file_name(&source_file_name);
    let log_file_path: PathBuf = [&source_folder, &log_file_name].iter().collect();
    log_helper::setup_log(log_file_path);

    log_helper::log_startup_params(&source_folder, &source_file_name, &res_file_name, 
                           cli_pars.import_source, cli_pars.process_source );
       

    // Set up the database connection pool

    let db_conn = env_reader::fetch_db_conn_string("ror").unwrap();  // need to handle error
    let pool = PgPoolOptions::new()
    .max_connections(5)
    .connect(&db_conn).await?;


    // ******************************************************************************** 
    // Phase 1 - Import data into matching tables
    // ******************************************************************************** 

    if cli_pars.import_source == true
    {
        // Obtain the raw data as text
        // check the file exists...by opening it and checking no error
        // Create full source file path

        let source_file_path: PathBuf = [&source_folder, &source_file_name].iter().collect();
        let data = fs::read_to_string(source_file_path).expect("Unable to read file");
        info!("Got the data from the file");

        // Parse into an internal JSON structure

        let res:Vec<RorRecord> = serde_json::from_str(&data).expect("Unable to parse JSON");
        info!("{} records found", res.len());

        // recreate the src tables

        data::src_table_code::recreate_src_tables(&pool).await;
    
        // Set up vector variables.
        // Vectors are grouped into structs for ease of reference.

        let vector_size = 100;
        let mut cdv: CoreDataVecs = CoreDataVecs::new(vector_size);
        let mut rdv: RequiredDataVecs = RequiredDataVecs::new(vector_size);
        let mut ndv: NonRequiredDataVecs = NonRequiredDataVecs::new(vector_size);

        // Run through each record and store contents in relevant vectors.
        // After every (vector_size) records store vector contents to database
        // and clear vectors, but continue looping through records.

        for (i, r) in res.iter().enumerate() {
        
            let db_id = extract_id_from(&r.id).to_string();

            cdv.add_core_data(r, &db_id); 
            rdv.add_required_data(r, &db_id); 
            ndv.add_non_required_data(r, &db_id); 
         
            if i > 205 { break;  }

            if (i + 1) % vector_size == 0 {  // store records to DB and clear vectors
                cdv.store_data(&pool).await;
                cdv = CoreDataVecs::new(vector_size);
                rdv.store_data(&pool).await;
                rdv = RequiredDataVecs::new(vector_size);
                ndv.store_data(&pool).await;
                ndv = NonRequiredDataVecs::new(vector_size);
            }
        }
        
        //store any residual vector contents

        cdv.store_data(&pool).await;
        rdv.store_data(&pool).await;
        ndv.store_data(&pool).await;

    }
    

    // ******************************************************************************** 
    // phase 2
    // ******************************************************************************** 

    if cli_pars.process_source == true
    {
        // recreate the org tables

        data::org_table_code::recreate_org_tables(&pool).await;

        // Do data transformations to create the org schema data
        // to do...

        // Do data summary to provide a report in a pre-set location
        //let _results_path = results_file_path;  // Note *_*results_path, as currently not used
        let _results_file_path: PathBuf = [&source_folder, &res_file_name].iter().collect();

    }

    Ok(())

}


fn extract_id_from(full_id: &String) -> &str {
    let b = full_id.as_bytes();
    std::str::from_utf8(&b[b.len()-9..]).unwrap()
}




