mod import;
mod setup;
mod transform;
mod errors;

use errors::AppError;
use sqlx::postgres::PgPoolOptions;
//use sqlx::Error;

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

async fn main() -> Result<(), AppError> {
    
    // Important that there are no errors in these intial two steps.
    // If one does occur the error needs to be logged and 
    // the program must then stop.

    // First, fetch start parameters such as file names and CLI flags
    
    let params_res = setup::get_params().await;
    let p = match params_res {
        Ok(pres) => pres,
        Err(e) => {
            log_critical_error(&e); 
            return Err(e)
        }, 
    };
    
    // Second, set up the database connection pool

    let db_conn = p.db_conn_string;
    let pool = PgPoolOptions::new()
    .max_connections(5)
    .connect(&db_conn).await.unwrap();

    if p.import_source == true
    {
        // Import data into matching tables
        let _i = import::import_data(&p.source_file_path, &pool).await;
    }
    

    if p.process_source == true
    {
        // Transform data into more useful tables
        let _i = transform::process_data(&p.res_file_path, &pool).await;
    }

    Ok(())  
}



fn log_critical_error (_e : &AppError ) {
    //println!("Error: {}", e)
}


fn _log_process_error (_e : &AppError ) {
    //println!("Error: {}", e)
}




