mod import;
mod setup;
mod transform;

use sqlx::postgres::PgPoolOptions;

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
    
    // Fetch start parameters such as file names and CLI flags
    // To check for error...

    let p = setup::get_params().await.unwrap();

    // Set up the database connection pool

    let db_conn = p.db_conn_string;
    let pool = PgPoolOptions::new().max_connections(5).connect(&db_conn).await?;
    
    // Import data into matching tables
        
    if p.import_source == true
    {
        let _i = import::import_data(&p.source_file_path, &pool).await;
    }
        
    // Transform data into more useful tables
    
    if p.process_source == true
    {
        let _i = transform::process_data(&p.res_file_path, &pool).await;
    }

    Ok(())

}




