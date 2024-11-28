mod import;
mod setup;
mod transform;
mod errors;

use errors::AppError;

/// A small program to process the ROR organisation data, as made
/// available in a JSON file download by ROR, and load that data
/// into a series of tables in a Postgres database. 

#[tokio::main(flavor = "current_thread")]

async fn main() -> Result<(), AppError> {
    
    // Important that there are no errors in these intial two steps.
    // If one does occur the error needs to be logged and 
    // the program must then stop.

    // First, fetch start parameters such as file names and CLI flags
    
    let try_params = setup::get_params().await;
    let ip = match try_params {
        Ok(p) => p,
        Err(e) => {
            return Err(e)
        }, 
    };
    
    // Second, set up the database connection pool

    let db_conn = ip.db_conn_string;
    let try_pool = setup::get_db_pool(db_conn).await;
    let pool = match try_pool {
        Ok(p) => p,
        Err(e) => {
            return Err(e)
        }, 
    };

    // Next two stages dependent on the presence of the relevant flag(s)
    // In each, initial step is to recreate the DB tables, before doing
    // the processing and summarising

    if ip.import_source == true
    {
        import::create_src_tables(&pool).await?;
        
        import::import_data(&ip.source_file_path, &pool).await?;

        import::summarise_import(&pool).await?;
    }
    

    if ip.process_source == true
    {
        transform::create_org_tables(&pool).await?;

        transform::process_data(&ip.res_file_path, &pool).await?;

        //transform::summarise_results(&pool).await?;
    }

    Ok(())  

}

