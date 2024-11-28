pub mod org_table_code;

use log::{info, error};
use std::path::PathBuf;
use sqlx::{Pool, Postgres};

pub async fn process_data(_res_file_path : &PathBuf, pool : &Pool<Postgres>)
{
    let r = org_table_code::recreate_org_tables(&pool).await;
    match r {
        Ok(()) => info!("Source tables created"),
        Err(e) => error!("Oops, an error occured while creating the source tables: {}", e),
    }

    // Do data transformations to create the org schema data
    // to do...

    // create the res file. If it already exists overwrite it.

    
    


}