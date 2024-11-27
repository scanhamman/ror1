pub mod org_table_code;

// use log::{info, error};
use std::path::PathBuf;
use sqlx::{Pool, Postgres};

pub async fn process_data(_res_file_path : &PathBuf, pool : &Pool<Postgres>)
{
    org_table_code::recreate_org_tables(&pool).await;

    // Do data transformations to create the org schema data
    // to do...

    // create the res file. If it already exists overwrite it.

    
    


}