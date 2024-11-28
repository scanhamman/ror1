pub mod org_table_code;

use log::{info, error};
use std::path::PathBuf;
use sqlx::{Pool, Postgres};
use crate::AppError;

pub async fn create_org_tables(pool : &Pool<Postgres>) -> Result<(), AppError>
{
    let r = org_table_code::recreate_org_tables(&pool).await;
    match r {
        Ok(()) => {
            info!("Org tables created"); 
            return Ok(())
        },
        Err(e) => {
            error!("Oops, an error occured while creating the org tables: {}", e);
            return Err(AppError::SqErr(e))
            },
    }
}


pub async fn process_data(_res_file_path : &PathBuf, _pool : &Pool<Postgres>) -> Result<(), AppError>
{
    // Do data transformations to create the org schema data
    // to do...

    // create the res file. If it already exists overwrite it.

    
    
    Ok(())


}