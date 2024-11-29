pub mod org_table_code;
pub mod org_data_import;
pub mod org_data_reporter;

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
            error!("An error occured while creating the org tables: {}", e);
            return Err(AppError::SqErr(e))
            },
    }
}

pub async fn process_data(pool : &Pool<Postgres>) -> Result<(), AppError>
{
    // import the data

    let r = org_data_import::import_data(pool).await;
    match r {
        Ok(()) => {
            info!("Initial data imported to org tables"); 
        },
        Err(e) => {
            error!("An error occured while importing to the org tables: {}", e);
            return Err(AppError::SqErr(e))
            },
    }

    // Summarise data and populate the admin data table with results

    let r = org_data_import::summarise_data(pool).await;
    match r {
        Ok(()) => {
            info!("Data processed and results added to admin table"); 
            return Ok(())
        },
        Err(e) => {
            error!("An error occured while processing the imported data: {}", e);
            return Err(AppError::SqErr(e))
            },
    }

}


pub async fn summarise_results(res_file_path: &PathBuf, pool : &Pool<Postgres>) -> Result<(), AppError>
{
    let r = org_data_reporter::report_on_data(res_file_path, pool).await;
    match r {
        Ok(()) => {
            info!("Data summarised and written out"); 
            return Ok(())
        },
        Err(e) => {
            error!("An error occured while reporting on the the org tables: {}", e);
            return Err(AppError::SqErr(e))
            },
    }
}
