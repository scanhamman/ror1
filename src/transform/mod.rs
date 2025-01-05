// The transform module. Referenced in main by 'mod transform'.
// It makes use of the other modules in the folder, each corresponding to a file of the same name.
// The folder modules do not need to be public - they are referenced only within this module.

mod src_table_creator;
mod cxt_table_creator;

mod src_data_importer;
mod src_data_processor;
mod src_data_reporter;

use log::{info, error};
use std::path::PathBuf;
use sqlx::{Pool, Postgres};
use crate::AppError;
use chrono::NaiveDate;

pub async fn create_org_tables(pool : &Pool<Postgres>) -> Result<(), AppError>
{
    let r = src_table_creator::recreate_src_tables(&pool).await;
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

pub async fn create_lup_tables(pool : &Pool<Postgres>) -> Result<(), AppError>
{
    let r = cxt_table_creator::recreate_lup_tables(&pool).await;
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

    let r = src_data_importer::import_data(pool).await;
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

    let r = src_data_processor::summarise_data(pool).await;
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


pub async fn store_results(_data_date: &NaiveDate, _pool : &Pool<Postgres>) -> Result<(), AppError>
{
    //let r = src_data_reporter::report_on_data(data_date, pool).await;
    //match r {
    //    Ok(()) => {
    //        info!("Data summarised and written out"); 
    //        return Ok(())
    //    },
    //    Err(e) => {
    //        error!("An error occured while reporting on the the org tables: {}", e);
    //        return Err(AppError::SqErr(e))
    //        },
    //}

    Ok(())
}


pub async fn output_results(res_file_path: &PathBuf, pool : &Pool<Postgres>) -> Result<(), AppError>
{
    let r = src_data_reporter::report_on_data(res_file_path, pool).await;
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
