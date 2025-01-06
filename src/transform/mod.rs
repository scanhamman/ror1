// The transform module. Referenced in main by 'mod transform'.
// It makes use of the other modules in the folder, each corresponding to a file of the same name.
// The folder modules do not need to be public - they are referenced only within this module.

mod src_table_creator;
mod cxt_table_creator;
mod smm_table_creator;

mod src_data_importer;
mod src_data_processor;
mod src_data_reporter;
mod src_data_storer;

use log::{info, error};
use sqlx::{Pool, Postgres};
use crate::AppError;
use chrono::NaiveDate;

pub async fn create_src_tables(pool : &Pool<Postgres>) -> Result<(), AppError>
{
    let r = src_table_creator::recreate_src_tables(&pool).await;
    match r {
        Ok(()) => {
            info!("tables created for src schema"); 
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
            info!("tables created for lup schema"); 
            return Ok(())
        },
        Err(e) => {
            error!("An error occured while creating the lup tables: {}", e);
            return Err(AppError::SqErr(e))
            },
    }
}


pub async fn create_smm_tables(pool : &Pool<Postgres>) -> Result<(), AppError>
{
    let r = smm_table_creator::recreate_smm_tables(&pool).await;
    match r {
        Ok(()) => {
            info!("tables created for smm schema"); 
            return Ok(())
        },
        Err(e) => {
            error!("An error occured while creating the smm tables: {}", e);
            return Err(AppError::SqErr(e))
            },
    }
}

pub async fn process_data(data_version: &String, data_date: &NaiveDate, pool : &Pool<Postgres>) -> Result<(), AppError>
{
    // import the data from ror schema to src schema

    let r = src_data_importer::import_data(pool).await;
    match r {
        Ok(()) => {
            info!("Initial ror data processed and transferred to src tables"); 
        },
        Err(e) => {
            error!("An error occured while transferring to the src tables: {}", e);
            return Err(AppError::SqErr(e))
            },
    }

    // Calculate number of attributes for each org,
    // and populate the admin data table with results

    let r = src_data_processor::store_org_attribute_numbers(pool).await;
    match r {
        Ok(()) => {
            info!("Org attributes counted and results added to admin table"); 
        },
        Err(e) => {
            error!("An error occured while processing the imported data: {}", e);
            return Err(AppError::SqErr(e))
            },
    }

    // Store data into summ tables
    let r = src_data_storer::store_summary_data(data_version, data_date, pool).await;
    
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


pub async fn report_results(output_folder : &String, output_file_name: &String, pool : &Pool<Postgres>) -> Result<(), AppError>
{
    // Write out summary data for this dataset into the designated file

    let r = src_data_reporter::report_on_data(output_folder, output_file_name, pool).await;
    match r {
        Ok(()) => {
            info!("Data summary generated as file"); 
            return Ok(())
        },
        Err(e) => {
            error!("An error occured while writing out the : {}", e);
            return Err(e)
            },
    }
}

