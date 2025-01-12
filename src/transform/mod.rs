// The transform module. Referenced in main by 'mod transform'.
// It makes use of the other modules in the folder, each corresponding to a file of the same name.
// The folder modules do not need to be public - they are referenced only within this module.

mod src_data_importer;
mod src_data_processor;
mod src_tables_create;

mod smm_data_report;
mod smm_data_storer;
mod smm_storage_helper;
mod smm_structs;

use log::{info, error};
use sqlx::{Pool, Postgres};
use crate::AppError;
use chrono::NaiveDate;
use std::path::PathBuf;


pub async fn create_src_tables(pool : &Pool<Postgres>) -> Result<(), AppError>
{
    match src_tables_create::create_tables(pool).await {
        Ok(()) => info!("Tables created for src schema"),
        Err(e) => {
            error!("An error occured while creating the src schema tables: {}", e);
            return Err(e)
            },
    };
    Ok(())
}

pub async fn process_data(data_version: &String, data_date: &NaiveDate, pool : &Pool<Postgres>) -> Result<(), AppError>
{
    // Import the data from ror schema to src schema.

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

    // Calculate number of attributes for each org, and populate the admin data table with results.

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

    // Add the script codes to the names.

    let r = src_data_processor::add_script_codes(pool).await;
    match r {
        Ok(()) => {
            info!("Script codes added to organisation names"); 
        },
        Err(e) => {
            error!("An error occured while adding the script codes: {}", e);
            return Err(AppError::SqErr(e))
            },
    }

    // Store data into smm tables.

    let r = smm_data_storer::store_summary_data(data_version, data_date, pool).await;
    
    match r {
        Ok(()) => {
            info!("Summary data transferred to smm tables"); 
            return Ok(())
        },
        Err(e) => {
            error!("An error occured while transferring summary: {}", e);
            return Err(e)
            },
    }

}


pub async fn report_results(output_folder : &PathBuf, output_file_name: &String, pool : &Pool<Postgres>) -> Result<(), AppError>
{
    // Write out summary data for this dataset into the designated file

    let r = smm_data_report::report_on_data(output_folder, output_file_name, pool).await;
    match r {
        Ok(()) => {
            info!("Data summary generated as file"); 
            return Ok(())
        },
        Err(e) => {
            error!("An error occured while writing out the results: {}", e);
            return Err(e)
            },
    }
}

