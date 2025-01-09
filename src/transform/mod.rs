// The transform module. Referenced in main by 'mod transform'.
// It makes use of the other modules in the folder, each corresponding to a file of the same name.
// The folder modules do not need to be public - they are referenced only within this module.

mod src_data_importer;
mod src_data_processor;
mod smm_data_reporter;
mod src_data_storer;

use log::{info, error};
use sqlx::{Pool, Postgres};
use crate::AppError;
use chrono::NaiveDate;
use std::path::PathBuf;
use std::fs;

pub async fn create_src_tables(pool : &Pool<Postgres>) -> Result<(), AppError>
{
    let s = fs::read_to_string("./db_scripts/create_src_tables.sql")?;
    let _r = sqlx::raw_sql(&s).execute(pool).await?;
    info!("Tables created for src schema"); 
    Ok(())
}

pub async fn create_lup_tables(pool : &Pool<Postgres>) -> Result<(), AppError>
{
    let s = fs::read_to_string("./db_scripts/create_lup_tables.sql")?;
    let _r = sqlx::raw_sql(&s).execute(pool).await?;
    info!("Tables created for lup schema"); 

    let s = fs::read_to_string("./db_scripts/fill_lup_tables.sql")?;
    let _r = sqlx::raw_sql(&s).execute(pool).await?;
    info!("Data added to lup tables"); 

    Ok(())
}


pub async fn create_smm_tables(pool : &Pool<Postgres>) -> Result<(), AppError>
{
    let s = fs::read_to_string("./db_scripts/create_smm_tables.sql")?;
    let _r = sqlx::raw_sql(&s).execute(pool).await?;
    info!("Tables created for smm schema"); 
    Ok(())
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


pub async fn report_results(output_folder : &PathBuf, output_file_name: &String, pool : &Pool<Postgres>) -> Result<(), AppError>
{
    // Write out summary data for this dataset into the designated file

    let r = smm_data_reporter::report_on_data(output_folder, output_file_name, pool).await;
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

