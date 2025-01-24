// The transform module. Referenced in main by 'mod transform'.
// It makes use of the other modules in the folder, each corresponding to a file of the same name.
// The folder modules do not need to be public - they are referenced only within this module.

mod src_data_importer;
mod src_data_processor;
mod src_tables_create;
mod smm_data_storer;
mod smm_structs;
mod src_rmv_dup_names;
pub mod smm_helper;

use log::{info, error};
use sqlx::{Pool, Postgres};
use crate::AppError;


pub async fn create_src_tables(pool : &Pool<Postgres>) -> Result<(), AppError>
{
    match src_tables_create::create_tables(pool).await {
        Ok(()) => info!("Tables created for src schema"),
        Err(e) => {
            error!("An error occured while creating the src schema tables: {}", e);
            return Err(e)
            },
    };
    match src_tables_create::create_admin_data_table(pool).await {
        Ok(()) => info!("Admin data table created in src schema"),
        Err(e) => {
            error!("An error occured while creating the src admin data table: {}", e);
            return Err(e)
            },
    };
    Ok(())

}

pub async fn process_data(pool : &Pool<Postgres>) -> Result<(), AppError>
{

    // Import the data from ror schema to src schema.

    match src_data_importer::import_data(pool).await
    {
        Ok(()) => {
            info!("ror schema data processed and transferred to src tables"); 
        },
        Err(e) => {
            error!("An error occured while transferring to the src tables: {}", e);
            return Err(e)
            },
    }

    // Calculate number of attributes for each org, and populate the admin data table with results.

    match src_data_processor::store_org_attribute_numbers(pool).await
    {
        Ok(()) => {
            info!("Org attributes counted and results added to admin table"); 
        },
        Err(e) => {
            error!("An error occured while processing the imported data: {}", e);
            return Err(e)
            },
    }

    // Add the script codes to the names.

    match src_data_processor::add_script_codes(pool).await
    {
        Ok(()) => {
            info!("Script codes added to organisation names"); 
        },
        Err(e) => {
            error!("An error occured while adding the script codes: {}", e);
            return Err(AppError::SqErr(e))
            },
    }

    Ok(())
}


pub async fn summarise_data(pool : &Pool<Postgres>) -> Result<(), AppError>
{
    // Store data into smm tables.

    match smm_data_storer::store_summary_data(pool).await
    {
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


