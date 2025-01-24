mod export_text;
mod export_csv;
mod export_structs;

use log::{info, error};
use sqlx::{Pool, Postgres};
use crate::AppError;
use std::path::PathBuf;

pub async fn export_as_text(output_folder : &PathBuf, output_file_name: &String, 
               data_version: &String, pool : &Pool<Postgres>) -> Result<(), AppError>
{
    // Write out summary data for this dataset into the designated file

    let r = export_text::generate_text(output_folder, output_file_name, 
            data_version, pool).await;
    match r {
        Ok(()) => {
            info!("Data summary generated as text file"); 
            return Ok(())
        },
        Err(e) => {
            error!("An error occured while writing out the text file: {}", e);
            return Err(e)
            },
    }
}


pub async fn export_as_csv(output_folder : &PathBuf, output_file_name: &String,  
                data_version: &String, pool : &Pool<Postgres>) -> Result<(), AppError>
{
    // Write out summary data for this as a set of csv files into the designated folder

    let r = export_csv::generate_csv(output_folder, output_file_name, 
            data_version, pool).await;
    match r {
        Ok(()) => {
            info!("Data summary generated as csv files"); 
            return Ok(())
        },
        Err(e) => {
            error!("An error occured while writing out the csv files: {}", e);
            return Err(e)
            },
    }
}
