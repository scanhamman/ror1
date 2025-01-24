use sqlx::{Pool, Postgres};
use std::path::PathBuf;
use crate::AppError;

pub async fn generate_csv(output_folder : &PathBuf, output_file_name: &String, 
            _data_version: &String, _pool : &Pool<Postgres>) -> Result<(), AppError>
{
    // Report data analysis into a text file
    
    let _output_file_path: PathBuf = [output_folder, &PathBuf::from(output_file_name)].iter().collect();
            
    // set up some tables...

    // Set up some views??...

    // Use these to produce json

    // Use these to produce a text version

    // Create the res file. If it already exists overwrite it.

    Ok(())

}
