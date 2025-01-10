use sqlx::{Pool, Postgres};
use std::path::PathBuf;
use crate::AppError;

pub async fn report_on_data(_output_folder : &PathBuf, _output_file_name: &String, _pool : &Pool<Postgres>) -> Result<(), AppError>
{
    // Report data analysis into a text file
    
    let _output_file_path: PathBuf = [_output_folder, &PathBuf::from(_output_file_name)].iter().collect();
    
    

    Ok(())

}
