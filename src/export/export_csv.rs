use sqlx::{Pool, Postgres};
use std::path::PathBuf;
use crate::AppError;

pub async fn generate_csv(output_folder : &PathBuf, output_file_name: &String, 
            _data_version: &String, _pool : &Pool<Postgres>) -> Result<(), AppError>
{
    // Report data analysis into a text file
    
    let _output_file_path: PathBuf = [output_folder, &PathBuf::from(output_file_name)].iter().collect();
            
    // files to be exported
    // If version specified only for that version
    // if no version stipulated will be for current (most recently imported) version
    // if capital X will be for all data
    
    // 1) Version summary row(s)

    // set up some tables...

    // Set up some views??...

    // COPY your_table TO '/path/to/your_file.csv' WITH (FORMAT CSV, HEADER);

    // COPY (SELECT * FROM your_view_name) TO '/path/to/your/file.csv' WITH (FORMAT CSV, HEADER);


    // Create the res file. If it already exists overwrite it.

    Ok(())

}
