pub mod ror_json_models;
pub mod ror_table_creator;
pub mod ror_data_vectors;

use log::{info, error};
use std::path::PathBuf;
use std::fs;
use sqlx::{Pool, Postgres};
use crate::AppError;

use ror_json_models::RorRecord;
use ror_data_vectors::{CoreDataVecs, RequiredDataVecs, NonRequiredDataVecs};


pub async fn create_src_tables(pool : &Pool<Postgres>) -> Result<(), AppError>
{
    let r = ror_table_creator::recreate_ror_tables(&pool).await;
    match r {
        Ok(()) => {
            info!("Source tables created"); 
            return Ok(())
        },
        Err(e) => {
            error!("An error occured while creating the source tables: {}", e);
            return Err(AppError::SqErr(e))
            },
    }
}


pub async fn import_data(source_file_path : &PathBuf, pool : &Pool<Postgres>) -> Result<(), AppError>
{
    // Import data into matching tables

    // First obtain the raw data as text
    // This also checks the file exists...by opening it and checking no error

    let data: String = match fs::read_to_string(source_file_path)
    {
        Ok(d) => {
            info!("Got the data from the file");
            d
        }, 
        Err(e) => {
            error!("An error occured while opening or reading from the source file: {}", e);
            return Err(AppError::IoErr(e))
            },
    };

    // Parse into an internal JSON structure

    let res:Vec<RorRecord> = match serde_json::from_str(&data)
    {
        Ok(r) => {
            info!("Parsed the data into ROR json objects");
            r
        }, 
        Err(e) => {
            error!("An error occured while attempting tp parse the source data into json: {}", e);
            return Err(AppError::SdErr(e))
            },
    };
    
    info!("{} records found", res.len());

    // Set up vector variables.
    // Vectors are grouped into structs for ease of reference.

    let vector_size = 200;
    let mut cdv: CoreDataVecs = CoreDataVecs::new(vector_size);
    let mut rdv: RequiredDataVecs = RequiredDataVecs::new(vector_size);
    let mut ndv: NonRequiredDataVecs = NonRequiredDataVecs::new(vector_size);

    // Run through each record and store contents in relevant vectors.
    // After every (vector_size) records store vector contents to database
    // and clear vectors, but continue looping through records.
    let mut n = 0;
    for (i, r) in res.iter().enumerate() {
    
        let db_id = extract_id_from(&r.id).to_string();

        cdv.add_core_data(r, &db_id); 
        rdv.add_required_data(r, &db_id); 
        ndv.add_non_required_data(r, &db_id); 
        
        if i > 1505 { break;  }

        if (i + 1) % vector_size == 0 {  

            n = i+1;
            info!("{} records processed", n);
            
            // store records to DB and clear vectors
            cdv.store_data(&pool).await;
            cdv = CoreDataVecs::new(vector_size);
            rdv.store_data(&pool).await;
            rdv = RequiredDataVecs::new(vector_size);
            ndv.store_data(&pool).await;
            ndv = NonRequiredDataVecs::new(vector_size);
        }
    }
    
    //store any residual vector contents

    cdv.store_data(&pool).await;
    rdv.store_data(&pool).await;
    ndv.store_data(&pool).await;

    info!("Total records processed: {}", n + cdv.db_ids.len());

    Ok(())

}


pub async fn summarise_import(pool : &Pool<Postgres>) -> Result<(), AppError>
{
     // Go through each table and get total record number
    ror_table_creator::log_record_nums(pool).await?;
    Ok(())
}


fn extract_id_from(full_id: &String) -> &str {
    let b = full_id.as_bytes();
    std::str::from_utf8(&b[b.len()-9..]).unwrap()
}