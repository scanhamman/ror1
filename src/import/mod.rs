pub mod ror_json_models;
pub mod src_table_code;
pub mod ror_data_vectors;

use log::{info, error};
use std::path::PathBuf;
use std::fs;
use sqlx::{Pool, Postgres};

use ror_json_models::RorRecord;
use ror_data_vectors::{CoreDataVecs, RequiredDataVecs, NonRequiredDataVecs};

pub async fn import_data(source_file_path : &PathBuf, pool : &Pool<Postgres>)
{
    //Import data into matching tables

    // First obtain the raw data as text
    // check the file exists...by opening it and checking no error

    let data = fs::read_to_string(source_file_path).expect("Unable to read file");
    info!("Got the data from the file");

    // Parse into an internal JSON structure

    let res:Vec<RorRecord> = serde_json::from_str(&data).expect("Unable to parse JSON");
    info!("{} records found", res.len());

    // recreate the src tables

    let r = src_table_code::recreate_src_tables(&pool).await;
    match r {
        Ok(()) => info!("Source tables created"),
        Err(e) => error!("Oops, an error occured while creating the source tables: {}", e),
    }

    // Set up vector variables.
    // Vectors are grouped into structs for ease of reference.

    let vector_size = 100;
    let mut cdv: CoreDataVecs = CoreDataVecs::new(vector_size);
    let mut rdv: RequiredDataVecs = RequiredDataVecs::new(vector_size);
    let mut ndv: NonRequiredDataVecs = NonRequiredDataVecs::new(vector_size);

    // Run through each record and store contents in relevant vectors.
    // After every (vector_size) records store vector contents to database
    // and clear vectors, but continue looping through records.

    for (i, r) in res.iter().enumerate() {
    
        let db_id = extract_id_from(&r.id).to_string();

        cdv.add_core_data(r, &db_id); 
        rdv.add_required_data(r, &db_id); 
        ndv.add_non_required_data(r, &db_id); 
        
        if i > 205 { break;  }

        if (i + 1) % vector_size == 0 {  // store records to DB and clear vectors
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

}


fn extract_id_from(full_id: &String) -> &str {
    let b = full_id.as_bytes();
    std::str::from_utf8(&b[b.len()-9..]).unwrap()
}