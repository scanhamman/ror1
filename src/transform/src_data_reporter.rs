use sqlx::{Pool, Postgres};
use std::path::PathBuf;


pub async fn report_on_data(_res_file_path : &PathBuf, _pool : &Pool<Postgres>) -> Result<(), sqlx::Error>
{
    /* 
    // Create the res file. If it already exists overwrite it.

    let r = org_data_reporter::summarise_data(pool).await;
    match r {
        Ok(()) => {
            info!("Initial data imported to org tables"); 
            return Ok(())
        },
        Err(e) => {
            error!("Oops, an error occured while importing to the org tables: {}", e);
            return Err(AppError::SqErr(e))
            },
    }
    */

    Ok(())

}
