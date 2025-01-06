use sqlx::{Pool, Postgres};

pub async fn recreate_smm_tables (_pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {
    //TO DO
    Ok(())
}