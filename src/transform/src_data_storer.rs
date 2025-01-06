use sqlx::{Pool, Postgres};
use chrono::NaiveDate;

pub async fn store_summary_data (_data_version:&String, _data_date: &NaiveDate,_pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {
    //TO DO
    Ok(())
}