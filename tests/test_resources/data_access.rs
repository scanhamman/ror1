//use sqlx::postgres::PgPoolOptions;
use sqlx::{Postgres, Pool};
//use std::env;
use ror1::error_defs::AppError;
use ror1::setup::get_db_pool;
use ror1::setup::env_reader;
//use log::error;

use super::record_structs::{RorCoreData, RorRelationship, RorExternalId, 
                            RorName, RorLocation, RorLink, RorType, RorAdminData};

pub async fn fetch_db_pool() -> Result<Pool<Postgres>, AppError>  {
    
    // Use the process set up in the library under test
    // Helps to ensure exactly the same database connections are used

    env_reader::populate_env_vars()?; 
    get_db_pool().await
}

pub async fn fetch_record_num (table_name: &str, pool: &Pool<Postgres>) -> i64 {
    let sql = "SELECT COUNT(*) FROM ror.".to_owned() + table_name;
    sqlx::query_scalar(&sql).fetch_one(pool).await.unwrap()
}

pub async fn fetch_first_record_id (pool: &Pool<Postgres>) -> String {
    let sql = "SELECT id FROM ror.core_data order by id LIMIT 1;".to_owned();
    sqlx::query_scalar(&sql).fetch_one(pool).await.unwrap()
}

pub async fn fetch_last_record_id (pool: &Pool<Postgres>) -> String {
    let sql = "SELECT id FROM ror.core_data order by id desc LIMIT 1;".to_owned();
    sqlx::query_scalar(&sql).fetch_one(pool).await.unwrap()
}

pub async fn fetch_core_data_record (id: &str, pool: &Pool<Postgres>) -> RorCoreData {
    let sql: &str  = "select * from ror.core_data where id = $1";
    sqlx::query_as(sql)
        .bind(id)
        .fetch_one(pool).await.unwrap()
}

pub async fn fetch_admin_data_record (id: &str, pool: &Pool<Postgres>) -> RorAdminData {
    let sql: &str  = "select * from ror.admin_data where id = $1";
    sqlx::query_as(sql)
        .bind(id)
        .fetch_one(pool).await.unwrap()
}

pub async fn fetch_relationship_records (id: &str, pool: &Pool<Postgres>) -> Vec<RorRelationship> {
    let sql: &str  = "select * from ror.relationships where id = $1 order by related_id";
    sqlx::query_as(sql)
        .bind(id)
        .fetch_all(pool).await.unwrap()
}

pub async fn fetch_external_id_records (id: &str, pool: &Pool<Postgres>) -> Vec<RorExternalId> {
    let sql: &str  = "select * from ror.external_ids where id = $1 order by id_value";
    sqlx::query_as(sql)
        .bind(id)
        .fetch_all(pool).await.unwrap()
}

pub async fn fetch_location_records (id: &str, pool: &Pool<Postgres>) -> Vec<RorLocation> {
    let sql: &str  = "select * from ror.locations where id = $1 order by geonames_id";
    sqlx::query_as(sql)
        .bind(id)
        .fetch_all(pool).await.unwrap()
}

pub async fn fetch_link_records (id: &str, pool: &Pool<Postgres>) -> Vec<RorLink> {
    let sql: &str  = "select * from ror.links where id = $1 order by value";
    sqlx::query_as(sql)
        .bind(id)
        .fetch_all(pool).await.unwrap()
}

pub async fn fetch_type_records (id: &str, pool: &Pool<Postgres>) -> Vec<RorType> {
    let sql: &str  = "select * from ror.type where id = $1 order by type";
    sqlx::query_as(sql)
        .bind(id)
        .fetch_all(pool).await.unwrap()
}

pub async fn fetch_name_records (id: &str, pool: &Pool<Postgres>) -> Vec<RorName> {
    let sql: &str  = "select * from ror.names where id = $1 order by value";
    sqlx::query_as(sql)
        .bind(id)
        .fetch_all(pool).await.unwrap()
}
