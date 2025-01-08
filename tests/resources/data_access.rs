use sqlx::postgres::PgPoolOptions;
use sqlx::{Postgres, Pool};
use std::env;
use ror1::error_defs::AppError;
use log::error;

use super::record_structs::{RorCoreData, RorRelationship, RorExternalId, 
                            RorName, RorLocation, RorLink, RorType, RorAdminData};

pub async fn fetch_db_pool(db_name: &str) -> Result<Pool<Postgres>, AppError>  {

    let _env_res  = match dotenv::from_filename("ror.env")
    {
        Ok(pb) => pb,
        Err(err) => return Err(AppError::DeErr(err)),
    };

    let host: String = env::var("db_host").unwrap_or("localhost".to_string());
    let user: String = env::var("db_user").unwrap_or("no user".to_string());
    let password: String = env::var("db_password").unwrap_or("no password".to_string());
    let port: String = env::var("db_port").unwrap_or("5432".to_string());

    let db_conn_string = format!("postgres://{}:{}@{}:{}/{}", user, password, host, port, db_name);
    
     let try_pool = PgPoolOptions::new()
                  .max_connections(5).connect(&db_conn_string).await;
     let pool = match try_pool {
        Ok(p) => Ok(p),
        Err(e) => {
            error!("An error occured while creating the DB pool: {}", e);
            error!("Check the DB credentials and confirm the database is available");
            return Err(AppError::SqErr(e))
        }, 
    };
    pool
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
