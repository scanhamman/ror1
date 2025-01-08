use sqlx::postgres::PgPoolOptions;
use sqlx::{Postgres, Pool};
use std::env;
use ror1::error_defs::AppError;
use log::error;

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

//pub async fn fetch_record (sql_string: &str, pool: &Pool<Postgres>) -> Vec<PgRow> {
//    sqlx::query(CoreData, sql_string).fetch_all(pool).await.unwrap()
//}

    