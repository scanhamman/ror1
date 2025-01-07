use sqlx::{Pool, Postgres};

pub async fn recreate_smm_tables (_pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {
    
    let schema_sql  = r#"create schema if not exists smm"#;
    sqlx::raw_sql(schema_sql).execute(_pool).await?;

    Ok(())
}