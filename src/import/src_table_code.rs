use sqlx::{Pool, Postgres};
use log::info;

pub async fn recreate_src_tables (pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {

    let table_sql  = r#"drop table if exists src.core_data;
    create table src.core_data
    (
          id                varchar     not null primary key 
        , ror_full_id       varchar     not null  
        , status            varchar     not null
        , established       int         null
    );"#;
    sqlx::raw_sql(table_sql).execute(pool).await?;


    let table_sql = r#"drop table if exists src.admin_data;
    create table src.admin_data
    (
          id                varchar     not null primary key
        , created           date        not null
        , cr_schema         varchar     not null
        , last_modified     date        not null
        , lm_schema         varchar     not null  
    );"#;
    sqlx::raw_sql(table_sql).execute(pool).await?;
    

    let table_sql = r#"drop table if exists src.names;
    create table src.names
    (  
          id                varchar     not null
        , value             varchar     not null  
        , name_type         varchar     not null
        , is_ror_name       bool        null
        , lang              varchar     null
    );
    create index src_names_idx on src.names(id);"#;
    sqlx::raw_sql(table_sql).execute(pool).await?;
    

    let table_sql = r#"drop table if exists src.locations;
    create table src.locations
    (  
          id                varchar     not null
        , geonames_id       int         null
        , name              varchar     null	
        , lat               real        null
        , lng               real        null
        , country_code      varchar     null
        , country_name      varchar     null	
    );
    create index src_locations_idx on src.locations(id);"#;
    sqlx::raw_sql(table_sql).execute(pool).await?;
    

    let table_sql = r#"drop table if exists src.external_ids;
    create table src.external_ids
    (  
          id                varchar     not null
        , id_type           varchar     not null
        , id_value          varchar     not null
        , is_preferred      bool        null
    );
    create index src_external_ids_idx on src.external_ids(id);"#;
    sqlx::raw_sql(table_sql).execute(pool).await?;
    

    let table_sql = r#"drop table if exists src.links;
    create table src.links
    (  
          id                varchar	    not null
        , link_type         varchar     not null
        , value             varchar	    not null
    );
    create index src_links_idx on src.links(id);"#;
    sqlx::raw_sql(table_sql).execute(pool).await?;
    

    let table_sql = r#"drop table if exists src.type;
    create table src.type
    (  
          id                varchar	    not null
        , org_type          varchar     not null
    ); 
    create index src_type_idx on src.type(id);"#;
    sqlx::raw_sql(table_sql).execute(pool).await?;
    

    let table_sql = r#"drop table if exists src.relationships;
    create table src.relationships
    (
          id                varchar	    not null
        , rel_type          varchar     not null
        , related_id        varchar	    not null
        , related_label     varchar	    not null
    ); 
    create index src_relationships_idx on src.relationships(id);"#;
    sqlx::raw_sql(table_sql).execute(pool).await?;
    

    let table_sql = r#"drop table if exists src.domains;
    create table src.domains
    (  
          id                varchar	    not null
        , value             varchar     not null
    );
    create index src_domains_idx on src.domains(id);"#;
    sqlx::raw_sql(table_sql).execute(pool).await?;

    Ok(())

}


pub async fn log_record_nums (pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {
  
  info!("");
  info!("************************************");
  info!("Total record numbers for each table:");
  info!("************************************");
  info!("");

  write_record_num("core_data", pool).await?;
  write_record_num("admin_data", pool).await?;
  write_record_num("names", pool).await?;
  write_record_num("locations", pool).await?;
  write_record_num("external_ids", pool).await?;
  write_record_num("links", pool).await?;
  write_record_num("type", pool).await?;
  write_record_num("relationships", pool).await?;
  write_record_num("domains", pool).await?;
  
  info!("");
  info!("************************************");
  info!("");
 
  Ok(())
}

pub async fn write_record_num (table_name: &str, pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {
    let sql = "SELECT COUNT(*) FROM src.".to_owned() + table_name;
    let res: i64 = sqlx::query_scalar(&sql)
    .fetch_one(pool)
    .await?;
    info!("Total records in src.{}: {}", table_name, res);
    Ok(())
}
