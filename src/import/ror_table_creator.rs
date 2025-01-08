use sqlx::{Pool, Postgres};
use log::info;

pub async fn recreate_ror_tables (pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {
    
    let schema_sql  = r#"SET client_min_messages TO WARNING; 
                               create schema if not exists ror;
                               SET client_min_messages TO NOTICE;"#;
    let _res = sqlx::raw_sql(schema_sql).execute(pool).await?;
        
    let table_sql  = r#"drop table if exists ror.core_data;
    create table ror.core_data
    (
          id                varchar     not null primary key 
        , ror_full_id       varchar     not null  
        , status            varchar     not null
        , established       int         null
    );"#;
    sqlx::raw_sql(table_sql).execute(pool).await?;


    let table_sql = r#"drop table if exists ror.admin_data;
    create table ror.admin_data
    (
          id                varchar     not null primary key
        , created           date        not null
        , cr_schema         varchar     not null
        , last_modified     date        not null
        , lm_schema         varchar     not null  
    );"#;
    sqlx::raw_sql(table_sql).execute(pool).await?;


    let table_sql = r#"drop table if exists ror.names;
    create table ror.names
    (  
          id                varchar     not null
        , value             varchar     not null  
        , name_type         varchar     not null
        , is_ror_name       bool        null
        , lang              varchar     null
    );
    create index src_names_idx on ror.names(id);"#;
    sqlx::raw_sql(table_sql).execute(pool).await?;


    let table_sql = r#"drop table if exists ror.locations;
    create table ror.locations
    (  
          id                varchar     not null
        , geonames_id       int         null
        , name              varchar     null	
        , lat               real        null
        , lng               real        null
        , cont_code         varchar     null
        , cont_name         varchar     null	    
        , country_code      varchar     null
        , country_name      varchar     null	
        , csubdiv_code      varchar     null
        , csubdiv_name      varchar     null	
    );
    create index src_locations_idx on ror.locations(id);"#;
    sqlx::raw_sql(table_sql).execute(pool).await?;


    let table_sql = r#"drop table if exists ror.external_ids;
    create table ror.external_ids
    (
          id                varchar     not null
        , id_type           varchar     not null
        , id_value          varchar     not null
        , is_preferred      bool        null
    );
    create index src_external_ids_idx on ror.external_ids(id);"#;
    sqlx::raw_sql(table_sql).execute(pool).await?;


    let table_sql = r#"drop table if exists ror.links;
    create table ror.links
    (
          id                varchar	    not null
        , link_type         varchar     not null
        , value             varchar     not null
    );
    create index src_links_idx on ror.links(id);"#;
    sqlx::raw_sql(table_sql).execute(pool).await?;


    let table_sql = r#"drop table if exists ror.type;
    create table ror.type
    (  
          id                varchar	    not null
        , org_type          varchar     not null
    ); 
    create index src_type_idx on ror.type(id);"#;
    sqlx::raw_sql(table_sql).execute(pool).await?;


    let table_sql = r#"drop table if exists ror.relationships;
    create table ror.relationships
    (
          id                varchar     not null
        , rel_type          varchar     not null
        , related_id        varchar     not null
        , related_label     varchar     not null
    ); 
    create index src_relationships_idx on ror.relationships(id);"#;
    sqlx::raw_sql(table_sql).execute(pool).await?;


    let table_sql = r#"drop table if exists ror.domains;
    create table ror.domains
    (
          id                varchar     not null
        , value             varchar     not null
    );
    create index src_domains_idx on ror.domains(id);"#;
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
    let sql = "SELECT COUNT(*) FROM ror.".to_owned() + table_name;
    let res: i64 = sqlx::query_scalar(&sql)
    .fetch_one(pool)
    .await?;
    info!("Total records in ror.{}: {}", table_name, res);
    Ok(())
}


