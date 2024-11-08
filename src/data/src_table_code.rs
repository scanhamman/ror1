use sqlx::{Pool, Postgres};
use sqlx::postgres::PgQueryResult; 
use log::info;

pub async fn recreate_src_tables (pool: &Pool<Postgres>) {
    let _ = create_coredata_table(pool).await;
    let _ = create_admindata_table(pool).await;
    let _ = create_names_table(pool).await;
    let _ = create_locations_table(pool).await;
    let _ = create_extids_table(pool).await;
    let _ = create_links_table(pool).await;
    let _ = create_orgtype_table(pool).await;
    let _ = create_relationships_table(pool).await;
    let _ = create_domains_table(pool).await;

    info!("Recreated source tables");
}


async fn create_coredata_table(pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {
    sqlx::raw_sql(r#"drop table if exists src.core_data;
    create table src.core_data
    (  
        id       			varchar  	not null primary key 
        , ror_full_id       varchar	  	not null  
        , status			varchar 	not null
        , established		int			null
    );"#).execute(pool).await
}


async fn create_admindata_table(pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {
    sqlx::raw_sql(r#"drop table if exists src.admin_data;
    create table src.admin_data
    (  
        id       			varchar  	not null primary key
        , created         	date     	not null
        , cr_schema			varchar    	not null
        , last_modified		date     	not null
        , lm_schema			varchar    	not null  
    );"#).execute(pool).await
}


async fn create_names_table(pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {
    sqlx::raw_sql(r#"drop table if exists src.names;
    create table src.names
    (  
        id           		varchar	  	not null
        , value    			varchar   	not null  
        , name_type        	varchar		not null
        , is_ror_name       bool        null
        , lang				varchar     null
    );
    create index src_names_idx on src.names(id);"#).execute(pool).await
}


async fn create_locations_table(pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {
    sqlx::raw_sql(r#"drop table if exists src.locations;
    create table src.locations
    (  
        id           		varchar	  	not null
        , geonames_id       int			null
        , name 				varchar		null	
        , lat				real		null
        , lng				real		null
        , country_code  	varchar		null
        , country_name  	varchar		null	
    );
    create index src_locations_idx on src.locations(id);"#).execute(pool).await
}


async fn create_extids_table(pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {
    sqlx::raw_sql(r#"drop table if exists src.external_ids;
    create table src.external_ids
    (  
        id           		varchar	  	not null
        , id_type          	varchar		not null
        , id_value         	varchar		not null
        , is_preferred	    bool        null
    );
    create index src_external_ids_idx on src.external_ids(id);"#).execute(pool).await
}


async fn create_links_table(pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {
    sqlx::raw_sql(r#"drop table if exists src.links;
    create table src.links
    (  
          id           		varchar	  	not null
        , link_type  	    varchar 	not null
        , value           	varchar	  	not null
    );
    create index src_links_idx on src.links(id);"#).execute(pool).await
}


async fn create_orgtype_table(pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {
    sqlx::raw_sql(r#"drop table if exists src.type;
    create table src.type
    (  
          id           		varchar	  	not null
        , org_type    	   	varchar 	not null
    ); 
    create index src_type_idx on src.type(id);"#).execute(pool).await
}

async fn create_relationships_table(pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {
    sqlx::raw_sql(r#"drop table if exists src.relationships;
    create table src.relationships
    (  
        id           		varchar	  	not null
        , rel_type  	   	varchar 	not null
        , related_id        varchar	  	not null
        , related_label     varchar	  	not null
    ); 
    create index src_relationships_idx on src.relationships(id);"#).execute(pool).await
}
  

async fn create_domains_table(pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {
    sqlx::raw_sql(r#"drop table if exists src.domains;
    create table src.domains
    (  
        id           		varchar	  	not null
        , value           	varchar		not null
    );
    create index src_domains_idx on src.domains(id);"#).execute(pool).await
}
