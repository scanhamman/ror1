use sqlx::{postgres::PgQueryResult, Pool, Postgres};


pub async fn summarise_data (pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {
 
    summarise_names (pool).await?;
    summarise_nametypes (pool).await?;
    summarise_namenulls (pool).await?;
    summarise_links (pool).await?;
    summarise_external_ids (pool).await?;
	summarise_types (pool).await?;
	summarise_locations (pool).await?;
    summarise_relationships (pool).await?;
    summarise_domains (pool).await?;

    Ok(())
}

async fn summarise_names (pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {

    let import_sql  = r#"update src.admin_data ad
          set n_names = n
          from (
              select id, count(id) as n
              from src.names 
              group by id) c
          where ad.id = c.id"#;
    let qry_res = sqlx::raw_sql(import_sql).execute(pool).await?;
    Ok(qry_res)
}
async fn summarise_nametypes (pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {
    let import_sql  = r#"update src.admin_data ad
          set n_labels = n
          from (
              select id, count(id) as n
              from src.names where name_type = 5
              group by id) c
          where ad.id = c.id;"#;
    sqlx::raw_sql(import_sql).execute(pool).await?;
    
    let import_sql  = r#"update src.admin_data ad
          set n_aliases = n
          from (
              select id, count(id) as n
              from src.names where name_type = 7
              group by id) c
          where ad.id = c.id;"#;
    sqlx::raw_sql(import_sql).execute(pool).await?;

    let import_sql  = r#"update src.admin_data ad
          set n_acronyms = n
          from (
              select id, count(id) as n
              from src.names where name_type = 10
              group by id) c
          where ad.id = c.id;"#;
    let qry_res= sqlx::raw_sql(import_sql).execute(pool).await?;
    Ok(qry_res)
}

async fn summarise_namenulls (pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {
    
    let import_sql  = r#"update src.admin_data ad
          set n_null_langs = n
          from (
              select id, count(id) as n
              from src.names 
              where lang_code is null and name_type <> 10
              group by id) c
          where ad.id = c.id;"#;
    let qry_res = sqlx::raw_sql(import_sql).execute(pool).await?;
    Ok(qry_res)
}

async fn summarise_external_ids (pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {
    
    let import_sql  = r#"update src.admin_data ad
          set n_isni = n
          from (
              select id, count(id) as n
              from src.external_ids 
              where id_type = 11
              group by id) c
          where ad.id = c.id;"#;
    sqlx::raw_sql(import_sql).execute(pool).await?;

    let import_sql  = r#"update src.admin_data ad
          set n_grid = n
          from (
              select id, count(id) as n
              from src.external_ids 
              where id_type = 13
              group by id) c
         where ad.id = c.id;"#;
    sqlx::raw_sql(import_sql).execute(pool).await?;
    
    let import_sql  = r#"update src.admin_data ad
          set n_fundref = n
          from (
              select id, count(id) as n
              from src.external_ids 
              where id_type = 14
              group by id) c
          where ad.id = c.id;"#;
    sqlx::raw_sql(import_sql).execute(pool).await?;

    let import_sql  = r#"update src.admin_data ad
          set n_wikidata = n
          from (
              select id, count(id) as n
              from src.external_ids 
              where id_type = 12
              group by id) c
          where ad.id = c.id;"#;
    let qry_res = sqlx::raw_sql(import_sql).execute(pool).await?;
    Ok(qry_res)
}

async fn summarise_links (pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {

    let import_sql  = r#"update src.admin_data ad
          set n_wikipedia = n
          from (
              select id, count(id) as n
              from src.links 
              where link_type = 21
              group by id) c
          where ad.id = c.id;"#;
    sqlx::raw_sql(import_sql).execute(pool).await?;

    let import_sql  = r#"update src.admin_data ad
          set n_website = n
          from (
              select id, count(id) as n
              from src.links 
              where link_type = 22
              group by id) c
              where ad.id = c.id;"#;
    let qry_res = sqlx::raw_sql(import_sql).execute(pool).await?;
    Ok(qry_res)
}

async fn summarise_types (pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {

    let import_sql  = r#"update src.admin_data ad
          set n_types = n
          from (
              select id, count(id) as n
              from src.type 
              group by id) c
          where ad.id = c.id;"#;
    let qry_res = sqlx::raw_sql(import_sql).execute(pool).await?;
    Ok(qry_res)
}
    
async fn summarise_locations (pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {

    let import_sql  = r#"update src.admin_data ad
          set n_locs = n
          from (
              select id, count(id) as n
              from src.locations 
              group by id) c
          where ad.id = c.id"#;
    let qry_res = sqlx::raw_sql(import_sql).execute(pool).await?;
    Ok(qry_res)
}

async fn summarise_relationships (pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {

    let import_sql  = r#"update src.admin_data ad
          set n_relrels = n
          from (
              select id, count(id) as n
              from src.relationships
              where rel_type = 3
              group by id) c
          where ad.id = c.id;"#;
    sqlx::raw_sql(import_sql).execute(pool).await?;

    let import_sql  = r#"update src.admin_data ad
          set n_parrels = n
          from (
              select id, count(id) as n
              from src.relationships
              where rel_type = 1
              group by id) c
          where ad.id = c.id;"#;
    sqlx::raw_sql(import_sql).execute(pool).await?;

    let import_sql  = r#"update src.admin_data ad
          set n_chrels = n
          from (
              select id, count(id) as n
              from src.relationships
              where rel_type = 2
              group by id) c
          where ad.id = c.id;"#;
   sqlx::raw_sql(import_sql).execute(pool).await?;


    let import_sql  = r#"update src.admin_data ad
      set n_sucrels = n
      from (
          select id, count(id) as n
          from src.relationships
          where rel_type = 5
          group by id) c
     where ad.id = c.id;"#;
    sqlx::raw_sql(import_sql).execute(pool).await?;


    let import_sql  = r#"update src.admin_data ad
         set n_predrels = n
         from (
             select id, count(id) as n
             from src.relationships
             where rel_type = 4
             group by id) c
         where ad.id = c.id;"#;
    let qry_res = sqlx::raw_sql(import_sql).execute(pool).await?;
    Ok(qry_res)

}

async fn summarise_domains (pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {

    let import_sql  = r#"update src.admin_data ad
          set n_doms = n
          from (
              select id, count(id) as n
              from src.domains 
          group by id) c
          where ad.id = c.id;"#;
    let qry_res = sqlx::raw_sql(import_sql).execute(pool).await?;
    Ok(qry_res)
}