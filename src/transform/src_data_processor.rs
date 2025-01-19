use sqlx::{postgres::PgQueryResult, Pool, Postgres};


pub async fn store_org_attribute_numbers (pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {
    
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
    sqlx::raw_sql(import_sql).execute(pool).await?;

    let import_sql  = r#"update src.admin_data ad
          set n_nacro = n_names - n_acronyms;"#;
    let qry_res= sqlx::raw_sql(import_sql).execute(pool).await?;

    Ok(qry_res)
}

async fn summarise_namenulls (pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {
    
    let import_sql  = r#"update src.admin_data ad
          set n_names_wolc = n
          from (
              select id, count(id) as n
              from src.names 
              where lang_code is null
              group by id) c
          where ad.id = c.id;"#;
    sqlx::raw_sql(import_sql).execute(pool).await?;

    let import_sql  = r#"update src.admin_data ad
          set n_nacro_wolc = n
          from (
              select id, count(id) as n
              from src.names 
              where lang_code is null and name_type <> 10
              group by id) c
          where ad.id = c.id;"#;
    sqlx::raw_sql(import_sql).execute(pool).await?;

    let import_sql  = r#"update src.admin_data ad
          set n_is_company = 1
          from src.type t
          where ad.id = t.id
          and t.org_type = 400;"#;
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
    sqlx::raw_sql(import_sql).execute(pool).await?;

    let add_sql  = r#"update src.admin_data
              set n_ext_ids = n_isni + n_grid + n_fundref + n_wikidata"#;
    let qry_res = sqlx::raw_sql(add_sql).execute(pool).await?;

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
    sqlx::raw_sql(import_sql).execute(pool).await?;

    let add_sql  = r#"update src.admin_data
              set n_links = n_wikipedia + n_website"#;
    let qry_res = sqlx::raw_sql(add_sql).execute(pool).await?;

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

pub async fn add_script_codes (pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {

    // Examines the names and looks at the Unicode value of its first character. Uses that to 
    // determine the script (but checks for leading bracket - if present use the second character)
    
    #[derive(sqlx::FromRow)]
    struct Script {
        code: String,
        ascii_start: i32, 
        ascii_end: i32,
    }

    // Get the Unicode scripts with their ascii code boundaries.

    let sql  = r#"select code, ascii_start, ascii_end
    from lup.lang_scripts
    where ascii_end <> 0
    order by ascii_start;"#;
    let rows: Vec<Script> = sqlx::query_as(sql).fetch_all(pool).await?;

    // Update names records by testing against each unicode entry.

    for r in rows {
        
        sqlx::query(r#"update src.names
        set script_code = $1 
        where ascii(substr(value, 1, 1)) >= $2
        and   ascii(substr(value, 1, 1)) <= $3"#)
        .bind(r.code.clone())
        .bind(r.ascii_start)
        .bind(r.ascii_end)
        .execute(pool)
        .await?;
        
        // Correct for any bracketed names

        sqlx::query(r#"update src.names
        set script_code = $1 
        where ascii(substr(value, 2, 1)) >= $2
        and   ascii(substr(value, 2, 1)) <= $3
        and substr(value, 1, 1) = '('"#)
        .bind(r.code)
        .bind(r.ascii_start)
        .bind(r.ascii_end)
        .execute(pool)
        .await?;
    }

    Ok(())
 
}
