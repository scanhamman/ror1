use sqlx::{postgres::PgQueryResult, Pool, Postgres};

pub async fn import_data (pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {

    import_to_core_data (pool).await?;
    update_core_data_locations (pool).await?;
    import_admin_data_base (pool).await?;
    import_names (pool).await?;
    import_links (pool).await?;
    import_domains (pool).await?;
    import_external_ids (pool).await?;
    import_types (pool).await?;
    import_locations (pool).await?;
    import_relationships (pool).await?;
    Ok(())
}

async fn import_to_core_data (pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {

    let import_sql  = r#"insert into src.core_data (id, ror_full_id, 
          ror_name, status, established)
          select c.id, c.ror_full_id, m.value, c.status, c.established 
          from ror.core_data c
          inner join
              (select id, value from ror.names where is_ror_name = true) m
          on c.id = m.id;"#;
    let qry_res = sqlx::raw_sql(import_sql).execute(pool).await?;
    Ok(qry_res)
}

async fn update_core_data_locations (pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {

    let import_sql  = r#"update src.core_data c
          set location = t.name,
          csubdiv_code = t.country_subdivision_code,
          country_code = t.country_code
          from ror.locations t
          where c.id = t.id;"#;
    let qry_res = sqlx::raw_sql(import_sql).execute(pool).await?;
    Ok(qry_res)
}

async fn import_admin_data_base (pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {

    let import_sql  = r#"insert into src.admin_data(id, ror_name, created, cr_schema, 
          last_modified, lm_schema)
          select a.id, c.ror_name, a.created, a.cr_schema, a.last_modified, a.lm_schema 
          from ror.admin_data a
          inner join src.core_data c
          on a.id = c.id;"#;
    let qry_res = sqlx::raw_sql(import_sql).execute(pool).await?;
    Ok(qry_res)
}

async fn import_names (pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {

    let import_sql  = r#"insert into src.names(id, value, name_type, 
          is_ror_name, lang_code)
          select id, value, 
          case 
              when name_type = 'alias' then 7
              when name_type = 'acronym' then 10
              when name_type = 'label' then 5
              else 0
          end,
          case
              when is_ror_name = true then true
              else false
          end, 
          lang
          from ror.names a;"#;
    let qry_res = sqlx::raw_sql(import_sql).execute(pool).await?;
    Ok(qry_res)
}

async fn import_links (pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {

    let import_sql  = r#"insert into src.links(id, ror_name, link_type, link)
          select a.id, c.ror_name, 
          case 
              when a.link_type = 'wikipedia' then 21
              when a.link_type = 'website' then 22
              else 0
          end, 
          value
          from ror.links a
          inner join src.core_data c
        on a.id = c.id;"#;
    let qry_res = sqlx::raw_sql(import_sql).execute(pool).await?;
    Ok(qry_res)
}

async fn import_domains (pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {

    let import_sql  = r#"insert into src.domains(id, ror_name, domain)
          select a.id, c.ror_name, a.value
          from ror.domains a
          inner join src.core_data c
          on a.id = c.id;"#;
    let qry_res = sqlx::raw_sql(import_sql).execute(pool).await?;
    Ok(qry_res)
}

async fn import_external_ids (pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {

    let import_sql  = r#"insert into src.external_ids(id, ror_name, id_type, id_value, is_preferred)
          select a.id, c.ror_name,
          case 
              when id_type = 'isni' then 11
              when id_type = 'wikidata' then 12
              when id_type = 'grid' then 13
              when id_type = 'fundref' then 14
              else 0
          end,
          a.id_value, 
          case
              when a.is_preferred = true then true
              else false
          end
          from ror.external_ids a
          inner join src.core_data c
          on a.id = c.id;"#;
    let qry_res = sqlx::raw_sql(import_sql).execute(pool).await?;
    Ok(qry_res)
}

async fn import_types (pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {

    let import_sql  = r#"insert into src.type(id, ror_name, org_type)
          select a.id, c.ror_name, 
          case 
              when org_type = 'government' then 100
              when org_type = 'education' then 200
              when org_type = 'healthcare' then 300
              when org_type = 'company' then 400
              when org_type = 'nonprofit' then 500
              when org_type = 'funder' then 600
              when org_type = 'facility' then 700
              when org_type = 'archive' then 800
              when org_type = 'other' then 900
              else 0
          end
          from ror.type a
          inner join src.core_data c
          on a.id = c.id;"#;
    let qry_res = sqlx::raw_sql(import_sql).execute(pool).await?;
    Ok(qry_res)
}

async fn import_locations (pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {

    let import_sql  = r#"insert into src.locations(id, ror_name, geonames_id, 
          geonames_name, lat, lng, cont_code, cont_name, 
          country_code, country_name, csubdiv_code, csubdiv_name)
          select a.id, c.ror_name, a.geonames_id, a.name,
                 a.lat, a.lng, a.continent_code, a.continent_name, 
                 a.country_code, a.country_name, 
                 a.country_subdivision_code, a.country_subdivision_name
          from ror.locations a
          inner join src.core_data c
          on a.id = c.id;"#;
    let qry_res = sqlx::raw_sql(import_sql).execute(pool).await?;
    Ok(qry_res)
}

async fn import_relationships (pool: &Pool<Postgres>) -> Result<PgQueryResult, sqlx::Error> {

    let import_sql  = r#"insert into src.relationships(id, ror_name, rel_type, related_id, related_name)
          select a.id, c.ror_name, 
          case 
              when a.rel_type = 'parent' then 1
              when a.rel_type = 'child' then 2
              when a.rel_type = 'related' then 3
              when a.rel_type = 'predecessor' then 4
              when a.rel_type = 'successor' then 5
              else 0
          end, 
          a.related_id, a.related_label
          from ror.relationships a
          inner join src.core_data c
          on a.id = c.id;"#;
    let qry_res = sqlx::raw_sql(import_sql).execute(pool).await?;
    Ok(qry_res)
}
