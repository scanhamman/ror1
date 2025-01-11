use sqlx::{Pool, Postgres};
use crate::AppError;
use chrono::NaiveDate;


#[derive(sqlx::FromRow)]
pub struct DistribRow {
  pub vcode: String,
  pub vdate: NaiveDate,
  pub count: i32,
  pub num_of_orgs: i64,
  pub pc_of_orgs: f64,
}

#[derive(sqlx::FromRow)]
pub struct CountryRow {
  pub vcode: String,
  pub vdate: NaiveDate,
  pub country: String,
  pub num_of_locs: i64,
  pub pc_of_locs: f64,
}

#[derive(sqlx::FromRow)]
pub struct LangCodeRow {
  pub vcode: String,
  pub vdate: NaiveDate,
  pub lang: String,
  pub num_of_names: i64,
  pub pc_of_ne_names: f64,
  pub pc_of_all_names: f64,
}

#[derive(sqlx::FromRow)]
pub struct ScriptCodeRow {
  pub vcode: String,
  pub vdate: NaiveDate,
  pub script: String,
  pub num_of_names: i64,
  pub pc_of_nl_names: f64,
  pub pc_of_all_names: f64,
}


pub async fn get_count (sql_string: &str, pool: &Pool<Postgres>) -> Result<i64, AppError> {
    let res = sqlx::query_scalar(sql_string)
    .fetch_one(pool)
    .await?;
    Ok(res)
}

fn get_pc (top:i64, bottom:i64) -> f32 {
    if bottom == 0
    { 0.0 }
    else {
        let res = ((top as f32) * 100.0) / bottom as f32;
        (res * 100.0).round() / 100.0  // return number to 2 decimal places
    }
}

pub async fn delete_any_existing_data(data_version:&String, data_date: &NaiveDate,pool: &Pool<Postgres>) -> Result<(), AppError> {
   
    let where_clause = " WHERE vcode = \'".to_string() + data_version 
                                 + "\' AND vdate = \'" + &data_date.to_string() + "\'::date;";
    
    let del_sql = "DELETE from smm.version_summary ".to_string() + &where_clause
              + "DELETE from smm.name_summary "+ &where_clause
              + "DELETE from smm.name_ror " + &where_clause
              + "DELETE from smm.name_count_distribution " + &where_clause
              + "DELETE from smm.name_label_distribution " + &where_clause
              + "DELETE from smm.name_alias_distribution " + &where_clause
              + "DELETE from smm.name_acronym_distribution " + &where_clause
              + "DELETE from smm.ne_lang_code_distribution " + &where_clause
              + "DELETE from smm.nl_lang_script_distribution " + &where_clause
              + "DELETE from smm.orgs_of_type_summary " + &where_clause
              + "DELETE from smm.type_count_distribution " + &where_clause
              + "DELETE from smm.type_name_lang_code " + &where_clause
              + "DELETE from smm.ext_ids_summary " + &where_clause
              + "DELETE from smm.ext_ids_count_distribution " + &where_clause
              + "DELETE from smm.links_summary " + &where_clause
              + "DELETE from smm.links_count_distribution " + &where_clause
              + "DELETE from smm.relationships_summary " + &where_clause
              + "DELETE from smm.type_relationship " + &where_clause
              + "DELETE from smm.country_top_20_distribution " + &where_clause
              + "DELETE from smm.locs_count_distribution " + &where_clause;

   sqlx::raw_sql(&del_sql).execute(pool).await?;

    Ok(())

}


pub async fn store_name_summary(data_version:&String, data_date: &NaiveDate,pool: &Pool<Postgres>, num_names: i64) -> Result<(), AppError> {

    // let total_wolc = number of names without lang code
    // let pc_wolc = percentage of names without lang code
    // let num_label = number of labels 
    // let num_alias = number of aliases
    // let num_acronym = number of acronyms
    // let pc_label = labels as percentage of total
    // let pc_alias = aliases as percentage of total
    // let pc_acronym = acronyms as percentage of total

    // let num_label_wolc = number of labels without lang code
    // let num_alias_wolc = number of aliases without lang code
    // let num_acro_wolc = number of acronyms without lang code
    // let num_nacro_wolc = number of non-acronyms without lang code
    // let pc_label_wolc = labels without lang code as percentage of total labels
    // let pc_alias_wolc = aliases without lang code as percentage of total aliases
    // let pc_acro_wolc = acronyms without lang code as percentage of total acronyms
    // let pc_nacro_wolc = non-acronyms without lang code as percentage of total non-acronyms

    let total_wolc = get_count("select count(*) from src.names where lang_code is null", pool).await?;
    let pc_wolc = get_pc (total_wolc,num_names);
    let num_label = get_count("select count(*) from src.names where name_type = 5", pool).await?;
    let num_alias = get_count("select count(*) from src.names where name_type = 7", pool).await?;
    let num_acronym = get_count("select count(*) from src.names where name_type = 10", pool).await?;
    // let num nacro
    let pc_label = get_pc (num_label,num_names);
    let pc_alias = get_pc (num_alias,num_names);
    let pc_acronym = get_pc (num_acronym,num_names);
    // let pc_nacro
    let num_label_wolc = get_count(r#"select count(*) from src.names 
                                                     where name_type = 5 and lang_code is null"#, pool).await?;
    let num_alias_wolc = get_count(r#"select count(*) from src.names 
                                                     where name_type = 7 and lang_code is null"#, pool).await?;
    let num_acro_wolc = get_count(r#"select count(*) from src.names 
                                                     where name_type = 10 and lang_code is null"#, pool).await?;
    let num_nacro_wolc = get_count(r#"select count(*) from src.names 
                                                     where (name_type = 5 or name_type = 7) and lang_code is null"#, pool).await?;
    let pc_label_wolc = get_pc (num_label_wolc,num_label);
    let pc_alias_wolc = get_pc (num_alias_wolc,num_alias);
    let pc_acro_wolc = get_pc (num_acro_wolc,num_acronym);
    let pc_nacro_wolc = get_pc (num_nacro_wolc,num_label + num_alias);
    // let num_nacro_ne      int         null
    // let pc_nacro_ne       real        null
    // let num_nltn          int         null
    // let pc_nltn           real        null

    let sql = r#"INSERT into smm.name_summary (vcode, vdate, total, total_wolc,
    pc_wolc, num_label, num_alias, num_acronym, pc_label, pc_alias, pc_acronym,
    num_label_wolc, num_alias_wolc, num_acro_wolc, num_nacro_wolc, pc_label_wolc,
    pc_alias_wolc, pc_acro_wolc, pc_nacro_wolc)
    values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)"#;

    sqlx::query(sql)
    .bind(data_version).bind(data_date).bind(num_names)
    .bind(total_wolc).bind(pc_wolc)
    .bind(num_label).bind(num_alias).bind(num_acronym)
    .bind(pc_label).bind(pc_alias).bind(pc_acronym)
    .bind(num_label_wolc).bind(num_alias_wolc).bind(num_acro_wolc).bind(num_nacro_wolc)
    .bind(pc_label_wolc).bind(pc_alias_wolc).bind(pc_acro_wolc).bind(pc_nacro_wolc)
    .execute(pool)
    .await?;

    Ok(())
    
  }

  
  pub async fn store_name_ror(data_version:&String, data_date: &NaiveDate,pool: &Pool<Postgres>, num_orgs: i64) -> Result<(), AppError> {

    // name_ror
    
    // num_label_ror =  // labels that are also ror names
    // num_label_nror =  // labels that are not ror names
    // num_nlabel_ror =  // ror names that are not labels
    // pc_nlabel_ror = // percentage ror names that are not labels
    // num_en_ror =  // ror names that are english
    // num_nen_ror =  // ror names that are not english
    // num_wolc_ror =  // ror names that are not english
    // pc_en_ror = // pecentage ror names that are not english
    // pc_nen_ror = // pecentage ror names that are not english
    // pc_wolc_ror = // pecentage ror names that have no lang code

    let num_label_ror = get_count(r#"select count(*) from src.names 
                                                       where name_type = 5 and is_ror_name = true"#, pool).await?; 
    let num_label_nror = get_count(r#"select count(*) from src.names 
                                                       where name_type = 5 and is_ror_name = false"#, pool).await?; 
    let num_nlabel_ror = get_count(r#"select count(*) from src.names 
                                                       where name_type <> 5 and is_ror_name = true"#, pool).await?; 
    let pc_nlabel_ror = get_pc(num_nlabel_ror, num_orgs);                                                   
    let num_en_ror = get_count(r#"select count(*) from src.names 
                                                       where is_ror_name = true and lang_code = 'en'"#, pool).await?;                                                    
    let num_nen_ror = get_count(r#"select count(*) from src.names 
                                                        where is_ror_name = true and lang_code <> 'en' and lang_code is not null"#, pool).await?; 
    let num_wolc_ror = get_count(r#"select count(*) from src.names 
                                                        where is_ror_name = true and lang_code is null"#, pool).await?; 
    let pc_en_ror = get_pc(num_en_ror, num_orgs);
    let pc_nen_ror = get_pc(num_nen_ror, num_orgs);
    let pc_wolc_ror = get_pc(num_wolc_ror, num_orgs); 

    let sql = r#"INSERT into smm.name_ror (vcode, vdate, num_label_ror, num_label_nror,
    num_nlabel_ror, pc_nlabel_ror, num_en_ror, num_nen_ror, num_wolc_ror, pc_en_ror, pc_nen_ror, pc_wolc_ror)
    values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"#;

    sqlx::query(sql)
    .bind(data_version).bind(data_date).bind(num_label_ror)
    .bind(num_label_nror).bind(num_nlabel_ror).bind(pc_nlabel_ror)
    .bind(num_en_ror).bind(num_nen_ror).bind(num_wolc_ror)
    .bind(pc_en_ror).bind(pc_nen_ror).bind(pc_wolc_ror)
    .execute(pool)
    .await?;

    Ok(())

  }


  pub async fn store_name_count_distrib(dv_dt: String, pool: &Pool<Postgres>, num_orgs: i64) -> Result<(), AppError> {

    let sql = dv_dt + r#"n_names as count, count(id) as num_of_orgs, 
                       round(count(id) * 10000 :: float / "# + &num_orgs.to_string() + r#":: float)/100 :: float as pc_of_orgs
                       from src.admin_data
                       group by n_names
                       order by n_names;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;

    for r in rows {
        sqlx::query(r#"INSERT INTO smm.name_count_distribution (vcode, vdate, count, num_of_orgs, pc_of_orgs) 
                            values($1, $2, $3, $4, $5)"#)
        .bind(r.vcode)
        .bind(r.vdate)
        .bind(r.count)
        .bind(r.num_of_orgs)
        .bind(r.pc_of_orgs)
        .execute(pool)
        .await?;
    }
    
    Ok(())
  }

  pub async fn store_label_count_distrib(dv_dt: String, pool: &Pool<Postgres>, num_orgs: i64) -> Result<(), AppError> {

    let sql = dv_dt + r#"n_labels as count, count(id) as num_of_orgs, 
                       round(count(id) * 10000 :: float / "# + &num_orgs.to_string() + r#":: float)/100 :: float as pc_of_orgs
                       from src.admin_data where n_labels <> 0
                       group by n_labels
                       order by n_labels;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;

    for r in rows {
        sqlx::query(r#"INSERT INTO smm.name_label_distribution (vcode, vdate, count, num_of_orgs, pc_of_orgs) 
                            values($1, $2, $3, $4, $5)"#)
        .bind(r.vcode)
        .bind(r.vdate)
        .bind(r.count)
        .bind(r.num_of_orgs)
        .bind(r.pc_of_orgs)
        .execute(pool)
        .await?;
    }
    
    Ok(())

  }

  pub async fn store_alias_count_distrib(dv_dt: String, pool: &Pool<Postgres>, num_orgs: i64) -> Result<(), AppError> {

    let sql = dv_dt + r#"n_aliases as count, count(id) as num_of_orgs, 
                       round(count(id) * 10000 :: float / "# + &num_orgs.to_string() + r#":: float)/100 :: float as pc_of_orgs
                       from src.admin_data where n_aliases <> 0
                       group by n_aliases
                       order by n_aliases;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;

    for r in rows {
        sqlx::query(r#"INSERT INTO smm.name_alias_distribution (vcode, vdate, count, num_of_orgs, pc_of_orgs) 
                            values($1, $2, $3, $4, $5)"#)
        .bind(r.vcode)
        .bind(r.vdate)
        .bind(r.count)
        .bind(r.num_of_orgs)
        .bind(r.pc_of_orgs)
        .execute(pool)
        .await?;
    }
    Ok(())

  }

  pub async fn store_acronym_count_distrib(dv_dt: String, pool: &Pool<Postgres>, num_orgs: i64) -> Result<(), AppError> {

    let sql = dv_dt + r#"n_acronyms as count, count(id) as num_of_orgs, 
                       round(count(id) * 10000 :: float / "# + &num_orgs.to_string() + r#":: float)/100 :: float as pc_of_orgs
                       from src.admin_data where n_acronyms <> 0
                       group by n_acronyms
                       order by n_acronyms;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;

    for r in rows {
        sqlx::query(r#"INSERT INTO smm.name_acronym_distribution (vcode, vdate, count, num_of_orgs, pc_of_orgs) 
                            values($1, $2, $3, $4, $5)"#)
        .bind(r.vcode)
        .bind(r.vdate)
        .bind(r.count)
        .bind(r.num_of_orgs)
        .bind(r.pc_of_orgs)
        .execute(pool)
        .await?;
    }
    
    Ok(())

  }

  pub async fn store_type_summary(data_version:&String, data_date: &NaiveDate, pool: &Pool<Postgres>, num_orgs: i64) -> Result<(), AppError> {
  
      let government =  get_count("select count(*) from src.type where org_type = 100", pool).await?;   // num of orgs with type government
      let education =  get_count("select count(*) from src.type where org_type = 200", pool).await?;       // num of orgs with type education
      let healthcare =  get_count("select count(*) from src.type where org_type = 300", pool).await?;       // num of orgs with type healthcare
      let company =  get_count("select count(*) from src.type where org_type = 400", pool).await?;          // num of orgs with type company
      let nonprofit =  get_count("select count(*) from src.type where org_type = 500", pool).await?;        // num of orgs with type nonprofit
      let funder =  get_count("select count(*) from src.type where org_type = 600", pool).await?;           // num of orgs with type funder
      let facility=  get_count("select count(*) from src.type where org_type = 700", pool).await?;          // num of orgs with type facility
      let archive=  get_count("select count(*) from src.type where org_type = 800", pool).await?;           // num of orgs with type archive
      let other =  get_count("select count(*) from src.type where org_type = 900", pool).await?;            // num of orgs with type other
      let government_pc =  get_pc (government,num_orgs);   // pc of orgs with type government
      let education_pc =  get_pc (education,num_orgs);      // pc of orgs with type education
      let healthcare_pc =  get_pc (healthcare,num_orgs);     // pc of orgs with type healthcare
      let company_pc =  get_pc (company,num_orgs);        // pc of orgs with type company
      let nonprofit_pc =  get_pc (nonprofit,num_orgs);      // pc of orgs with type nonprofit
      let funder_pc =  get_pc (funder,num_orgs);         // pc of orgs with type funder
      let facility_pc =  get_pc (facility,num_orgs);       // pc of orgs with type facility
      let archive_pc =  get_pc (archive,num_orgs);        // pc of orgs with type archive
      let other_pc =  get_pc (other,num_orgs);          // pc of orgs with type other

     
      let sql = r#"INSERT into smm.orgs_of_type_summary (vcode, vdate, num_orgs, government,
      education, healthcare, company, nonprofit, funder, facility, archive, other,
      government_pc, education_pc, healthcare_pc, company_pc, nonprofit_pc, funder_pc, 
      facility_pc, archive_pc, other_pc)
      values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)"#;

      sqlx::query(sql)
      .bind(data_version).bind(data_date).bind(num_orgs)
      .bind(government).bind(education).bind(healthcare).bind(company)
      .bind(nonprofit).bind(funder).bind(facility).bind(archive).bind(other)
      .bind(government_pc).bind(education_pc).bind(healthcare_pc).bind(company_pc)
      .bind(nonprofit_pc).bind(funder_pc).bind(facility_pc).bind(archive_pc).bind(other_pc)
      .execute(pool)
      .await?;
Ok(())

}
  

pub async fn store_type_count_distrib(dv_dt: String, pool: &Pool<Postgres>, num_orgs: i64) -> Result<(), AppError> {

  let sql = dv_dt + r#"n_types as count, count(id) as num_of_orgs, 
                     round(count(id) * 10000 :: float / "# + &num_orgs.to_string() + r#":: float)/100 :: float as pc_of_orgs
                     from src.admin_data
                     group by n_types
                     order by n_types;"#;
  let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;

  for r in rows {
      sqlx::query(r#"INSERT INTO smm.type_count_distribution (vcode, vdate, count, num_of_orgs, pc_of_orgs) 
                          values($1, $2, $3, $4, $5)"#)
      .bind(r.vcode)
      .bind(r.vdate)
      .bind(r.count)
      .bind(r.num_of_orgs)
      .bind(r.pc_of_orgs)
      .execute(pool)
      .await?;
  }
  
  Ok(())
}


pub async fn store_ext_ids_count_distrib(dv_dt: String, pool: &Pool<Postgres>, num_orgs: i64) -> Result<(), AppError> {

  let sql = dv_dt + r#"n_ext_ids as count, count(id) as num_of_orgs, 
                     round(count(id) * 10000 :: float / "# + &num_orgs.to_string() + r#":: float)/100 :: float as pc_of_orgs
                     from src.admin_data where n_ext_ids <> 0
                     group by n_ext_ids
                     order by n_ext_ids;"#;
  let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;

  for r in rows {
      sqlx::query(r#"INSERT INTO smm.ext_ids_count_distribution (vcode, vdate, count, num_of_orgs, pc_of_orgs) 
                          values($1, $2, $3, $4, $5)"#)
      .bind(r.vcode)
      .bind(r.vdate)
      .bind(r.count)
      .bind(r.num_of_orgs)
      .bind(r.pc_of_orgs)
      .execute(pool)
      .await?;
  }
  
  Ok(())
}


pub async fn store_links_count_distrib(dv_dt: String, pool: &Pool<Postgres>, num_orgs: i64) -> Result<(), AppError> {

  let sql = dv_dt + r#"n_links as count, count(id) as num_of_orgs, 
                     round(count(id) * 10000 :: float / "# + &num_orgs.to_string() + r#":: float)/100 :: float as pc_of_orgs
                     from src.admin_data
                     group by n_links
                     order by n_links;"#;
  let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;

  for r in rows {
      sqlx::query(r#"INSERT INTO smm.links_count_distribution (vcode, vdate, count, num_of_orgs, pc_of_orgs) 
                          values($1, $2, $3, $4, $5)"#)
      .bind(r.vcode)
      .bind(r.vdate)
      .bind(r.count)
      .bind(r.num_of_orgs)
      .bind(r.pc_of_orgs)
      .execute(pool)
      .await?;
  }
  
  Ok(())
}


pub async fn store_country_top_20_distrib(dv_dt: String, pool: &Pool<Postgres>, num_locs: i64) -> Result<(), AppError> {

  // At the moment num_of_locs = num_of_orgs - might change!

  let sql = dv_dt + r#"country_name as country, count(country_name) as num_of_locs, 
                     round(count(country_name) * 10000 :: float / "# + &num_locs.to_string() + r#":: float)/100 :: float as pc_of_locs
                     from src.locations
                     group by country_name
                     order by count(country_name) desc
                     LIMIT 20;"#;
  let rows: Vec<CountryRow> = sqlx::query_as(&sql).fetch_all(pool).await?;

  for r in rows {
      sqlx::query(r#"INSERT INTO smm.country_top_20_distribution (vcode, vdate, country, num_of_locs, pc_of_locs) 
                          values($1, $2, $3, $4, $5)"#)
      .bind(r.vcode)
      .bind(r.vdate)
      .bind(r.country)
      .bind(r.num_of_locs)
      .bind(r.pc_of_locs)
      .execute(pool)
      .await?;
  }
  
  Ok(())
}


pub async fn store_locs_count_distrib(dv_dt: String, pool: &Pool<Postgres>, num_orgs: i64) -> Result<(), AppError> {

  let sql = dv_dt + r#"n_locs as count, count(id) as num_of_orgs, 
                     round(count(id) * 10000 :: float / "# + &num_orgs.to_string() + r#":: float)/100 :: float as pc_of_orgs
                     from src.admin_data
                     group by n_locs
                     order by n_locs;"#;
  let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;

  for r in rows {
      sqlx::query(r#"INSERT INTO smm.locs_count_distribution (vcode, vdate, count, num_of_orgs, pc_of_orgs) 
                          values($1, $2, $3, $4, $5)"#)
      .bind(r.vcode)
      .bind(r.vdate)
      .bind(r.count)
      .bind(r.num_of_orgs)
      .bind(r.pc_of_orgs)
      .execute(pool)
      .await?;
  }
  
  Ok(())
}