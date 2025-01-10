use sqlx::{Pool, Postgres};
use crate::AppError;
use chrono::NaiveDate;


#[derive(sqlx::FromRow)]
pub struct DistribRow {
  pub vcode: String,
  pub vdate: NaiveDate,
  pub count: i32,
  pub count_num: i64,
  pub count_pc: f64,
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
   
    delete_from_table("smm.version_summary", data_version, data_date, pool).await?;
    delete_from_table("smm.name_summary", data_version, data_date, pool).await?;
    delete_from_table("smm.name_ror", data_version, data_date, pool).await?;
    delete_from_table("smm.name_count_distribution", data_version, data_date, pool).await?;
    delete_from_table("smm.name_label_distribution", data_version, data_date, pool).await?;
    delete_from_table("smm.name_alias_distribution", data_version, data_date, pool).await?;
    delete_from_table("smm.name_acronym_distribution", data_version, data_date, pool).await?;
    delete_from_table("smm.orgs_of_type_summary", data_version, data_date, pool).await?;
    delete_from_table("smm.type_count_distribution", data_version, data_date, pool).await?;
    delete_from_table("smm.type_name_lang_code", data_version, data_date, pool).await?;
    delete_from_table("smm.ext_ids_summary", data_version, data_date, pool).await?;
    delete_from_table("smm.ext_ids_count_distribution", data_version, data_date, pool).await?;
    delete_from_table("smm.links_summary", data_version, data_date, pool).await?;
    delete_from_table("smm.links_count_distribution", data_version, data_date, pool).await?;
    delete_from_table("smm.relationships_summary", data_version, data_date, pool).await?;
    delete_from_table("smm.type_relationship", data_version, data_date, pool).await?;
    delete_from_table("smm.country_top_20_distribution", data_version, data_date, pool).await?;
    delete_from_table("smm.locs_count_distribution", data_version, data_date, pool).await?;
    Ok(())

}


async fn delete_from_table(table_name: &str, data_version:&String, data_date: &NaiveDate,
    pool: &Pool<Postgres>) -> Result<(), AppError> {

let sql = "DELETE from ".to_string() + table_name + " WHERE vcode = $1 AND vdate = $2";

sqlx::query(&sql)
.bind(data_version).bind(data_date)
.execute(pool)
.await?;

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
    let pc_label = get_pc (num_label,num_names);
    let pc_alias = get_pc (num_alias,num_names);
    let pc_acronym = get_pc (num_acronym,num_names);
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


  pub async fn store_name_count_distrib(data_version:&String, data_date: &NaiveDate, pool: &Pool<Postgres>, num_orgs: i64) -> Result<(), AppError> {
    
    let dv_dt = "select \'".to_string() + data_version + "\' as vcode, \'" +  &data_date.to_string() + "\'::date as vdate, ";

    let sql = dv_dt + r#"n_names as count, count(id) as count_num, 
                       round(count(id) * 10000 :: float / "# + &num_orgs.to_string() + r#":: float)/100 :: float as count_pc
                       from src.admin_data where n_names <> 0
                       group by n_names
                       order by n_names;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;

    for r in rows {
        sqlx::query(r#"INSERT INTO smm.name_count_distribution (vcode, vdate, count, count_num, count_pc) 
                            values($1, $2, $3, $4, $5)"#)
        .bind(r.vcode)
        .bind(r.vdate)
        .bind(r.count)
        .bind(r.count_num)
        .bind(r.count_pc)
        .execute(pool)
        .await?;
    }
    
    Ok(())

  }

  pub async fn store_label_count_distrib(data_version:&String, data_date: &NaiveDate, pool: &Pool<Postgres>, num_orgs: i64) -> Result<(), AppError> {
    
    let dv_dt = "select \'".to_string() + data_version + "\' as vcode, \'" +  &data_date.to_string() + "\'::date as vdate, ";

    let sql = dv_dt + r#"n_labels as count, count(id) as count_num, 
                       round(count(id) * 10000 :: float / "# + &num_orgs.to_string() + r#":: float)/100 :: float as count_pc
                       from src.admin_data where n_labels <> 0
                       group by n_labels
                       order by n_labels;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;

    for r in rows {
        sqlx::query(r#"INSERT INTO smm.name_label_distribution (vcode, vdate, count, count_num, count_pc) 
                            values($1, $2, $3, $4, $5)"#)
        .bind(r.vcode)
        .bind(r.vdate)
        .bind(r.count)
        .bind(r.count_num)
        .bind(r.count_pc)
        .execute(pool)
        .await?;
    }
    
    Ok(())

  }

  pub async fn store_alias_count_distrib(data_version:&String, data_date: &NaiveDate, pool: &Pool<Postgres>, num_orgs: i64) -> Result<(), AppError> {
   
    let dv_dt = "select \'".to_string() + data_version + "\' as vcode, \'" +  &data_date.to_string() + "\'::date as vdate, ";

    let sql = dv_dt + r#"n_aliases as count, count(id) as count_num, 
                       round(count(id) * 10000 :: float / "# + &num_orgs.to_string() + r#":: float)/100 :: float as count_pc
                       from src.admin_data where n_aliases <> 0
                       group by n_aliases
                       order by n_aliases;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;

    for r in rows {
        sqlx::query(r#"INSERT INTO smm.name_alias_distribution (vcode, vdate, count, count_num, count_pc) 
                            values($1, $2, $3, $4, $5)"#)
        .bind(r.vcode)
        .bind(r.vdate)
        .bind(r.count)
        .bind(r.count_num)
        .bind(r.count_pc)
        .execute(pool)
        .await?;
    }
    
    Ok(())

  }

  pub async fn store_acronym_count_distrib(data_version:&String, data_date: &NaiveDate, pool: &Pool<Postgres>, num_orgs: i64) -> Result<(), AppError> {

    let dv_dt = "select \'".to_string() + data_version + "\' as vcode, \'" +  &data_date.to_string() + "\'::date as vdate, ";

    let sql = dv_dt + r#"n_acronyms as count, count(id) as count_num, 
                       round(count(id) * 10000 :: float / "# + &num_orgs.to_string() + r#":: float)/100 :: float as count_pc
                       from src.admin_data where n_acronyms <> 0
                       group by n_acronyms
                       order by n_acronyms;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;

    for r in rows {
        sqlx::query(r#"INSERT INTO smm.name_acronym_distribution (vcode, vdate, count, count_num, count_pc) 
                            values($1, $2, $3, $4, $5)"#)
        .bind(r.vcode)
        .bind(r.vdate)
        .bind(r.count)
        .bind(r.count_num)
        .bind(r.count_pc)
        .execute(pool)
        .await?;
    }
    
    Ok(())

  }
  