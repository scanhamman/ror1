use crate::transform::smm_storage_helper;

use sqlx::{Pool, Postgres};
use chrono::NaiveDate;
use crate::AppError;

pub async fn store_summary_data (vcode:&String, vdate: &NaiveDate,pool: &Pool<Postgres>) -> Result<(), AppError> {
    
    smm_storage_helper::delete_any_existing_data(vcode, vdate, pool).await?;

    let num_orgs = get_record_num("src.core_data", pool).await?;
    let num_names = get_record_num("src.names", pool).await?;
    let num_types= get_record_num("src.type", pool).await?;
    let num_links= get_record_num("src.links", pool).await?;
    let num_ext_ids= get_record_num("src.external_ids", pool).await?;
    let num_rels= get_record_num("src.relationships", pool).await?;
    let num_locations= get_record_num("src.locations", pool).await?;
    let num_domains= get_record_num("src.domains", pool).await?;
    
    let sql = r#"INSERT into smm.version_summary (vcode, vdate, num_orgs, num_names,
                      num_types, num_links, num_ext_ids, num_rels, num_locations , num_domains)
                      values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"#;

    sqlx::query(sql)
    .bind(vcode).bind(vdate)
    .bind(num_orgs).bind(num_names)
    .bind(num_types).bind(num_links)
    .bind(num_ext_ids).bind(num_rels)
    .bind(num_locations).bind(num_domains)
    .execute(pool)
    .await?;

    // Standard first two items in many sql statements

    let dv_dt = "select \'".to_string() + vcode + "\' as vcode, \'" 
                     +  &vdate.to_string() + "\'::date as vdate, ";

    smm_storage_helper::store_name_summary(vcode, vdate, pool, num_names).await?;

    smm_storage_helper::store_name_ror(vcode, vdate, pool, num_orgs).await?;
    
    smm_storage_helper::store_name_count_distrib(dv_dt.clone(), pool, num_orgs).await?;

    smm_storage_helper::store_label_count_distrib(dv_dt.clone(), pool, num_orgs).await?;

    smm_storage_helper::store_alias_count_distrib(dv_dt.clone(), pool, num_orgs).await?;

    smm_storage_helper::store_acronym_count_distrib(dv_dt.clone(), pool, num_orgs).await?;


    // need name language distribution


    // need name script distribution
        
   
    smm_storage_helper::store_type_summary(vcode, vdate, pool, num_orgs).await?;

    smm_storage_helper::store_type_count_distrib(dv_dt.clone(), pool, num_orgs).await?;


    // type_name_lang_code



    // ext_ids_summary



    smm_storage_helper::store_ext_ids_count_distrib(dv_dt.clone(), pool, num_orgs).await?;


    // links_summary



    smm_storage_helper::store_links_count_distrib(dv_dt.clone(), pool, num_orgs).await?;



    // relationships_summary



    // type_relationship



    smm_storage_helper::store_country_top_20_distrib(dv_dt.clone(), pool, num_locations).await?;

    smm_storage_helper::store_locs_count_distrib(dv_dt.clone(), pool, num_orgs).await?;

    Ok(())

}


pub async fn get_record_num (table_name: &str, pool: &Pool<Postgres>) -> Result<i64, AppError> {
    let sql = "SELECT COUNT(*) FROM ror.".to_owned() + table_name;
    let res = sqlx::query_scalar(&sql)
    .fetch_one(pool)
    .await?;
    Ok(res)
}

/*
      
    ----------------------------------------------------
    Distribution by country (long list!)
    
    select country_code, count(country_code) from src.locations
    group by country_code
    order by count(country_code) desc

    // List perhaps top 20 or 25, or above a certain threshold...

    Numbers with multiple locations

    select count(*) from src.admin_data where n_locs = 0;
    select count(*) from src.admin_data where n_locs = 1; 
    select count(*) from src.admin_data where n_locs = 2;
    select count(*) from src.admin_data where n_locs = 3;
    select count(*) from src.admin_data where n_locs > 3;

    -------------------------
   
    Number of orgs with null lang codes for labels and aliases (excluding acronyms and commercial orgs)
    (difficult to apply a language to a commercial name)

    -----------------------------------

    Names not in local script...
    for selected countries

    set up script data in import process

    -----------------------------------
     
    -----------------------------------
  
    Number of orgs with 1, 2, 3, or more parent rel numbers

    select count(*) from src.admin_data where n_parrels = 0;
    select count(*) from src.admin_data where n_parrels = 1; 
    select count(*) from src.admin_data where n_parrels = 2;
    select count(*) from src.admin_data where n_parrels = 3;
    select count(*) from src.admin_data where n_parrels > 3;

    Number of orgs with 1, 2, 3, or more child rel numbers

    select count(*) from src.admin_data where n_chrels = 0;
    select count(*) from src.admin_data where n_chrels = 1; 
    select count(*) from src.admin_data where n_chrels = 2;
    select count(*) from src.admin_data where n_chrels = 3;
    select count(*) from src.admin_data where n_chrels > 3;

    Number of orgs with 1, 2, 3, or more rel rel numbers

    select count(*) from src.admin_data where n_relrels = 0;
    select count(*) from src.admin_data where n_relrels = 1; 
    select count(*) from src.admin_data where n_relrels = 2;
    select count(*) from src.admin_data where n_relrels = 3;
    select count(*) from src.admin_data where n_relrels > 3;

    Number of orgs with 1, 2, 3, or more pred rel numbers

    select count(*) from src.admin_data where n_predrels = 0;
    select count(*) from src.admin_data where n_predrels = 1; 
    select count(*) from src.admin_data where n_predrels = 2;
    select count(*) from src.admin_data where n_predrels = 3;
    select count(*) from src.admin_data where n_predrels > 3;

    Number of orgs with 1, 2, 3, or more succ rel numbers

    select count(*) from src.admin_data where n_sucrels = 0;
    select count(*) from src.admin_data where n_sucrels = 1; 
    select count(*) from src.admin_data where n_sucrels = 2;
    select count(*) from src.admin_data where n_sucrels = 3;
    select count(*) from src.admin_data where n_sucrels > 3;

    Any orgs with non-reciprocated relationship?

    look at par-ch rels
    look at rel-rel rels from both directions
    look at pred-suc rels

    for par-ch - create a temp table with the inverse relationship
    check it against the actual data
    any discrepencies...

    Any parent child rels where all are commercial?

    Can they be made into one org with multiple locations? 

    Any parent child 'chains' of more than one level?

    look at chrel orgs having a parrel as well...

    -----------------------------------
    
    // set up some tables...

    // Set up some views??...

    // Use these to produce json

    // Use these to produce a text version

    // Create the res file. If it already exists overwrite it.

    */



