use super::smm_storage_helper;

use sqlx::{Pool, Postgres};
use chrono::NaiveDate;
use crate::AppError;
use super::smm_structs::RorVersion;

pub async fn store_summary_data (vcode:&String, vdate: &NaiveDate,pool: &Pool<Postgres>) -> Result<(), AppError> {
    
    /*
        async fn ensure_version_details_known() -> () {

            // Ensure that dataversion and date are known, taking them
            // from the relevant table and padding them to the params struct
        }
     */

    smm_storage_helper::delete_any_existing_data(vcode, vdate, pool).await?;

    let num_orgs = get_record_num("core_data", pool).await?;

    // Derive standard first two items in many sql statements and construct RorVersion
    // struct as an easier means of passing parameters to helper functions

    let dv_dt = "select \'".to_string() + vcode + "\' as vcode, \'" 
                     +  &vdate.to_string() + "\'::date as vdate, ";

    let v = RorVersion {
        vcode: vcode.to_string(),
        vdate: vdate.to_owned(),
        num_orgs: num_orgs,
        dvdd: dv_dt.to_string(),
    };

    let num_names = get_record_num("names", pool).await?;
    let num_types= get_record_num("type", pool).await?;
    let num_links= get_record_num("links", pool).await?;
    let num_ext_ids= get_record_num("external_ids", pool).await?;
    let num_rels= get_record_num("relationships", pool).await?;
    let num_locations= get_record_num("locations", pool).await?;
    let num_domains= get_record_num("domains", pool).await?;
    
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
    
    smm_storage_helper::store_name_summary(&v, pool, num_names).await?;

    smm_storage_helper::store_name_ror(&v, pool).await?;
    
    smm_storage_helper::store_name_count_distrib(&v, pool).await?;

    smm_storage_helper::store_label_count_distrib(&v, pool).await?;

    smm_storage_helper::store_alias_count_distrib(&v, pool).await?;

    smm_storage_helper::store_acronym_count_distrib(&v, pool).await?;

    smm_storage_helper::store_lang_code_distrib(&v, pool).await?;

    smm_storage_helper::store_script_code_distrib(&v, pool).await?;
   
    smm_storage_helper::store_type_summary(&v, pool, num_types).await?;

    smm_storage_helper::store_type_count_distrib(&v, pool).await?;

    smm_storage_helper::store_types_with_lang_code(&v, pool).await?;

    smm_storage_helper::store_ext_ids_summary(&v, pool,num_ext_ids).await?;

    smm_storage_helper::store_ext_ids_count_distrib(&v, pool).await?;

    smm_storage_helper::store_links_summary(&v, pool, num_links).await?;

    smm_storage_helper::store_links_count_distrib(&v, pool).await?;

    smm_storage_helper::store_relationships_summary(&v, pool, num_rels).await?;

    smm_storage_helper::store_types_and_relationships(&v, pool).await?;

    smm_storage_helper::store_country_top_25_distrib(&v, pool, num_locations).await?;

    smm_storage_helper::store_locs_count_distrib(&v, pool).await?;

    Ok(())
}


pub async fn get_record_num (table_name: &str, pool: &Pool<Postgres>) -> Result<i64, AppError> {
    let sql = "SELECT COUNT(*) FROM src.".to_string() + table_name;
    let res = sqlx::query_scalar(&sql)
    .fetch_one(pool)
    .await?;
    Ok(res)
}