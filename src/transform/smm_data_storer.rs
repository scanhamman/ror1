use super::smm_storage_helper;

use sqlx::{Pool, Postgres};
use chrono::NaiveDate;
use crate::AppError;
use super::smm_structs::{FileParams, RorVersion, DistribRow, RankedRow, TypeRow};

pub async fn store_summary_data (pool: &Pool<Postgres>) -> Result<(), AppError> {
    
    // Obtain the data version and date (as previously stored in table during import process)

    let sql = "SELECT version as vcode, data_date as vdate_as_string from src.version_details;";
    let fp: FileParams = sqlx::query_as(&sql).fetch_one(pool).await?;

    let vdate = NaiveDate::parse_from_str(&fp.vdate_as_string, "%Y-%m-%d").unwrap();

    let num_orgs = get_record_num("core_data", pool).await?;
    let num_orgs_str = num_orgs.to_string();

    // Derive standard first two items in many sql statements and construct RorVersion
    // struct as an easier means of passing parameters to helper functions

    let dv_dt = "select \'".to_string() + &fp.vcode + "\' as vcode, \'" 
                     +  &vdate.to_string() + "\'::date as vdate, ";

    let v = RorVersion {
        vcode: fp.vcode.to_string(),
        vdate: vdate.to_owned(),
        num_orgs: num_orgs,
        //dvdd: dv_dt.clone()
    };

    smm_storage_helper::delete_any_existing_data(&v, pool).await?;

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
    .bind(fp.vcode).bind(vdate)
    .bind(num_orgs).bind(num_names)
    .bind(num_types).bind(num_links)
    .bind(num_ext_ids).bind(num_rels)
    .bind(num_locations).bind(num_domains)
    .execute(pool)
    .await?;
    
    //smm_storage_helper::store_name_summary(&v, pool, num_names).await?;

    // Name attributes summary
   
    let sql  = r#"select * from
                ("#.to_string() + &dv_dt + r#"rn.id, rn.name, count(t.id) as number_atts, 0::float as pc_of_atts, 
                count(distinct t.id) as number_orgs, 0::float as pc_of_orgs
                from lup.ror_name_types rn
                inner join src.names t
                on rn.id = t.name_type 
                group by rn.id, rn.name
                order by rn.id) a
            union
                ("# + &dv_dt + r#"12, 'nacro', sum(n_nacro), 0::float, count(id), 0::float
                from src.admin_data t where n_nacro > 0) 
            union 
                ("# + &dv_dt + r#"22, 'nacro (excl. cmps)', sum(n_nacro), 0::float, count(id), 0::float
                from src.admin_data t where n_nacro > 0 and n_is_company = 0) 
            union
                ("# + &dv_dt + r#"rn.id + 100, rn.name||'_wolc', count(t.id), 0::float, count(distinct t.id), 0::float
                from lup.ror_name_types rn
                inner join src.names t
                on rn.id = t.name_type 
                where t.lang_code is null
                group by rn.id, rn.name
                order by rn.id)
            union
                ("# + &dv_dt + r#"112, 'nacro_wolc', sum(n_nacro_wolc), 0::float, count(id), 0::float
                from src.admin_data t where n_nacro_wolc > 0) 
            union 
                ("# + &dv_dt + r#"122, 'nacro_wolc (excl. cmps)', sum(n_nacro_wolc), 0::float, count(id), 0::float
                from src.admin_data t where n_nacro_wolc > 0 and n_is_company = 0) 
                order by id"#;
    let rows: Vec<TypeRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    smm_storage_helper::store_summary(rows, pool, 1, "name types").await?;

    let sql  = "".to_string() + r#"Update smm.attributes_summary set 
                      pc_of_atts = round(number_atts * 10000::float / "# + &num_names.to_string() + r#"::float)/100::float,
                      pc_of_orgs = round(number_orgs * 10000::float / "# + &num_orgs_str + r#"::float)/100::float"#;
    sqlx::raw_sql(&sql).execute(pool).await?;

    // Org type attributes summary

    let sql  = dv_dt.clone() + r#"gt.id, gt.name, count(t.id) as number_atts, 
        round(count(t.id) * 10000::float/"# + &num_types.to_string() + r#"::float)/100::float as pc_of_atts,
        count(distinct t.id) as number_orgs,
        round(count(distinct t.id) * 10000::float/"# + &num_orgs_str + r#"::float)/100::float as pc_of_orgs
        from lup.ror_org_types gt
        inner join src.type t
        on gt.id = t.org_type 
        group by gt.id, gt.name
        order by gt.id;"#;
    let rows: Vec<TypeRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    smm_storage_helper::store_summary(rows, pool, 2, "org types").await?;

    // External ids attributes summary
      
    let sql = dv_dt.clone()  + r#"it.id, it.name, count(t.id) as number_atts, 
        round(count(t.id) * 10000::float / "# + &num_ext_ids.to_string() + r#"::float)/100::float as pc_of_atts,
        count(distinct t.id) as number_orgs,
        round(count(distinct t.id) * 10000::float / "# + &num_orgs_str + r#"::float)/100::float as pc_of_orgs
        from lup.ror_id_types it
        inner join src.external_ids t
        on it.id = t.id_type 
        group by it.id, it.name
        order by it.id;"#;
    let rows: Vec<TypeRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    smm_storage_helper::store_summary(rows, pool, 3, "external id types").await?;

    // Links attributes summary

   let sql = dv_dt.clone()  + r#"lt.id, lt.name, count(t.id) as number_atts, 
        round(count(t.id) * 10000::float / "# + &num_links.to_string() + r#"::float)/100::float as pc_of_atts,
        count(distinct t.id) as number_orgs,
        round(count(distinct t.id) * 10000::float / "# + &num_orgs_str + r#"::float)/100::float as pc_of_orgs
        from lup.ror_link_types lt
        inner join src.links t
        on lt.id = t.link_type 
        group by lt.id, lt.name
        order by lt.id;"#;
    let rows: Vec<TypeRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    smm_storage_helper::store_summary(rows, pool, 4, "link types").await?;

    // Relationships attributes summary

    let sql = dv_dt.clone() + r#"rr.id, rr.name, count(t.id) as number_atts, 
        round(count(t.id) * 10000::float / "# + &num_rels.to_string() + r#"::float)/100::float as pc_of_atts,
        count(distinct t.id) as number_orgs,
        round(count(distinct t.id) * 10000::float / "# + &num_orgs_str + r#"::float)/100::float as pc_of_orgs
        from lup.ror_rel_types rr
        inner join src.relationships t
        on rr.id = t.rel_type 
        group by rr.id, rr.name
        order by rr.id;"#;
    let rows: Vec<TypeRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    smm_storage_helper::store_summary(rows, pool, 5, "rel types").await?;

    // All names count distribution

    let sql = dv_dt.clone() + r#"n_names as count, count(id) as num_of_orgs, 
                        round(count(id) * 10000 :: float / "# + &num_orgs_str + r#":: float)/100 :: float as pc_of_orgs
                        from src.admin_data
                        group by n_names
                        order by n_names;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    smm_storage_helper::store_distrib(rows, "names", pool).await?;
    
    // Labels count distribution
       
    let sql = dv_dt.clone() + r#"n_labels as count, count(id) as num_of_orgs, 
                        round(count(id) * 10000 :: float / "# + &num_orgs_str + r#":: float)/100 :: float as pc_of_orgs
                        from src.admin_data
                        group by n_labels
                        order by n_labels;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    smm_storage_helper::store_distrib(rows, "labels", pool).await?;
    
    // Aliases count distribution
   
    let sql = dv_dt.clone() + r#"n_aliases as count, count(id) as num_of_orgs, 
                        round(count(id) * 10000 :: float / "# + &num_orgs_str + r#":: float)/100 :: float as pc_of_orgs
                        from src.admin_data
                        group by n_aliases
                        order by n_aliases;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    smm_storage_helper::store_distrib(rows, "aliases", pool).await?;
    
    // Acronyms count distribution

    let sql = dv_dt.clone() + r#"n_acronyms as count, count(id) as num_of_orgs, 
                        round(count(id) * 10000 :: float / "# + &num_orgs_str + r#":: float)/100 :: float as pc_of_orgs
                        from src.admin_data
                        group by n_acronyms
                        order by n_acronyms;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    smm_storage_helper::store_distrib(rows, "acronyms", pool).await?;

    // Locations count distribution
    
    let sql = dv_dt.clone() + r#"n_locs as count, count(id) as num_of_orgs, 
                        round(count(id) * 10000 :: float / "# + &num_orgs_str + r#":: float)/100 :: float as pc_of_orgs
                        from src.admin_data
                        group by n_locs
                        order by n_locs;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    smm_storage_helper::store_distrib(rows, "locs", pool).await?;

    // Org types count distribution

    let sql = dv_dt.clone() + r#"n_types as count, count(id) as num_of_orgs, 
                      round(count(id) * 10000 :: float / "# + &num_orgs_str + r#":: float)/100 :: float as pc_of_orgs
                      from src.admin_data
                      group by n_types
                      order by n_types;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    smm_storage_helper::store_distrib(rows, "org_types", pool).await?;

    // External ids count distribution

    let sql = dv_dt.clone() + r#"n_ext_ids as count, count(id) as num_of_orgs, 
                      round(count(id) * 10000 :: float / "# + &num_orgs_str + r#":: float)/100 :: float as pc_of_orgs
                      from src.admin_data
                      group by n_ext_ids
                      order by n_ext_ids;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    smm_storage_helper::store_distrib(rows, "ext_ids", pool).await?;

    // Links count distribution

    let sql = dv_dt.clone() + r#"n_links as count, count(id) as num_of_orgs, 
                      round(count(id) * 10000 :: float / "# + &num_orgs_str + r#":: float)/100 :: float as pc_of_orgs
                      from src.admin_data
                      group by n_links
                      order by n_links;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    smm_storage_helper::store_distrib(rows, "links", pool).await?;

    // Non-English language ranked distribution
    
    let total_of_ne = smm_storage_helper::get_count("select count(*) from src.names where lang_code <> 'en'", pool).await?;
    let sql = dv_dt.clone() + r#"lc.name as entity, count(n.id) as number,
                        round(count(n.id) * 10000 :: float / "# + &total_of_ne.to_string() + r#":: float)/100 :: float as pc_of_entities,
                        round(count(distinct n.id) * 10000 :: float / "# + &(num_names.to_string()) + r#":: float)/100 :: float as pc_of_base_set
                        from src.names n inner join lup.lang_codes lc 
                        on n.lang_code = lc.code 
                        where lang_code <> 'en'
                        group by lc.name
                        order by count(n.id) desc;"#;
    let rows: Vec<RankedRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    smm_storage_helper::store_ranked_distrib(&v, &rows, pool, "Remaining languages", 1, 
            total_of_ne, num_names).await?;
    
    // Non-Latin script ranked distribution
      
    let total_of_nltn = smm_storage_helper::get_count("select count(*) from src.names where script_code <> 'Latn'", pool).await?;
    let sql = dv_dt.clone() + r#"ls.iso_name as entity, count(n.id) as number,
                        round(count(n.id) * 10000 :: float / "# + &total_of_nltn.to_string() + r#":: float)/100 :: float as pc_of_entities,
                        round(count(distinct n.id) * 10000 :: float / "# + &(num_names.to_string()) + r#":: float)/100 :: float as pc_of_base_set
                        from src.names n inner join lup.lang_scripts ls 
                        on n.script_code = ls.code 
                        where script_code <> 'Latn'
                        group by ls.iso_name
                        order by count(n.id) desc; "#;
    let rows: Vec<RankedRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    smm_storage_helper::store_ranked_distrib(&v, &rows, pool, "Remaining scripts", 2,
                        total_of_nltn, num_names).await?;

    // Country ranked distribution (and non US pc)

    let total_of_nus = smm_storage_helper::get_count("select count(*) from src.locations where country_code <> 'US'", pool).await?;
    let sql = dv_dt.clone() + r#"country_name as entity, count(id) as number, 
                      round(count(c.id) * 10000 :: float / "# + &total_of_nus.to_string() + r#":: float)/100 :: float as pc_of_entities,
                      round(count(distinct c.id) * 10000 :: float / "# + &(num_locations.to_string()) + r#":: float)/100 :: float as pc_of_base_set
                      from src.locations c
                      group by country_name
                      order by count(country_name) desc;"#;
    let rows: Vec<RankedRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    smm_storage_helper::store_ranked_distrib(&v, &rows, pool, "Remaining countries", 3,
                        total_of_nus, num_locations).await?;
    



    smm_storage_helper::store_name_ror(&v, pool).await?;
    
    smm_storage_helper::store_types_with_lang_code(&v, pool).await?;

    smm_storage_helper::store_types_and_relationships(&v, pool).await?;

    smm_storage_helper::store_singletons(&v, num_names, pool).await?;




    Ok(())
}


pub async fn get_record_num (table_name: &str, pool: &Pool<Postgres>) -> Result<i64, AppError> {
    let sql = "SELECT COUNT(*) FROM src.".to_string() + table_name;
    let res = sqlx::query_scalar(&sql)
    .fetch_one(pool)
    .await?;
    Ok(res)
}