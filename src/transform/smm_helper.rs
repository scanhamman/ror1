use sqlx::{Pool, Postgres};
use crate::AppError;
use chrono::NaiveDate;

#[derive(sqlx::FromRow)]
pub struct FileParams {
    pub vcode: String,
    pub vdate_as_string: String,
}

#[derive(sqlx::FromRow)]
pub struct DistribRow {
  pub vcode: String,
  pub vdate: NaiveDate,
  pub count: i32,
  pub num_of_orgs: i64,
  pub pc_of_orgs: f64,
}

#[derive(sqlx::FromRow)]
pub struct RankedRow {
  pub vcode: String,
  pub vdate: NaiveDate,
  pub entity: String,
  pub number: i64,
  pub pc_of_entities: f64,
  pub pc_of_base_set: f64,
}

#[derive(sqlx::FromRow)]
pub struct TypeRow {
    pub vcode: String,
    pub vdate: NaiveDate,
    pub id: i32,
    pub name: String,
    pub number_atts: i64,
    pub pc_of_atts: f64,
    pub number_orgs: i64,
    pub pc_of_orgs: f64,
}

#[derive(sqlx::FromRow)]
pub struct OrgRow {
    pub type_id: i32, 
    pub name: String,
    pub org_num: i64, 
}


pub async fn delete_any_existing_data(vcode: &String, vdate: &String, pool: &Pool<Postgres>) -> Result<(), AppError> {
   
    // format!() macro does not seem to recognise apostrrophes, even when escaped (???)

    let wc = " WHERE vcode = \'".to_string() + vcode
                                 + "\' AND vdate = \'" + vdate + "\'::date;";
        
    let del_sql = format!(r#"DELETE from smm.version_summary {}
                DELETE from smm.attributes_summary {}
                DELETE from smm.count_distributions {}
                DELETE from smm.ranked_distributions {}
                DELETE from smm.singletons {}
                DELETE from smm.org_type_and_lang_code {}
                DELETE from smm.org_type_and_relationships {}"#
                , wc, wc, wc, wc, wc, wc, wc);

   sqlx::raw_sql(&del_sql).execute(pool).await?;
   Ok(())
}


pub async fn create_name_attributes(dv_dt: &String, num_orgs_str: &String, num_names: &String, 
    pool: &Pool<Postgres>) ->  Result<(), AppError> {

    // Name attributes summary     

    let sql  = r#"select * from
            ("#.to_string() + dv_dt + r#"rn.id, rn.name, count(t.id) as number_atts, 0::float as pc_of_atts, 
            count(distinct t.id) as number_orgs, 0::float as pc_of_orgs
            from lup.ror_name_types rn
            inner join src.names t
            on rn.id = t.name_type 
            group by rn.id, rn.name
            order by rn.id) a
            union
            ("# + dv_dt + r#"12, 'nacro', sum(n_nacro), 0::float, count(id), 0::float
            from src.admin_data t where n_nacro > 0) 
            union 
            ("# + dv_dt + r#"22, 'nacro (excl. cmps)', sum(n_nacro), 0::float, count(id), 0::float
            from src.admin_data t where n_nacro > 0 and n_is_company = 0) 
            union
            ("# + dv_dt + r#"rn.id + 100, rn.name||'_wolc', count(t.id), 0::float, count(distinct t.id), 0::float
            from lup.ror_name_types rn
            inner join src.names t
            on rn.id = t.name_type 
            where t.lang_code is null
            group by rn.id, rn.name
            order by rn.id)
            union
            ("# + dv_dt + r#"112, 'nacro_wolc', sum(n_nacro_wolc), 0::float, count(id), 0::float
            from src.admin_data t where n_nacro_wolc > 0) 
            union 
            ("# + dv_dt + r#"122, 'nacro_wolc (excl. cmps)', sum(n_nacro_wolc), 0::float, count(id), 0::float
            from src.admin_data t where n_nacro_wolc > 0 and n_is_company = 0) 
            order by id"#;
    let rows: Vec<TypeRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    store_summary(rows, pool, 1, "name types").await?;

    let sql  = "".to_string() + r#"Update smm.attributes_summary set 
            pc_of_atts = round(number_atts * 10000::float / "# + num_names + r#"::float)/100::float,
            pc_of_orgs = round(number_orgs * 10000::float / "# + num_orgs_str + r#"::float)/100::float"#;
    sqlx::raw_sql(&sql).execute(pool).await?;
    Ok(())
}


pub async fn create_other_attributes(dv_dt: &String, num_orgs_str: &String, num_types: &String, 
num_ext_ids: &String, num_links: &String, num_rels: &String, pool: &Pool<Postgres>) ->  Result<(), AppError> {
    
    // Org type attributes summary

    let sql  = dv_dt.clone() + r#"gt.id, gt.name, count(t.id) as number_atts, 
            round(count(t.id) * 10000::float/"# + num_types + r#"::float)/100::float as pc_of_atts,
            count(distinct t.id) as number_orgs,
            round(count(distinct t.id) * 10000::float/"# + num_orgs_str + r#"::float)/100::float as pc_of_orgs
            from lup.ror_org_types gt
            inner join src.type t
            on gt.id = t.org_type 
            group by gt.id, gt.name
            order by gt.id;"#;
    let rows: Vec<TypeRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    store_summary(rows, pool, 2, "org types").await?;

    // External ids attributes summary

    let sql = dv_dt.clone()  + r#"it.id, it.name, count(t.id) as number_atts, 
            round(count(t.id) * 10000::float / "# + num_ext_ids + r#"::float)/100::float as pc_of_atts,
            count(distinct t.id) as number_orgs,
            round(count(distinct t.id) * 10000::float / "# + num_orgs_str + r#"::float)/100::float as pc_of_orgs
            from lup.ror_id_types it
            inner join src.external_ids t
            on it.id = t.id_type 
            group by it.id, it.name
            order by it.id;"#;
    let rows: Vec<TypeRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    store_summary(rows, pool, 3, "external id types").await?;

    // Links attributes summary

    let sql = dv_dt.clone()  + r#"lt.id, lt.name, count(t.id) as number_atts, 
            round(count(t.id) * 10000::float / "# + num_links + r#"::float)/100::float as pc_of_atts,
            count(distinct t.id) as number_orgs,
            round(count(distinct t.id) * 10000::float / "# + num_orgs_str + r#"::float)/100::float as pc_of_orgs
            from lup.ror_link_types lt
            inner join src.links t
            on lt.id = t.link_type 
            group by lt.id, lt.name
            order by lt.id;"#;
    let rows: Vec<TypeRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    store_summary(rows, pool, 4, "link types").await?;

    // Relationships attributes summary

    let sql = dv_dt.clone() + r#"rr.id, rr.name, count(t.id) as number_atts, 
            round(count(t.id) * 10000::float / "# + num_rels + r#"::float)/100::float as pc_of_atts,
            count(distinct t.id) as number_orgs,
            round(count(distinct t.id) * 10000::float / "# + num_orgs_str + r#"::float)/100::float as pc_of_orgs
            from lup.ror_rel_types rr
            inner join src.relationships t
            on rr.id = t.rel_type 
            group by rr.id, rr.name
            order by rr.id;"#;
    let rows: Vec<TypeRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    store_summary(rows, pool, 5, "rel types").await?;

    Ok(())
}


pub async fn create_count_distributions(dv_dt: &String, num_orgs_str: &String, pool: &Pool<Postgres>) ->  Result<(), AppError> {

    // All names count distribution

    let sql = dv_dt.clone() + r#"n_names as count, count(id) as num_of_orgs, 
            round(count(id) * 10000 :: float / "# + num_orgs_str + r#":: float)/100 :: float as pc_of_orgs
            from src.admin_data
            group by n_names
            order by n_names;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    store_distrib(rows, "names", pool).await?;

    // Labels count distribution

    let sql = dv_dt.clone() + r#"n_labels as count, count(id) as num_of_orgs, 
            round(count(id) * 10000 :: float / "# + num_orgs_str + r#":: float)/100 :: float as pc_of_orgs
            from src.admin_data
            group by n_labels
            order by n_labels;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    store_distrib(rows, "labels", pool).await?;

    // Aliases count distribution

    let sql = dv_dt.clone() + r#"n_aliases as count, count(id) as num_of_orgs, 
            round(count(id) * 10000 :: float / "# + num_orgs_str + r#":: float)/100 :: float as pc_of_orgs
            from src.admin_data
            group by n_aliases
            order by n_aliases;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    store_distrib(rows, "aliases", pool).await?;

    // Acronyms count distribution

    let sql = dv_dt.clone() + r#"n_acronyms as count, count(id) as num_of_orgs, 
            round(count(id) * 10000 :: float / "# + num_orgs_str + r#":: float)/100 :: float as pc_of_orgs
            from src.admin_data
            group by n_acronyms
            order by n_acronyms;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    store_distrib(rows, "acronyms", pool).await?;

    // Locations count distribution

    let sql = dv_dt.clone() + r#"n_locs as count, count(id) as num_of_orgs, 
            round(count(id) * 10000 :: float / "# + num_orgs_str + r#":: float)/100 :: float as pc_of_orgs
            from src.admin_data
            group by n_locs
            order by n_locs;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    store_distrib(rows, "locs", pool).await?;

    // Org types count distribution

    let sql = dv_dt.clone() + r#"n_types as count, count(id) as num_of_orgs, 
            round(count(id) * 10000 :: float / "# + num_orgs_str + r#":: float)/100 :: float as pc_of_orgs
            from src.admin_data
            group by n_types
            order by n_types;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    store_distrib(rows, "org_types", pool).await?;

    // External ids count distribution

    let sql = dv_dt.clone() + r#"n_ext_ids as count, count(id) as num_of_orgs, 
            round(count(id) * 10000 :: float / "# + num_orgs_str + r#":: float)/100 :: float as pc_of_orgs
            from src.admin_data
            group by n_ext_ids
            order by n_ext_ids;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    store_distrib(rows, "ext_ids", pool).await?;

    // Links count distribution

    let sql = dv_dt.clone() + r#"n_links as count, count(id) as num_of_orgs, 
            round(count(id) * 10000 :: float / "# + num_orgs_str + r#":: float)/100 :: float as pc_of_orgs
            from src.admin_data
            group by n_links
            order by n_links;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    store_distrib(rows, "links", pool).await?;

    Ok(())
}

pub async fn create_ranked_count_distributions(vcode: &String, vdate: NaiveDate, dv_dt: &String, num_names: i64, 
num_locs: i64, total_of_ne: i64, total_of_nltn: i64, total_of_nus: i64, pool: &Pool<Postgres>) ->  Result<(), AppError> {

    // Non-English language ranked distribution

    let sql = dv_dt.clone() + r#"lc.name as entity, count(n.id) as number,
            round(count(n.id) * 10000 :: float / "# + &total_of_ne.to_string() + r#":: float)/100 :: float as pc_of_entities,
            round(count(distinct n.id) * 10000 :: float / "# + &(num_names.to_string()) + r#":: float)/100 :: float as pc_of_base_set
            from src.names n inner join lup.lang_codes lc 
            on n.lang_code = lc.code 
            where lang_code <> 'en'
            group by lc.name
            order by count(n.id) desc;"#;
    let rows: Vec<RankedRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    store_ranked_distrib(&vcode, vdate, &rows, pool, "Remaining languages", 1, 
    total_of_ne, num_names).await?;

    // Non-Latin script ranked distribution

    let sql = dv_dt.clone() + r#"ls.iso_name as entity, count(n.id) as number,
            round(count(n.id) * 10000 :: float / "# + &total_of_nltn.to_string() + r#":: float)/100 :: float as pc_of_entities,
            round(count(distinct n.id) * 10000 :: float / "# + &(num_names.to_string()) + r#":: float)/100 :: float as pc_of_base_set
            from src.names n inner join lup.lang_scripts ls 
            on n.script_code = ls.code 
            where script_code <> 'Latn'
            group by ls.iso_name
            order by count(n.id) desc; "#;
    let rows: Vec<RankedRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    store_ranked_distrib(&vcode, vdate, &rows, pool, "Remaining scripts", 2,
    total_of_nltn, num_names).await?;

    // Country ranked distribution (and non US pc)

    let sql = dv_dt.clone() + r#"country_name as entity, count(id) as number, 
            round(count(c.id) * 10000 :: float / "# + &total_of_nus.to_string() + r#":: float)/100 :: float as pc_of_entities,
            round(count(distinct c.id) * 10000 :: float / "# + &(num_locs.to_string()) + r#":: float)/100 :: float as pc_of_base_set
            from src.locations c
            group by country_name
            order by count(country_name) desc;"#;
    let rows: Vec<RankedRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    store_ranked_distrib(&vcode, vdate, &rows, pool, "Remaining countries", 3,
    total_of_nus, num_locs).await?;

    Ok(())
}


pub async fn create_type_linked_tables(dv_dt: &String, pool: &Pool<Postgres>) ->  Result<(), AppError> {

    // Get the organisation type categories and total numbers.

    let org_sql  = r#"select org_type as type_id, p.name, 
            count(distinct t.id) as org_num
            from src.type t
            inner join lup.ror_org_types p
            on t.org_type = p.id
            group by org_type, p.name
            order by org_type;"#;
    let rows: Vec<OrgRow> = sqlx::query_as(org_sql).fetch_all(pool).await?;

    store_types_with_lang_code(dv_dt, rows, pool).await?;

    let rows: Vec<OrgRow> = sqlx::query_as(org_sql).fetch_all(pool).await?;

    store_types_and_relationships(dv_dt, rows, pool).await?;

    Ok(())
}


pub async fn store_types_with_lang_code(dv_dt: &String, rows: Vec<OrgRow>, pool: &Pool<Postgres>) -> Result<(), AppError> {

    // For each org type, and each of the three name types (therefore 9 x 3 rows),
    // get total number of names and numbers with / without lang codes.

    #[derive(sqlx::FromRow)]
    struct NameLCRow {
        vcode: String,
        vdate: NaiveDate,
        ntype: String,
        total: i64,
        names_wlc: i64,
        names_wolc: i64,
        names_wlc_pc: f64,
        names_wolc_pc: f64
    }
   
    for t in rows {

        // Get the data on the names linked to these organisations

        let lc_sql = dv_dt.to_owned() + r#"case name_type
                    when 5 then 'label' 
                    when 7 then 'alias'  
                    when 10 then 'acronym' 
                end as ntype, 
                count(lc) as names_wlc, count(nolc) as names_wolc, count(lc) + count(nolc) as total,
                (round(count(lc) * 10000::float / (count(lc) + count(nolc))::float)/100::float) as names_wlc_pc,
                (round(count(nolc) * 10000::float / (count(lc) + count(nolc))::float)/100::float) as names_wolc_pc
                from
                    (select n.id, n.name_type,
                    case 
                        when n.lang_code is not null then 'x'
                    end as lc, 
                    case 
                        when n.lang_code is  null then 'x'
                    end as nolc
                    from src.names n 
                    inner join src.type t
                    on n.id = t.id
                    where t.org_type = "# + &t.type_id.to_string() + r#") ns
                group by ns.name_type 
                order by ns.name_type;"#;
        let rows: Vec<NameLCRow> = sqlx::query_as(&lc_sql).fetch_all(pool).await?;

        // Store the individual rows.

        for r in rows {
            sqlx::query(r#"INSERT INTO smm.org_type_and_lang_code (vcode, vdate, org_type, name_type, 
                        names_num, names_wlc, names_wolc, names_wlc_pc, names_wolc_pc) 
                        values($1, $2, $3, $4, $5, $6, $7, $8, $9)"#)
            .bind(r.vcode).bind(r.vdate)
            .bind(t.name.clone()).bind(r.ntype).bind(r.total)
            .bind(r.names_wlc).bind(r.names_wolc).bind(r.names_wlc_pc).bind(r.names_wolc_pc)
            .execute(pool)
            .await?;
        }
    }

    Ok(())
}


pub async fn store_types_and_relationships(dv_dt: &String, rows: Vec<OrgRow>, pool: &Pool<Postgres>) -> Result<(), AppError> {

    // For each org type, and each of the 5 relationship types (therefore up to 9 x 5 rows),
    // get number of orgs having one or more relationships of each type, and the total number of orgs involved.

    #[derive(sqlx::FromRow)]
    struct TypeRelRow {
        vcode: String,
        vdate: NaiveDate,
        rtype: String,
        num_rels: i64,
        num_orgs: i64,
        num_orgs_pc: f64,
    }

    for t in rows {

        // Get the data on the names linked to these organisations

        let tr_sql = dv_dt.to_owned() + r#"case rs.rel_type
                when 1 then 'parent'  
                when 2 then 'child' 
                when 3 then 'related' 
                when 4 then 'predecessor' 
                when 5 then 'successor' 
                end as rtype, 
            count(rs.id) as num_rels,
            count(distinct rs.id) as num_orgs,
            round((count(distinct rs.id) * 10000::float /"#
                + &t.org_num.to_string() + r#"::float)) /100::float as num_orgs_pc
            from
                (select r.id, r.rel_type
                from src.relationships r 
                inner join src.type t
                on r.id = t.id
                where t.org_type ="# + &t.type_id.to_string() + r#") rs 
            group by rs.rel_type 
            order by rs.rel_type;"#;

        let rows: Vec<TypeRelRow> = sqlx::query_as(&tr_sql).fetch_all(pool).await?;

        // Store the individual rows.

        for r in rows {
            sqlx::query(r#"INSERT INTO smm.org_type_and_relationships (vcode, vdate, org_type, 
                    rel_type, num_links, num_orgs, num_orgs_total, num_orgs_pc) 
                    values($1, $2, $3, $4, $5, $6, $7, $8)"#)
            .bind(r.vcode).bind(r.vdate)
            .bind(t.name.clone()).bind(r.rtype).bind(r.num_rels).bind(r.num_orgs)
            .bind(t.org_num).bind(r.num_orgs_pc)
            .execute(pool)
            .await?;
        }
    }

    Ok(())
}


pub async fn store_singletons(vcode: &String, vdate: NaiveDate, num_orgs: i64, num_names: i64, 
                              num_nltn: i64, pool: &Pool<Postgres>) -> Result<(), AppError> {

    // Names not in English or not in Latin script

    let num_nacro = get_count("select count(*) from src.names where name_type <> 10", pool).await?;
    let num_nacro_ne = get_count("select count(*) from src.names where name_type <> 10 and lang_code <> 'en'", pool).await?;
    let pc_nacro_ne = get_pc (num_nacro_ne,num_nacro);   
    let pc_nltn = get_pc (num_nltn,num_nacro);  

    store_singleton(vcode, vdate, "Non acronym names that are not English, number and pc of total names",  
                        num_nacro_ne, Some(pc_nacro_ne), pool).await?;
    store_singleton(vcode, vdate, "Names that are not in Latin script, number and pc of total names",  
                        num_nltn, Some(pc_nltn), pool).await?;

    // Names without a language code

    let total_wolc = get_count("select count(*) from src.names where lang_code is null", pool).await?;
    let pc_total_wolc = get_pc (total_wolc,num_names);
    store_singleton(vcode, vdate, "Names that do not have a language code, number and pc of total names",  
                        total_wolc, Some(pc_total_wolc), pool).await?;
    
    let nacro_wolc = get_count("select count(*) from src.names where name_type <> 10 and lang_code is null", pool).await?;
    let pc_nacro_wolc =  get_pc (nacro_wolc, num_nacro);
    store_singleton(vcode, vdate, "Non-acronym names that do not have a language code, number and pc of total non-acronym names",  
    nacro_wolc, Some(pc_nacro_wolc), pool).await?;

    let nacro_ncmp_wolc = get_count(r#"select count(n.id) from 
                    src.names n
                    inner join src.admin_data ad
                    on n.id = ad.id 
                    where n.name_type <> 10 and ad.n_is_company = 0
                    and n.lang_code is null"#, pool).await?;   
    let num_ncmp_orgs = get_count(r#"select count(id) from src.admin_data where n_is_company = 0"#, pool).await?;
    let pc_nacro_ncmp_wolc =  get_pc (nacro_ncmp_wolc, num_ncmp_orgs);
    store_singleton(vcode, vdate, "Non-acronym, non company names that do not have a language code, number and pc of non-acronym non company names",  
    nacro_ncmp_wolc, Some(pc_nacro_ncmp_wolc), pool).await?;

    // Additional relationship data points

    let parch_orgs =  get_count("select count(*) from src.admin_data where n_chrels > 0 and n_parrels > 0", pool).await?;
    let parch_orgs_pc =  get_pc(parch_orgs, num_orgs);   
    store_singleton(vcode, vdate, "Orgs both parent and child, number and pc of total orgs",  
                        parch_orgs, Some(parch_orgs_pc), pool).await?;

    let par_no_child = get_rel_imbalance(1, 2, pool).await.unwrap();
    let par_no_parent = get_rel_imbalance(2, 1, pool).await.unwrap();
    let non_recip_pc = par_no_child + par_no_parent;
    let non_recip_rr = get_rel_imbalance(3, 3, pool).await.unwrap();
    let pred_no_succ = get_rel_imbalance(4, 5, pool).await.unwrap();
    let succ_no_pred = get_rel_imbalance(5, 4, pool).await.unwrap();
    let non_recip_ps = pred_no_succ + succ_no_pred;
    
    store_singleton(vcode, vdate, "Non-reciprocated parent-child relationships, number", non_recip_pc, None, pool).await?;
    store_singleton(vcode, vdate, "Non-reciprocated 'related' relationships, number", non_recip_rr, None, pool).await?;
    store_singleton(vcode, vdate, "Non-reciprocated pred-succ relationships, number", non_recip_ps, None, pool).await?;

    // Data on ROR labels

    let num_label_ror = get_count(r#"select count(*) from src.names 
                                     where name_type = 5 and is_ror_name = true"#, pool).await?; 
    let num_label_nror = get_count(r#"select count(*) from src.names 
                                     where name_type = 5 and is_ror_name = false"#, pool).await?; 
    let num_nlabel_ror = get_count(r#"select count(*) from src.names 
                                     where name_type <> 5 and is_ror_name = true"#, pool).await?; 
    let pc_nlabel_ror = get_pc(num_nlabel_ror, num_orgs);  

    store_singleton(vcode, vdate, "Labels that are designated ROR names, number", num_label_ror, None, pool).await?;
    store_singleton(vcode, vdate, "Labels that are not designated ROR names, number", num_label_nror, None, pool).await?;
    store_singleton(vcode, vdate, "Any non-Labels that are designated ROR names, number and pc of total orgs",  
                        num_nlabel_ror, Some(pc_nlabel_ror), pool).await?;

    let num_en_ror = get_count(r#"select count(*) from src.names 
                                  where is_ror_name = true and lang_code = 'en'"#, pool).await?;                                                    
    let num_nen_ror = get_count(r#"select count(*) from src.names 
                                  where is_ror_name = true and lang_code <> 'en' and lang_code is not null"#, pool).await?; 
    let num_wolc_ror = get_count(r#"select count(*) from src.names 
                                  where is_ror_name = true and lang_code is null"#, pool).await?; 

    let pc_en_ror = get_pc(num_en_ror, num_orgs);
    let pc_nen_ror = get_pc(num_nen_ror, num_orgs);
    let pc_wolc_ror = get_pc(num_wolc_ror, num_orgs); 

    store_singleton(vcode, vdate,  "ROR names in English, number and pc of total orgs", num_en_ror, Some(pc_en_ror), pool).await?;
    store_singleton(vcode, vdate, "ROR names not in English, number and pc of total orgs", 
                        num_nen_ror, Some(pc_nen_ror), pool).await?;
    store_singleton(vcode, vdate, "ROR names without a language code, number and pc of total orgs", 
                        num_wolc_ror, Some(pc_wolc_ror), pool).await?;

    // Consider non-company organisations only.

    let num_ncmp_wolc_ror = get_count(r#"select count(n.id) from 
                    src.names n
                    inner join src.admin_data ad
                    on n.id = ad.id 
                    where ad.n_is_company = 0
                    and n.is_ror_name = true
                    and n.lang_code is null"#, pool).await?;   
    let pc_ncmp_wolc_ror = get_pc(num_ncmp_wolc_ror, num_ncmp_orgs); 
    store_singleton(vcode, vdate, "Non company ROR names without a language code, number and pc of total non-company orgs", 
    num_ncmp_wolc_ror, Some(pc_ncmp_wolc_ror), pool).await?;

    Ok(())
}


pub async fn get_count (sql_string: &str, pool: &Pool<Postgres>) -> Result<i64, AppError> {
    let res = sqlx::query_scalar(sql_string)
    .fetch_one(pool)
    .await?;
    Ok(res)
}


fn get_pc (top:i64, bottom:i64) -> f64 {
    if bottom == 0
    { 0.0 }
    else {
        let res = ((top as f64) * 100.0) / bottom as f64;
        (res * 100.0).round() / 100.0  // return number to 2 decimal places
    }
}


async fn store_singleton(vcode: &String, vdate: NaiveDate, description: &str, number: i64, pc: Option<f64>, pool: &Pool<Postgres>)-> Result<(), AppError> {

    let sql = format!(r#"INSERT INTO smm.singletons (vcode, vdate, 
    description, number, pc) values($1, $2, $3, $4, $5)"#);
    sqlx::query(&sql)
    .bind(vcode.clone()).bind(vdate).bind(description).bind(number).bind(pc)
    .execute(pool).await?;
    Ok(())
}


async fn store_summary(rows: Vec<TypeRow>, pool: &Pool<Postgres>, att_type: i32, att_name: &str) -> Result<(), AppError> {

    for t in rows {
        let sql = r#"INSERT into smm.attributes_summary (vcode, vdate, att_type, att_name, 
        id, name, number_atts, pc_of_atts, number_orgs, pc_of_orgs)
        values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"#;
        sqlx::query(sql)
        .bind(t.vcode).bind(t.vdate).bind(att_type).bind(att_name).bind(t.id).bind(t.name)
        .bind(t.number_atts).bind::<f64>(t.pc_of_atts.into()).bind(t.number_orgs).bind::<f64>(t.pc_of_orgs.into())
        .execute(pool)
        .await?;
    }
    Ok(())
}


async fn store_distrib(rows: Vec<DistribRow>, count_type: &str, pool: &Pool<Postgres>)-> Result<(), AppError> {

    let sql = format!(r#"INSERT INTO smm.count_distributions (vcode, vdate, 
    count_type, count, num_of_orgs, pc_of_orgs) values($1, $2, $3, $4, $5, $6)"#);
    for r in rows {
        sqlx::query(&sql)
        .bind(r.vcode).bind(r.vdate).bind(count_type)
        .bind(r.count).bind(r.num_of_orgs).bind(r.pc_of_orgs)
        .execute(pool)
        .await?;
    }
    Ok(())
}


async fn store_ranked_distrib(vcode: &String, vdate:NaiveDate, rows: &Vec<RankedRow>, pool: &Pool<Postgres>, remainder_name: &str,
    dist_type : i32, entity_total: i64, base_set_total: i64) -> Result<(), AppError> {

    let mut i = 0;
    let mut rest_total = 0;

    for r in rows {
        i += 1;
        if i < 26 {
            sqlx::query(r#"INSERT INTO smm.ranked_distributions (vcode, vdate, dist_type, rank, entity, 
            number, pc_of_entities, pc_of_base_set) 
            values($1, $2, $3, $4, $5, $6, $7, $8)"#)
            .bind(r.vcode.clone()).bind(r.vdate).bind(dist_type).bind(i)
            .bind(r.entity.clone()).bind(r.number).bind(r.pc_of_entities).bind(r.pc_of_base_set)
            .execute(pool)
            .await?;
        }
        else {
            rest_total += r.number;
        } 
    }
    if rest_total > 0 {

        let rest_ne_pc: f64 = get_pc(rest_total, entity_total).into();
        let rest_total_pc: f64 = get_pc(rest_total, base_set_total).into();

        sqlx::query(r#"INSERT INTO smm.ranked_distributions (vcode, vdate, dist_type, rank, entity, 
        number, pc_of_entities, pc_of_base_set) 
        values($1, $2, $3, $4, $5, $6, $7, $8)"#)
        .bind(vcode).bind(vdate).bind(dist_type).bind(26)
        .bind(remainder_name).bind(rest_total).bind(rest_ne_pc).bind(rest_total_pc)
        .execute(pool)
        .await?;
    }
    Ok(())
}


async fn get_rel_imbalance(f1_type: u8, f2_type: u8, pool: &Pool<Postgres>) -> Result<i64, AppError> {
 
    let sql = format!(r"select count(f1.id) from
          (select id, related_id from src.relationships where rel_type = {}) as f1
          left join
          (select id, related_id from src.relationships where rel_type = {}) as f2
          on f1.id = f2.related_id 
          and f1.related_id = f2.id
          where f2.id is null;", f1_type, f2_type);
           
    let res = sqlx::query_scalar(&sql)
    .fetch_one(pool)
    .await?;

    Ok(res)
  }