use sqlx::{Pool, Postgres};
use crate::AppError;
use super::smm_structs::{RorVersion, DistribRow, RankedRow, TypeRow};


pub async fn delete_any_existing_data(v: &RorVersion,pool: &Pool<Postgres>) -> Result<(), AppError> {
   
    // format!() macro does not seem to recognise apostrrophes, even when escaped (???)

    let wc = " WHERE vcode = \'".to_string() + &v.vcode
                                 + "\' AND vdate = \'" + &v.vdate.to_string() + "\'::date;";
        
    let del_sql = format!(r#"DELETE from smm.version_summary {}
                DELETE from smm.name_summary {}
                DELETE from smm.name_lang_code {}
                DELETE from smm.name_ror {}
                DELETE from smm.count_distributions {}
                DELETE from smm.ranked_distributions {}
                DELETE from smm.attributes_summary {}
                DELETE from smm.type_name_lang_code {}
                DELETE from smm.relationships_summary {}
                DELETE from smm.type_relationship {}"#
                , wc, wc, wc, wc, wc, wc, wc, wc, wc, wc);

   sqlx::raw_sql(&del_sql).execute(pool).await?;
   Ok(())
}


pub async fn store_name_summary(v: &RorVersion, pool: &Pool<Postgres>, num_names: i64) -> Result<(), AppError> {

    let num_label = get_count("select count(*) from src.names where name_type = 5", pool).await?;
    let num_alias = get_count("select count(*) from src.names where name_type = 7", pool).await?;
    let num_acronym = get_count("select count(*) from src.names where name_type = 10", pool).await?;
    let num_nacro = get_count("select count(*) from src.names where name_type <> 10", pool).await?;
      
    let pc_label = get_pc (num_label,num_names);
    let pc_alias = get_pc (num_alias,num_names);
    let pc_acronym = get_pc (num_acronym,num_names);
    let pc_nacro = get_pc (num_nacro,num_names);

    let num_nacro_ne = get_count("select count(*) from src.names where name_type <> 10 and lang_code <> 'en'", pool).await?;
    let pc_nacro_ne = get_pc (num_nacro_ne,num_nacro);   
    let num_nltn = get_count("select count(*) from src.names where script_code <> 'Latn'", pool).await?; 
    let pc_nltn = get_pc (num_nltn,num_nacro);  

    let sql = r#"INSERT into smm.name_summary (vcode, vdate, total, 
    num_label, num_alias, num_nacro, num_acronym, 
    pc_label,  pc_alias, pc_nacro, pc_acronym, 
    num_nacro_ne, pc_nacro_ne, num_nltn, pc_nltn)
    values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)"#;

    sqlx::query(sql)
    .bind(v.vcode.clone()).bind(v.vdate).bind(num_names)
    .bind(num_label).bind(num_alias).bind(num_nacro).bind(num_acronym)
    .bind(pc_label).bind(pc_alias).bind(pc_nacro).bind(pc_acronym)
    .bind(num_nacro_ne).bind(pc_nacro_ne).bind(num_nltn).bind(pc_nltn)
    .execute(pool)
    .await?;

    // 'wolc' = without lang code

    let total_wolc = get_count("select count(*) from src.names where lang_code is null", pool).await?;
    let pc_total_wolc = get_pc (total_wolc,num_names);
    
    let num_label_wolc = get_count(r#"select count(*) from src.names 
                                                     where name_type = 5 and lang_code is null"#, pool).await?;
    let num_alias_wolc = get_count(r#"select count(*) from src.names 
                                                     where name_type = 7 and lang_code is null"#, pool).await?;
    let num_acro_wolc = get_count(r#"select count(*) from src.names 
                                                     where name_type = 10 and lang_code is null"#, pool).await?;
    let num_nacro_wolc = get_count(r#"select count(*) from src.names 
                                                     where name_type <> 10 and lang_code is null"#, pool).await?;
    
    let pc_label_wolc = get_pc (num_label_wolc,num_label);
    let pc_alias_wolc = get_pc (num_alias_wolc,num_alias);
    let pc_acro_wolc = get_pc (num_acro_wolc,num_acronym);
    let pc_nacro_wolc = get_pc (num_nacro_wolc,num_nacro);

    // Consider non company, non acronym names.

    let num_ncmpacr = get_count(r#"select count(*) from 
                                                    src.names n
                                                    inner join 
                                                    (   select cd.id from src.core_data cd
                                                        left join 
                                                            (select id from src.type 
                                                            where org_type = 400) cmps 
                                                        on cd.id = cmps.id
                                                        where cmps.id is null) ncorgs
                                                    on n.id = ncorgs.id
                                                    where name_type <> 10"#, pool).await?;  

    let num_ncmpacr_wolc = get_count(r#"select count(*) from 
                                                    src.names n
                                                    inner join 
                                                    (   select cd.id from src.core_data cd
                                                        left join 
                                                            (select id from src.type 
                                                            where org_type = 400) cmps 
                                                        on cd.id = cmps.id
                                                        where cmps.id is null) ncorgs
                                                    on n.id = ncorgs.id
                                                    where name_type <> 10
                                                    and n.lang_code is null"#, pool).await?;  

    let pc_ncmpacr_wolc = get_pc (num_ncmpacr_wolc,num_ncmpacr);

    let sql = r#"INSERT into smm.name_lang_code (vcode, vdate, total, 
    total_wolc, label_wolc, alias_wolc, acro_wolc, nacro_wolc, ncmpacr_wolc,
    pc_total_wolc, pc_label_wolc, pc_alias_wolc, pc_acro_wolc, pc_nacro_wolc, pc_ncmpacr_wolc)
    values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)"#;

    sqlx::query(sql)
    .bind(v.vcode.clone()).bind(v.vdate).bind(num_names)
    .bind(total_wolc).bind(num_label_wolc).bind(num_alias_wolc)
    .bind(num_acro_wolc).bind(num_nacro_wolc).bind(num_ncmpacr_wolc)
    .bind(pc_total_wolc).bind(pc_label_wolc).bind(pc_alias_wolc)
    .bind(pc_acro_wolc).bind(pc_nacro_wolc).bind(pc_ncmpacr_wolc)
    .execute(pool)
    .await?;

    Ok(())
    
}


pub async fn store_name_ror(v: &RorVersion, pool: &Pool<Postgres>) -> Result<(), AppError> {

    let num_label_ror = get_count(r#"select count(*) from src.names 
                                                        where name_type = 5 and is_ror_name = true"#, pool).await?; 
    let num_label_nror = get_count(r#"select count(*) from src.names 
                                                        where name_type = 5 and is_ror_name = false"#, pool).await?; 
    let num_nlabel_ror = get_count(r#"select count(*) from src.names 
                                                        where name_type <> 5 and is_ror_name = true"#, pool).await?; 
    let pc_nlabel_ror = get_pc(num_nlabel_ror, v.num_orgs);                                                   
    let num_en_ror = get_count(r#"select count(*) from src.names 
                                                        where is_ror_name = true and lang_code = 'en'"#, pool).await?;                                                    
    let num_nen_ror = get_count(r#"select count(*) from src.names 
                                                        where is_ror_name = true and lang_code <> 'en' and lang_code is not null"#, pool).await?; 
    let num_wolc_ror = get_count(r#"select count(*) from src.names 
                                                        where is_ror_name = true and lang_code is null"#, pool).await?; 
                                                   
    let pc_en_ror = get_pc(num_en_ror, v.num_orgs);
    let pc_nen_ror = get_pc(num_nen_ror, v.num_orgs);
    let pc_wolc_ror = get_pc(num_wolc_ror, v.num_orgs); 
    
    // Consider non-company organisations only.

    let num_ncmp_orgs = get_count(r#"select count(*) from src.core_data cd
                                                    left join 
                                                        (select id from src.type 
                                                        where org_type = 400) cmps 
                                                    on cd.id = cmps.id
                                                    where cmps.id is null"#, pool).await?;
    let num_ncmp_wolc_ror = get_count(r#"select count(*) from 
                                                    src.names n
                                                    inner join 
                                                    (
                                                        select cd.id from src.core_data cd
                                                        left join 
                                                            (select id from src.type 
                                                            where org_type = 400) cmps 
                                                        on cd.id = cmps.id
                                                        where cmps.id is null) ncorgs
                                                    on n.id = ncorgs.id
                                                    where is_ror_name = true
                                                    and n.lang_code is null"#, pool).await?;   
    let pc_ncmp_wolc_ror = get_pc(num_ncmp_wolc_ror, num_ncmp_orgs); 

    let sql = r#"INSERT into smm.name_ror (vcode, vdate, num_label_ror, num_label_nror,
    num_nlabel_ror, pc_nlabel_ror, num_en_ror, num_nen_ror, num_wolc_ror, num_ncmp_wolc_ror, 
    pc_en_ror, pc_nen_ror, pc_wolc_ror, pc_ncmp_wolc_ror)
    values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)"#;

    sqlx::query(sql)
    .bind(v.vcode.clone()).bind(v.vdate).bind(num_label_ror)
    .bind(num_label_nror).bind(num_nlabel_ror).bind(pc_nlabel_ror)
    .bind(num_en_ror).bind(num_nen_ror).bind(num_wolc_ror).bind(num_ncmp_wolc_ror)
    .bind(pc_en_ror).bind(pc_nen_ror).bind(pc_wolc_ror).bind(pc_ncmp_wolc_ror)
    .execute(pool)
    .await?;

    Ok(())

}


pub async fn store_name_count_distrib(v: &RorVersion, pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = v.dvdd.clone() + r#"n_names as count, count(id) as num_of_orgs, 
                        round(count(id) * 10000 :: float / "# + &(v.num_orgs.to_string()) + r#":: float)/100 :: float as pc_of_orgs
                        from src.admin_data
                        group by n_names
                        order by n_names;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    store_distrib(rows, "names", pool).await
}


pub async fn store_label_count_distrib(v: &RorVersion, pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = v.dvdd.clone() + r#"n_labels as count, count(id) as num_of_orgs, 
                        round(count(id) * 10000 :: float / "# + &(v.num_orgs.to_string()) + r#":: float)/100 :: float as pc_of_orgs
                        from src.admin_data
                        group by n_labels
                        order by n_labels;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    store_distrib(rows, "labels", pool).await
}


pub async fn store_alias_count_distrib(v: &RorVersion, pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = v.dvdd.clone() + r#"n_aliases as count, count(id) as num_of_orgs, 
                        round(count(id) * 10000 :: float / "# + &(v.num_orgs.to_string()) + r#":: float)/100 :: float as pc_of_orgs
                        from src.admin_data
                        group by n_aliases
                        order by n_aliases;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    store_distrib(rows, "aliases", pool).await
}


pub async fn store_acronym_count_distrib(v: &RorVersion, pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = v.dvdd.clone() + r#"n_acronyms as count, count(id) as num_of_orgs, 
                        round(count(id) * 10000 :: float / "# + &(v.num_orgs.to_string()) + r#":: float)/100 :: float as pc_of_orgs
                        from src.admin_data
                        group by n_acronyms
                        order by n_acronyms;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    store_distrib(rows, "acronyms", pool).await
}


pub async fn store_lang_code_distrib(v: &RorVersion, pool: &Pool<Postgres>, num_names: i64) -> Result<(), AppError> {
    
    let total_of_ne = get_count("select count(*) from src.names where lang_code <> 'en'", pool).await?;

    let sql = v.dvdd.clone() + r#"lc.name as entity, count(n.id) as number,
                        round(count(n.id) * 10000 :: float / "# + &total_of_ne.to_string() + r#":: float)/100 :: float as pc_of_entities,
                        round(count(n.id) * 10000 :: float / "# + &(num_names.to_string()) + r#":: float)/100 :: float as pc_of_base_set
                        from src.names n inner join lup.lang_codes lc 
                        on n.lang_code = lc.code 
                        where lang_code <> 'en'
                        group by lc.name
                        order by count(n.id) desc;"#;
    let rows: Vec<RankedRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    store_ranked_distrib(&v, &rows, pool, "Remaining languages", 1, 
           total_of_ne, num_names).await?;

    Ok(())

}


pub async fn store_script_code_distrib(v: &RorVersion, pool: &Pool<Postgres>, num_names: i64) -> Result<(), AppError> {
  
    let total_of_nltn = get_count("select count(*) from src.names where script_code <> 'Latn'", pool).await?;

    let sql = v.dvdd.clone() + r#"ls.iso_name as entity, count(n.id) as number,
                        round(count(id) * 10000 :: float / "# + &total_of_nltn.to_string() + r#":: float)/100 :: float as pc_of_entities,
                        round(count(id) * 10000 :: float / "# + &(num_names.to_string()) + r#":: float)/100 :: float as pc_of_base_set
                        from src.names n inner join lup.lang_scripts ls 
                        on n.script_code = ls.code 
                        where script_code <> 'Latn'
                        group by ls.iso_name
                        order by count(n.id) desc; "#;
    let rows: Vec<RankedRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    store_ranked_distrib(&v, &rows, pool, "Remaining scripts", 2,
                        total_of_nltn, num_names).await?;

    Ok(())
}


pub async fn store_country_distrib(v: &RorVersion, pool: &Pool<Postgres>, num_locs: i64) -> Result<(), AppError> {

    // At the moment num_of_locs = num_of_orgs - might change!

    let total_of_nus = get_count("select count(*) from src.locations where country_code <> 'US'", pool).await?;

    let sql = v.dvdd.clone() + r#"country_name as entity, count(id) as number, 
                      round(count(id) * 10000 :: float / "# + &total_of_nus.to_string() + r#":: float)/100 :: float as pc_of_entities,
                      round(count(id) * 10000 :: float / "# + &(num_locs.to_string()) + r#":: float)/100 :: float as pc_of_base_set
                      from src.locations
                      group by country_name
                      order by count(country_name) desc;"#;
    let rows: Vec<RankedRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    store_ranked_distrib(&v, &rows, pool, "Remaining countries", 3,
                        total_of_nus, num_locs).await?;
    
    Ok(())
}


pub async fn store_type_summary(v: &RorVersion, pool: &Pool<Postgres>, num_types: i64) -> Result<(), AppError> {

    let sql  = "".to_string() + r#"select gt.id, gt.name, count(t.id) as number,
                        round(count(t.id) * 10000 :: float /  "# + &num_types.to_string() + r#":: float)/100 :: float as pc_of_atts,
                        round(count(t.id) * 10000 :: float /  "# + &v.num_orgs.to_string() + r#":: float)/100 :: float as pc_of_orgs
                        from lup.ror_org_types gt
                        inner join src.type t
                        on gt.id = t.org_type 
                        group by gt.id, gt.name
                        order by gt.id;"#;
    let rows: Vec<TypeRow> = sqlx::query_as(&sql).fetch_all(pool).await?;

    for t in rows {

        let sql = r#"INSERT into smm.attributes_summary (vcode, vdate, att_type, att_name, 
        id, name, number, pc_of_atts, pc_of_orgs)
        values ($1, $2, $3, $4, $5, $6, $7, $8, $9)"#;
            
        sqlx::query(sql)
        .bind(v.vcode.clone()).bind(v.vdate).bind(2).bind("org types").bind(t.id).bind(t.name)
        .bind(t.number).bind(t.pc_of_atts).bind(t.pc_of_orgs)
        .execute(pool)
        .await?;
    }

    Ok(())

}
   

pub async fn store_type_count_distrib(v: &RorVersion, pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = v.dvdd.clone() + r#"n_types as count, count(id) as num_of_orgs, 
                      round(count(id) * 10000 :: float / "# + &(v.num_orgs.to_string()) + r#":: float)/100 :: float as pc_of_orgs
                      from src.admin_data
                      group by n_types
                      order by n_types;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    store_distrib(rows, "org_types", pool).await
}


pub async fn store_types_with_lang_code(v: &RorVersion, pool: &Pool<Postgres>) -> Result<(), AppError> {

    // For each org type, and each of the three name types (therefore 9 x 3 rows),
    // get total number of names and numbers with / without lang codes.

    #[derive(sqlx::FromRow)]
    struct OrgType {
        id: i32, 
        name: String,
    }

    #[derive(sqlx::FromRow)]
    struct NameLCRow {
        ntype: String,
        total: i64,
        names_wlc: i64,
        names_wolc: i64,
        names_wlc_pc: f64,
        names_wolc_pc: f64
    }

    // Get the organisation type categories.

    let sql  = r#"select id, name
    from lup.ror_org_types
    order by id;"#;
    let rows: Vec<OrgType> = sqlx::query_as(sql).fetch_all(pool).await?;

    for t in rows {

        // Get the data on the names linkede to these organisations

        let lc_sql = r#"select 
                case name_type
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
                    where t.org_type = "#.to_string() + &(t.id.to_string()) + r#") ns
                group by ns.name_type 
                order by ns.name_type;"#;
            let rows: Vec<NameLCRow> = sqlx::query_as(&lc_sql).fetch_all(pool).await?;

            // Store the individual rows.

            for r in rows {
            sqlx::query(r#"INSERT INTO smm.type_name_lang_code (vcode, vdate, org_type, name_type, 
                             names_num, names_wlc, names_wolc, names_wlc_pc, names_wolc_pc) 
                             values($1, $2, $3, $4, $5, $6, $7, $8, $9)"#)
            .bind(v.vcode.clone())
            .bind(v.vdate)
            .bind(t.name.clone())
            .bind(r.ntype).bind(r.total)
            .bind(r.names_wlc).bind(r.names_wolc)
            .bind(r.names_wlc_pc).bind(r.names_wolc_pc)
            .execute(pool)
            .await?;
        }
    }

    Ok(())
}


pub async fn store_ext_ids_summary(v: &RorVersion, pool: &Pool<Postgres>, num_ids: i64) -> Result<(), AppError> {
      
    let sql = "".to_string() + r#"select it.id, it.name, count(t.id) as number,
        round(count(t.id) * 10000 :: float / "# + &num_ids.to_string() + r#":: float)/100 :: float as pc_of_atts,
        round(count(t.id) * 10000 :: float / "# + &v.num_orgs.to_string() + r#":: float)/100 :: float as pc_of_orgs
        from lup.ror_id_types it
        inner join src.external_ids t
        on it.id = t.id_type 
        group by it.id, it.name
        order by it.id;"#;
    let rows: Vec<TypeRow> = sqlx::query_as(&sql).fetch_all(pool).await?;

    for t in rows {

        let sql = r#"INSERT into smm.attributes_summary (vcode, vdate, att_type, att_name,
        id, name, number, pc_of_atts, pc_of_orgs)
        values ($1, $2, $3, $4, $5, $6, $7, $8, $9)"#;
            
        sqlx::query(sql)
        .bind(v.vcode.clone()).bind(v.vdate).bind(3).bind("ext id types").bind(t.id).bind(t.name)
        .bind(t.number).bind(t.pc_of_atts).bind(t.pc_of_orgs)
        .execute(pool)
        .await?;
    }

    Ok(())
}


pub async fn store_ext_ids_count_distrib(v: &RorVersion, pool: &Pool<Postgres>) -> Result<(), AppError> {

  let sql = v.dvdd.clone() + r#"n_ext_ids as count, count(id) as num_of_orgs, 
                    round(count(id) * 10000 :: float / "# + &(v.num_orgs.to_string()) + r#":: float)/100 :: float as pc_of_orgs
                    from src.admin_data where n_ext_ids <> 0
                    group by n_ext_ids
                    order by n_ext_ids;"#;
  let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
  store_distrib(rows, "ext_ids", pool).await
}


pub async fn store_links_summary(v: &RorVersion, pool: &Pool<Postgres>, num_links: i64) -> Result<(), AppError> {

   let sql = "".to_string() + r#"select lt.id, lt.name, count(t.id) as number,
        round(count(t.id) * 10000 :: float / "# + &num_links.to_string() + r#":: float)/100 :: float as pc_of_atts,
        round(count(t.id) * 10000 :: float / "# + &v.num_orgs.to_string() + r#":: float)/100 :: float as pc_of_orgs
        from lup.ror_link_types lt
        inner join src.links t
        on lt.id = t.link_type 
        group by lt.id, lt.name
        order by lt.id;"#;
    let rows: Vec<TypeRow> = sqlx::query_as(&sql).fetch_all(pool).await?;

    for t in rows {

        let sql = r#"INSERT into smm.attributes_summary (vcode, vdate, att_type, att_name,
        id, name, number, pc_of_atts, pc_of_orgs)
        values ($1, $2, $3, $4, $5, $6, $7, $8, $9)"#;
            
        sqlx::query(sql)
        .bind(v.vcode.clone()).bind(v.vdate).bind(4).bind("link types").bind(t.id).bind(t.name)
        .bind(t.number).bind(t.pc_of_atts).bind(t.pc_of_orgs)
        .execute(pool)
        .await?;
    }

    Ok(())
}


pub async fn store_links_count_distrib(v: &RorVersion, pool: &Pool<Postgres>) -> Result<(), AppError> {

  let sql = v.dvdd.clone() + r#"n_links as count, count(id) as num_of_orgs, 
                    round(count(id) * 10000 :: float / "# + &(v.num_orgs.to_string()) + r#":: float)/100 :: float as pc_of_orgs
                    from src.admin_data
                    group by n_links
                    order by n_links;"#;
  let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
  store_distrib(rows, "links", pool).await
}


pub async fn store_relationships_summary(v: &RorVersion, pool: &Pool<Postgres>, total_links: i64) -> Result<(), AppError> {

    let parent_lnks =  get_count("select count(*) from src.relationships where rel_type = 1", pool).await?;   
    let child_lnks =  get_count("select count(*) from src.relationships where rel_type = 2", pool).await?;      
    let rel_lnks =  get_count("select count(*) from src.relationships where rel_type = 3", pool).await?;   
    let pred_lnks =  get_count("select count(*) from src.relationships where rel_type = 4", pool).await?;      
    let succ_lnks =  get_count("select count(*) from src.relationships where rel_type = 5", pool).await?;   
 
    let parent_lnks_pc =  get_pc(parent_lnks, total_links);    
    let child_lnks_pc =  get_pc(child_lnks, total_links);    
    let rel_lnks_pc =  get_pc(rel_lnks, total_links);      
    let pred_lnks_pc =  get_pc(pred_lnks, total_links);       
    let succ_lnks_pc =  get_pc(succ_lnks, total_links);     
    
    let parent_orgs =  get_count("select count(*) from src.admin_data where n_parrels > 0", pool).await?;   
    let child_orgs =  get_count("select count(*) from src.admin_data where n_chrels > 0", pool).await?;
    let parch_orgs =  get_count("select count(*) from src.admin_data where n_chrels > 0 and n_parrels > 0", pool).await?;
    let rel_orgs  =  get_count("select count(*) from src.admin_data where n_relrels > 0", pool).await?;   
    let pred_orgs =  get_count("select count(*) from src.admin_data where n_predrels > 0", pool).await?;    
    let succ_orgs =  get_count("select count(*) from src.admin_data where n_sucrels > 0", pool).await?;  

    let parent_orgs_pc =  get_pc(parent_orgs, v.num_orgs);   
    let child_orgs_pc =  get_pc(child_orgs, v.num_orgs);   
    let parch_orgs_pc =  get_pc(parch_orgs, v.num_orgs);   
    let rel_orgs_pc  =  get_pc(rel_orgs, v.num_orgs);      
    let pred_orgs_pc =  get_pc(pred_orgs, v.num_orgs);      
    let succ_orgs_pc =  get_pc(succ_orgs, v.num_orgs);   

    let par_no_child = get_rel_imbalance(1, 2, pool).await.unwrap();
    let par_no_parent = get_rel_imbalance(2, 1, pool).await.unwrap();
    let non_recip_pc = par_no_child + par_no_parent;
    let non_recip_rr = get_rel_imbalance(3, 3, pool).await.unwrap();
    let pred_no_succ = get_rel_imbalance(4, 5, pool).await.unwrap();
    let succ_no_pred = get_rel_imbalance(5, 4, pool).await.unwrap();
    let non_recip_ps = pred_no_succ + succ_no_pred;
     
    let sql = r#"INSERT into smm.relationships_summary (vcode, vdate, total_lnks, 
    parent_lnks, child_lnks, rel_lnks, pred_lnks, succ_lnks,
    parent_lnks_pc, child_lnks_pc, rel_lnks_pc, pred_lnks_pc, succ_lnks_pc,
    total_orgs, parent_orgs, child_orgs, parch_orgs, rel_orgs, pred_orgs, succ_orgs,
    parent_orgs_pc, child_orgs_pc, parch_orgs_pc, rel_orgs_pc, pred_orgs_pc, succ_orgs_pc,
    non_recip_pc, non_recip_rr, non_recip_ps)
    values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
            $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29)"#;

    sqlx::query(sql)
    .bind(v.vcode.clone()).bind(v.vdate).bind(total_links)
    .bind(parent_lnks).bind(child_lnks).bind(rel_lnks).bind(pred_lnks).bind(succ_lnks)
    .bind(parent_lnks_pc).bind(child_lnks_pc).bind(rel_lnks_pc).bind(pred_lnks_pc).bind(succ_lnks_pc)
    .bind(v.num_orgs).bind(parent_orgs).bind(child_orgs).bind(parch_orgs)
    .bind(rel_orgs).bind(pred_orgs).bind(succ_orgs)
    .bind(parent_orgs_pc).bind(child_orgs_pc).bind(parch_orgs_pc)
    .bind(rel_orgs_pc).bind(pred_orgs_pc).bind(succ_orgs_pc)
    .bind(non_recip_pc).bind(non_recip_rr).bind(non_recip_ps)
    .execute(pool)
    .await?;
    
    Ok(())

}


pub async fn store_types_and_relationships(v: &RorVersion, pool: &Pool<Postgres>) -> Result<(), AppError> {

    // For each org type, and each of the 5 relationship types (therefore up to 9 x 5 rows),
    // get number of orgs having one or more relationships of each type, and the total number of orgs involved.

    #[derive(sqlx::FromRow)]
    struct OrgType {
        type_id: i32, 
        name: String,
        org_num: i64, 
    }

    #[derive(sqlx::FromRow)]
    struct TypeRelRow {
        rtype: String,
        num_rels: i64,
        num_orgs: i64,
        num_orgs_pc: f64,
    }

    // Get the organisation type categories and total numbers.

    let sql  = r#"select org_type as type_id, p.name, 
    count(distinct t.id) as org_num
    from src.type t
    inner join lup.ror_org_types p
    on t.org_type = p.id
    group by org_type, p.name
    order by org_type;"#;
    let rows: Vec<OrgType> = sqlx::query_as(sql).fetch_all(pool).await?;

    for t in rows {

        // Get the data on the names linked to these organisations

        let tr_sql = r#"select case rs.rel_type
            when 1 then 'parent'  
            when 2 then 'child' 
            when 3 then 'related' 
            when 4 then 'predecessor' 
            when 5 then 'successor' 
            end as rtype, 
            count(rs.id) as num_rels,
            count(distinct rs.id) as num_orgs,
            round((count(distinct rs.id) * 10000::float /"#.to_string() 
                           + &(t.org_num.to_string()) + r#"::float)) /100::float as num_orgs_pc
            from
                (select r.id, r.rel_type
                from src.relationships r 
                inner join src.type t
                on r.id = t.id
                where t.org_type ="# + &(t.type_id.to_string()) + r#") rs 
            group by rs.rel_type 
            order by rs.rel_type;"#;

            let rows: Vec<TypeRelRow> = sqlx::query_as(&tr_sql).fetch_all(pool).await?;

            // Store the individual rows.
 
             for r in rows {
            sqlx::query(r#"INSERT INTO smm.type_relationship (vcode, vdate, org_type, org_type_total, 
                             rel_type, num_links, num_orgs, num_orgs_total, num_orgs_pc) 
                             values($1, $2, $3, $4, $5, $6, $7, $8, $9)"#)
            .bind(v.vcode.clone()).bind(v.vdate)
            .bind(t.name.clone()).bind(t.org_num)
            .bind(r.rtype).bind(r.num_rels).bind(r.num_orgs)
            .bind(t.org_num).bind(r.num_orgs_pc)
            .execute(pool)
            .await?;
        }
    }

    Ok(())
}


pub async fn store_locs_count_distrib(v: &RorVersion, pool: &Pool<Postgres>) -> Result<(), AppError> {

  let sql = v.dvdd.clone() + r#"n_locs as count, count(id) as num_of_orgs, 
                     round(count(id) * 10000 :: float / "# + &(v.num_orgs.to_string()) + r#":: float)/100 :: float as pc_of_orgs
                     from src.admin_data
                     group by n_locs
                     order by n_locs;"#;
  let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
  store_distrib(rows, "locs", pool).await
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


pub async fn store_ranked_distrib(v: &RorVersion, rows: &Vec<RankedRow>, pool: &Pool<Postgres>, remainder_name: &str,
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
        .bind(v.vcode.clone()).bind(v.vdate).bind(dist_type).bind(26)
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