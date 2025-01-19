use sqlx::{Pool, Postgres};
use crate::AppError;
use super::smm_structs::{RorVersion, DistribRow, RankedRow, TypeRow};


pub async fn delete_any_existing_data(v: &RorVersion,pool: &Pool<Postgres>) -> Result<(), AppError> {
   
    // format!() macro does not seem to recognise apostrrophes, even when escaped (???)

    let wc = " WHERE vcode = \'".to_string() + &v.vcode
                                 + "\' AND vdate = \'" + &v.vdate.to_string() + "\'::date;";
        
    let del_sql = format!(r#"DELETE from smm.version_summary {}
                DELETE from smm.attributes_summary {}
                DELETE from smm.count_distributions {}
                DELETE from smm.ranked_distributions {}
                DELETE from smm.singletons {}
                DELETE from smm.name_ror {}
                DELETE from smm.type_name_lang_code {}
                DELETE from smm.type_relationship {}"#
                , wc, wc, wc, wc, wc, wc, wc, wc);

   sqlx::raw_sql(&del_sql).execute(pool).await?;
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
            .bind(v.vcode.clone()).bind(v.vdate)
            .bind(t.name.clone()).bind(r.ntype).bind(r.total)
            .bind(r.names_wlc).bind(r.names_wolc).bind(r.names_wlc_pc).bind(r.names_wolc_pc)
            .execute(pool)
            .await?;
        }
    }

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
            sqlx::query(r#"INSERT INTO smm.type_relationship (vcode, vdate, org_type, 
                            rel_type, num_links, num_orgs, num_orgs_total, num_orgs_pc) 
                            values($1, $2, $3, $4, $5, $6, $7, $8)"#)
            .bind(v.vcode.clone()).bind(v.vdate)
            .bind(t.name.clone()).bind(r.rtype).bind(r.num_rels).bind(r.num_orgs)
            .bind(t.org_num).bind(r.num_orgs_pc)
            .execute(pool)
            .await?;
        }
    }

    Ok(())
}


pub async fn store_singletons(v: &RorVersion, num_names: i64, pool: &Pool<Postgres>) -> Result<(), AppError> {

    let num_nacro = get_count("select count(*) from src.names where name_type <> 10", pool).await?;
    let num_nacro_ne = get_count("select count(*) from src.names where name_type <> 10 and lang_code <> 'en'", pool).await?;
    let pc_nacro_ne = get_pc (num_nacro_ne,num_nacro);   
    let num_nltn = get_count("select count(*) from src.names where script_code <> 'Latn'", pool).await?; 
    let pc_nltn = get_pc (num_nltn,num_nacro);  

    store_singleton(v, "Non acronym names that are not English, numbers and pc of total names",  num_nacro_ne, Some(pc_nacro_ne), pool).await?;
    store_singleton(v, "Non acronym names that are not in Latin script, numbers and pc of total names",  num_nltn, Some(pc_nltn), pool).await?;

    let total_wolc = get_count("select count(*) from src.names where lang_code is null", pool).await?;
    let pc_total_wolc = get_pc (total_wolc,num_names);
    store_singleton(v, "Names that do not have a language code, numbers and pc of total names",  total_wolc, Some(pc_total_wolc), pool).await?;

    let parch_orgs =  get_count("select count(*) from src.admin_data where n_chrels > 0 and n_parrels > 0", pool).await?;
    let parch_orgs_pc =  get_pc(parch_orgs, v.num_orgs);   
    store_singleton(v, "Orgs both parent and child, numbers and pc of total orgs",  parch_orgs, Some(parch_orgs_pc), pool).await?;

    let par_no_child = get_rel_imbalance(1, 2, pool).await.unwrap();
    let par_no_parent = get_rel_imbalance(2, 1, pool).await.unwrap();
    let non_recip_pc = par_no_child + par_no_parent;
    let non_recip_rr = get_rel_imbalance(3, 3, pool).await.unwrap();
    let pred_no_succ = get_rel_imbalance(4, 5, pool).await.unwrap();
    let succ_no_pred = get_rel_imbalance(5, 4, pool).await.unwrap();
    let non_recip_ps = pred_no_succ + succ_no_pred;

    
    store_singleton(v, "Non-reciprocated parent-child relationships, numbers",  non_recip_pc, None, pool).await?;
    store_singleton(v, "Non-reciprocated 'related' relationships, numbers",  non_recip_rr, None, pool).await?;
    store_singleton(v, "Non-reciprocated pred-succ relationships, numbers",  non_recip_ps, None, pool).await?;
    
    Ok(())

}


pub async fn get_count (sql_string: &str, pool: &Pool<Postgres>) -> Result<i64, AppError> {
    let res = sqlx::query_scalar(sql_string)
    .fetch_one(pool)
    .await?;
    Ok(res)
}


pub fn get_pc (top:i64, bottom:i64) -> f64 {
    if bottom == 0
    { 0.0 }
    else {
        let res = ((top as f64) * 100.0) / bottom as f64;
        (res * 100.0).round() / 100.0  // return number to 2 decimal places
    }
}


pub async fn store_singleton(v: &RorVersion, description: &str, number: i64, pc: Option<f64>, pool: &Pool<Postgres>)-> Result<(), AppError> {

    let sql = format!(r#"INSERT INTO smm.singletons (vcode, vdate, 
    description, number, pc) values($1, $2, $3, $4, $5)"#);

    sqlx::query(&sql)
    .bind(v.vcode.clone()).bind(v.vdate).bind(description).bind(number).bind(pc)
    .execute(pool).await?;

    Ok(())
}


pub async fn store_summary(rows: Vec<TypeRow>, pool: &Pool<Postgres>, att_type: i32, att_name: &str) -> Result<(), AppError> {

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

pub async fn store_distrib(rows: Vec<DistribRow>, count_type: &str, pool: &Pool<Postgres>)-> Result<(), AppError> {

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


pub async fn get_rel_imbalance(f1_type: u8, f2_type: u8, pool: &Pool<Postgres>) -> Result<i64, AppError> {
 
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