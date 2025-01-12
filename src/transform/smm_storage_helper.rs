use sqlx::{Pool, Postgres};
use crate::AppError;
use chrono::NaiveDate;
use super::smm_structs::{RorVersion, DistribRow, CountryRow, LangCodeRow, ScriptCodeRow};


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
              + "DELETE from smm.type_summary " + &where_clause
              + "DELETE from smm.type_by_orgs_summary " + &where_clause
              + "DELETE from smm.type_count_distribution " + &where_clause
              + "DELETE from smm.type_name_lang_code " + &where_clause
              + "DELETE from smm.ext_ids_summary " + &where_clause
              + "DELETE from smm.ext_ids_count_distribution " + &where_clause
              + "DELETE from smm.links_summary " + &where_clause
              + "DELETE from smm.links_count_distribution " + &where_clause
              + "DELETE from smm.relationships_summary " + &where_clause
              + "DELETE from smm.type_relationship " + &where_clause
              + "DELETE from smm.country_distribution " + &where_clause
              + "DELETE from smm.locs_count_distribution " + &where_clause;

   sqlx::raw_sql(&del_sql).execute(pool).await?;

    Ok(())

}


pub async fn store_name_summary(v: &RorVersion, pool: &Pool<Postgres>, num_names: i64) -> Result<(), AppError> {

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
    let num_nacro = get_count("select count(*) from src.names where name_type <> 10", pool).await?;
    let pc_label = get_pc (num_label,num_names);
    let pc_alias = get_pc (num_alias,num_names);
    let pc_acronym = get_pc (num_acronym,num_names);
    let pc_nacro = get_pc (num_nacro,num_names);
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
    let pc_nacro_wolc = get_pc (num_nacro_wolc,num_nacro);
    let num_nacro_ne = get_count("select count(*) from src.names where name_type <> 10 and lang_code <> 'en'", pool).await?;
    let pc_nacro_ne = get_pc (num_nacro_ne,num_nacro);   
    let num_nltn = get_count("select count(*) from src.names where script_code <> 'Latn'", pool).await?; 
    let pc_nltn = get_pc (num_nltn,num_nacro);  

    let sql = r#"INSERT into smm.name_summary (vcode, vdate, total, total_wolc,
    pc_wolc, num_label, num_alias, num_acronym, num_nacro, pc_label, pc_alias, pc_acronym, pc_nacro,
    num_label_wolc, num_alias_wolc, num_acro_wolc, num_nacro_wolc, pc_label_wolc,
    pc_alias_wolc, pc_acro_wolc, pc_nacro_wolc, num_nacro_ne, pc_nacro_ne, num_nltn, pc_nltn)
    values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, 
            $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25)"#;

    sqlx::query(sql)
    .bind(v.vcode.clone()).bind(v.vdate).bind(num_names)
    .bind(total_wolc).bind(pc_wolc)
    .bind(num_label).bind(num_alias).bind(num_acronym).bind(num_nacro)
    .bind(pc_label).bind(pc_alias).bind(pc_acronym).bind(pc_nacro)
    .bind(num_label_wolc).bind(num_alias_wolc).bind(num_acro_wolc).bind(num_nacro_wolc)
    .bind(pc_label_wolc).bind(pc_alias_wolc).bind(pc_acro_wolc).bind(pc_nacro_wolc)
    .bind(num_nacro_ne).bind(pc_nacro_ne).bind(num_nltn).bind(pc_nltn)
    .execute(pool)
    .await?;

    Ok(())
    
  }

  
pub async fn store_name_ror(v: &RorVersion, pool: &Pool<Postgres>) -> Result<(), AppError> {

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

    let sql = r#"INSERT into smm.name_ror (vcode, vdate, num_label_ror, num_label_nror,
    num_nlabel_ror, pc_nlabel_ror, num_en_ror, num_nen_ror, num_wolc_ror, pc_en_ror, pc_nen_ror, pc_wolc_ror)
    values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"#;

    sqlx::query(sql)
    .bind(v.vcode.clone()).bind(v.vdate).bind(num_label_ror)
    .bind(num_label_nror).bind(num_nlabel_ror).bind(pc_nlabel_ror)
    .bind(num_en_ror).bind(num_nen_ror).bind(num_wolc_ror)
    .bind(pc_en_ror).bind(pc_nen_ror).bind(pc_wolc_ror)
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


pub async fn store_label_count_distrib(v: &RorVersion, pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = v.dvdd.clone() + r#"n_labels as count, count(id) as num_of_orgs, 
                        round(count(id) * 10000 :: float / "# + &(v.num_orgs.to_string()) + r#":: float)/100 :: float as pc_of_orgs
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


pub async fn store_alias_count_distrib(v: &RorVersion, pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = v.dvdd.clone() + r#"n_aliases as count, count(id) as num_of_orgs, 
                        round(count(id) * 10000 :: float / "# + &(v.num_orgs.to_string()) + r#":: float)/100 :: float as pc_of_orgs
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


pub async fn store_acronym_count_distrib(v: &RorVersion, pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = v.dvdd.clone() + r#"n_acronyms as count, count(id) as num_of_orgs, 
                        round(count(id) * 10000 :: float / "# + &(v.num_orgs.to_string()) + r#":: float)/100 :: float as pc_of_orgs
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


pub async fn store_lang_code_distrib(v: &RorVersion, pool: &Pool<Postgres>) -> Result<(), AppError> {
    
    let total_of_ne = get_count("select count(*) from src.names where lang_code <> 'en'", pool).await?;
    let sql = v.dvdd.clone() + r#"lang_code as lang, count(id) as num_of_names, 
                        round(count(id) * 10000 :: float / "# + &total_of_ne.to_string() + r#":: float)/100 :: float as pc_of_ne_names,
                        round(count(id) * 10000 :: float / "# + &(v.num_orgs.to_string()) + r#":: float)/100 :: float as pc_of_all_names
                        from src.names where lang_code <> 'en'
                        group by lang_code
                        order by lang_code;"#;
    let rows: Vec<LangCodeRow> = sqlx::query_as(&sql).fetch_all(pool).await?;

    let mut i = 0;
    let mut rest_total = 0;
    for r in rows {
        if i < 25 {
            sqlx::query(r#"INSERT INTO smm.ne_lang_code_distribution (vcode, vdate, lang, num_of_names, pc_of_ne_names, pc_of_all_names) 
                                values($1, $2, $3, $4, $5, $6)"#)
            .bind(r.vcode)
            .bind(r.vdate)
            .bind(r.lang)
            .bind(r.num_of_names)
            .bind(r.pc_of_ne_names)
            .bind(r.pc_of_all_names)
            .execute(pool)
            .await?;
        }
        else {
          rest_total += r.num_of_names;
        } 
        i += 1;
    }
    let rest_ne_pc = get_pc(rest_total, total_of_ne);
    let rest_total_pc = get_pc(rest_total, v.num_orgs);

    sqlx::query(r#"INSERT INTO smm.ne_lang_code_distribution (vcode, vdate, lang, num_of_names, pc_of_ne_names, pc_of_all_names) 
    values($1, $2, $3, $4, $5, $6)"#)
    .bind(v.vcode.clone())
    .bind(v.vdate)
    .bind("Remaining languages")
    .bind(rest_total)
    .bind(rest_ne_pc)
    .bind(rest_total_pc)
    .execute(pool)
    .await?;
    
    Ok(())
}


pub async fn store_script_code_distrib(v: &RorVersion, pool: &Pool<Postgres>) -> Result<(), AppError> {
  
    let total_of_nltn = get_count("select count(*) from src.names where script_code <> 'Latn'", pool).await?;
    let sql = v.dvdd.clone() + r#"script_code as script, count(id) as num_of_names, 
                        round(count(id) * 10000 :: float / "# + &total_of_nltn.to_string() + r#":: float)/100 :: float as pc_of_nl_names,
                        round(count(id) * 10000 :: float / "# + &(v.num_orgs.to_string()) + r#":: float)/100 :: float as pc_of_all_names
                        from src.names where script_code <> 'Latn'
                        group by script_code
                        order by script_code;"#;
    let rows: Vec<ScriptCodeRow> = sqlx::query_as(&sql).fetch_all(pool).await?;

    for r in rows {
        sqlx::query(r#"INSERT INTO smm.nl_lang_script_distribution (vcode, vdate, script, num_of_names, pc_of_nl_names, pc_of_all_names) 
                            values($1, $2, $3, $4, $5, $6)"#)
        .bind(r.vcode)
        .bind(r.vdate)
        .bind(r.script)
        .bind(r.num_of_names)
        .bind(r.pc_of_nl_names)
        .bind(r.pc_of_all_names)
        .execute(pool)
        .await?;
    }
    Ok(())

}


pub async fn store_type_summary(v: &RorVersion, pool: &Pool<Postgres>, num_types: i64) -> Result<(), AppError> {

    let government =  get_count("select count(*) from src.type where org_type = 100", pool).await?;   // num of orgs with type government
    let education =  get_count("select count(*) from src.type where org_type = 200", pool).await?;       // num of orgs with type education
    let healthcare =  get_count("select count(*) from src.type where org_type = 300", pool).await?;       // num of orgs with type healthcare
    let company =  get_count("select count(*) from src.type where org_type = 400", pool).await?;          // num of orgs with type company
    let nonprofit =  get_count("select count(*) from src.type where org_type = 500", pool).await?;        // num of orgs with type nonprofit
    let funder =  get_count("select count(*) from src.type where org_type = 600", pool).await?;           // num of orgs with type funder
    let facility=  get_count("select count(*) from src.type where org_type = 700", pool).await?;          // num of orgs with type facility
    let archive=  get_count("select count(*) from src.type where org_type = 800", pool).await?;           // num of orgs with type archive
    let other =  get_count("select count(*) from src.type where org_type = 900", pool).await?;            // num of orgs with type other
    
    let government_pc =  get_pc (government,num_types);   // pc of types with type government
    let education_pc =  get_pc (education,num_types);      // pc of types with type education
    let healthcare_pc =  get_pc (healthcare,num_types);     // pc of types with type healthcare
    let company_pc =  get_pc (company,num_types);        // pc of types with type company
    let nonprofit_pc =  get_pc (nonprofit,num_types);      // pc of types with type nonprofit
    let funder_pc =  get_pc (funder,num_types);         // pc of types with type funder
    let facility_pc =  get_pc (facility,num_types);       // pc of types with type facility
    let archive_pc =  get_pc (archive,num_types);        // pc of types with type archive
    let other_pc =  get_pc (other,num_types);          // pc of types with type other
    
    let sql = r#"INSERT into smm.type_summary (vcode, vdate, num_types, government,
    education, healthcare, company, nonprofit, funder, facility, archive, other,
    government_pc, education_pc, healthcare_pc, company_pc, nonprofit_pc, funder_pc, 
    facility_pc, archive_pc, other_pc)
    values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)"#;

    sqlx::query(sql)
    .bind(v.vcode.clone()).bind(v.vdate).bind(num_types)
    .bind(government).bind(education).bind(healthcare).bind(company)
    .bind(nonprofit).bind(funder).bind(facility).bind(archive).bind(other)
    .bind(government_pc).bind(education_pc).bind(healthcare_pc).bind(company_pc)
    .bind(nonprofit_pc).bind(funder_pc).bind(facility_pc).bind(archive_pc).bind(other_pc)
    .execute(pool)
    .await?;

    let government_pc =  get_pc (government,v.num_orgs);   // pc of orgs with type government
    let education_pc =  get_pc (education,v.num_orgs);      // pc of orgs with type education
    let healthcare_pc =  get_pc (healthcare,v.num_orgs);     // pc of orgs with type healthcare
    let company_pc =  get_pc (company,v.num_orgs);        // pc of orgs with type company
    let nonprofit_pc =  get_pc (nonprofit,v.num_orgs);      // pc of orgs with type nonprofit
    let funder_pc =  get_pc (funder,v.num_orgs);         // pc of orgs with type funder
    let facility_pc =  get_pc (facility,v.num_orgs);       // pc of orgs with type facility
    let archive_pc =  get_pc (archive,v.num_orgs);        // pc of orgs with type archive
    let other_pc =  get_pc (other,v.num_orgs);          // pc of orgs with type 
    
    let sql = r#"INSERT into smm.type_by_orgs_summary (vcode, vdate, num_orgs, government,
    education, healthcare, company, nonprofit, funder, facility, archive, other,
    government_pc, education_pc, healthcare_pc, company_pc, nonprofit_pc, funder_pc, 
    facility_pc, archive_pc, other_pc)
    values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)"#;

    sqlx::query(sql)
    .bind(v.vcode.clone()).bind(v.vdate).bind(v.num_orgs)
    .bind(government).bind(education).bind(healthcare).bind(company)
    .bind(nonprofit).bind(funder).bind(facility).bind(archive).bind(other)
    .bind(government_pc).bind(education_pc).bind(healthcare_pc).bind(company_pc)
    .bind(nonprofit_pc).bind(funder_pc).bind(facility_pc).bind(archive_pc).bind(other_pc)
    .execute(pool)
    .await?;

    Ok(())
}
   

pub async fn store_type_count_distrib(v: &RorVersion, pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = v.dvdd.clone() + r#"n_types as count, count(id) as num_of_orgs, 
                      round(count(id) * 10000 :: float / "# + &(v.num_orgs.to_string()) + r#":: float)/100 :: float as pc_of_orgs
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


pub async fn store_types_with_lang_code(v: &RorVersion, pool: &Pool<Postgres>) -> Result<(), AppError> {

    // for each org type, and each of the three name types (therefore 9 x 3 rows)
    // get total number of names and numbers with / without lang codes

    // get org type codes and names and load into a vector

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

    // Get the Unicode scripts with their ascii code boundaries.

    let sql  = r#"select id, name
    from lup.ror_org_types
    order by id;"#;
    let rows: Vec<OrgType> = sqlx::query_as(sql).fetch_all(pool).await?;

    for t in rows {

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

            // store the individual rows
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
  
    let isni =  get_count("select count(*) from src.external_ids where id_type = 11", pool).await?;   
    let grid =  get_count("select count(*) from src.external_ids where id_type = 13", pool).await?;      
    let fundref =  get_count("select count(*) from src.external_ids where id_type = 14", pool).await?;     
    let wikidata =  get_count("select count(*) from src.external_ids where id_type = 12", pool).await?;    
    let isni_pc =  get_pc(isni, num_ids); 
    let grid_pc =  get_pc(grid, num_ids); 
    let fundref_pc = get_pc(fundref, num_ids); 
    let wikidata_pc =  get_pc(wikidata, num_ids); 
    let isni_orgs =  get_count("select count(*) from src.admin_data where n_isni > 0", pool).await?;   
    let grid_orgs =  get_count("select count(*) from src.admin_data where n_grid > 0", pool).await?;      
    let fundref_orgs =  get_count("select count(*) from src.admin_data where n_fundref > 0", pool).await?;     
    let wikidata_orgs =  get_count("select count(*) from src.admin_data where n_wikidata > 0", pool).await?;    
    let isni_pc_orgs =  get_pc(isni_orgs, v.num_orgs); 
    let grid_pc_orgs =  get_pc(grid_orgs, v.num_orgs); 
    let fundref_pc_orgs = get_pc(fundref_orgs, v.num_orgs); 
    let wikidata_pc_orgs =  get_pc(wikidata_orgs, v.num_orgs); 

    let sql = r#"INSERT into smm.ext_ids_summary (vcode, vdate, num_ids, isni,
    grid, fundref, wikidata, isni_pc, grid_pc, fundref_pc, wikidata_pc, num_orgs, isni_orgs,
    grid_orgs, fundref_orgs, wikidata_orgs, isni_pc_orgs, grid_pc_orgs, fundref_pc_orgs, wikidata_pc_orgs)
    values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)"#;

    sqlx::query(sql)
    .bind(v.vcode.clone()).bind(v.vdate).bind(num_ids)
    .bind(isni).bind(grid).bind(fundref).bind(wikidata)
    .bind(isni_pc).bind(grid_pc).bind(fundref_pc).bind(wikidata_pc)
    .bind(v.num_orgs).bind(isni_orgs).bind(grid_orgs).bind(fundref_orgs).bind(wikidata_orgs)
    .bind(isni_pc_orgs).bind(grid_pc_orgs).bind(fundref_pc_orgs).bind(wikidata_pc_orgs)
    .execute(pool)
    .await?;

    Ok(())
}


pub async fn store_ext_ids_count_distrib(v: &RorVersion, pool: &Pool<Postgres>) -> Result<(), AppError> {

  let sql = v.dvdd.clone() + r#"n_ext_ids as count, count(id) as num_of_orgs, 
                    round(count(id) * 10000 :: float / "# + &(v.num_orgs.to_string()) + r#":: float)/100 :: float as pc_of_orgs
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


pub async fn store_links_summary(v: &RorVersion, pool: &Pool<Postgres>, num_links: i64) -> Result<(), AppError> {

    let wikipedia =  get_count("select count(*) from src.links where link_type = 21", pool).await?;   
    let website =  get_count("select count(*) from src.links where link_type = 22", pool).await?;      
    
    let wikipedia_pc =  get_pc(wikipedia, num_links); 
    let website_pc =  get_pc(website, num_links); 
    
    let wikipedia_orgs =  get_count("select count(*) from src.admin_data where n_wikipedia > 0", pool).await?;   
    let website_orgs =  get_count("select count(*) from src.admin_data where n_website > 0", pool).await?;      
   
    let wikipedia_pc_orgs =  get_pc(wikipedia_orgs, v.num_orgs); 
    let website_pc_orgs =  get_pc(website_orgs, v.num_orgs); 

    let sql = r#"INSERT into smm.links_summary (vcode, vdate, 
    num_links, wikipedia, website, wikipedia_pc, website_pc, 
    num_orgs, wikipedia_orgs, website_orgs, wikipedia_pc_orgs, website_pc_orgs)
    values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"#;

    sqlx::query(sql)
    .bind(v.vcode.clone()).bind(v.vdate).bind(num_links)
    .bind(wikipedia).bind(website).bind(wikipedia_pc).bind(website_pc)
    .bind(v.num_orgs).bind(wikipedia_orgs).bind(website_orgs).bind(wikipedia_pc_orgs).bind(website_pc_orgs)
    .execute(pool)
    .await?;
    
    Ok(())
}


pub async fn store_links_count_distrib(v: &RorVersion, pool: &Pool<Postgres>) -> Result<(), AppError> {

  let sql = v.dvdd.clone() + r#"n_links as count, count(id) as num_of_orgs, 
                    round(count(id) * 10000 :: float / "# + &(v.num_orgs.to_string()) + r#":: float)/100 :: float as pc_of_orgs
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

// relationships_summary
pub async fn store_relationships_summary(_v: &RorVersion, _pool: &Pool<Postgres>) -> Result<(), AppError> {

/*
create table smm.relationships_summary
    (    
          vcode             varchar     not null primary key
        , vdate             date        not null
        , total_lnks        int         null
        , parent_lnks       int         null
        , child_lnks        int         null
        , related_lnks      int         null
        , predecessor_lnks  int         null
        , successor_lnks    int         null
        , total             int         null
        , parent            int         null
        , child             int         null
        , par_and_ch        int         null
        , related           int         null
        , predecessor       int         null
        , successor         int         null
        , parent_pc         real        null
        , child_pc          real        null
        , par_and_ch_pc     real        null
        , related_pc        real        null
        , predecessor_pc    real        null
        , successor_pc      real        null
        , non_recip_pc      int         null
        , non_recip_rr      int         null
        , non_recip_ps      int         null
    ); */
    Ok(())
}

// type_relationship
pub async fn store_types_and_relationships(_: &RorVersion, _pool: &Pool<Postgres>) -> Result<(), AppError> {

/*
create table smm.type_relationship
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , org_type          varchar     null
        , org_type_total    int         null
        , rel_type          varchar     null
        , count             int         null
        , count_distinct    int         null
        , count_pc          real        null
    );
*/

    Ok(())
}

pub async fn store_country_top_25_distrib(v: &RorVersion, pool: &Pool<Postgres>, num_locs: i64) -> Result<(), AppError> {

    // At the moment num_of_locs = num_of_orgs - might change!

    let sql = v.dvdd.clone() + r#"country_name as country, count(country_name) as num_of_locs, 
                      round(count(country_name) * 10000 :: float / "# + &(num_locs.to_string()) + r#":: float)/100 :: float as pc_of_locs
                      from src.locations
                      group by country_name
                      order by count(country_name) desc;"#;
    let rows: Vec<CountryRow> = sqlx::query_as(&sql).fetch_all(pool).await?;

    let mut i = 0;
    let mut rest_total = 0;
    for r in rows {
        if i < 25 {
            sqlx::query(r#"INSERT INTO smm.country_distribution (vcode, vdate, country, num_of_locs, pc_of_locs) 
                                values($1, $2, $3, $4, $5)"#)
            .bind(r.vcode)
            .bind(r.vdate)
            .bind(r.country)
            .bind(r.num_of_locs)
            .bind(r.pc_of_locs)
            .execute(pool)
            .await?;
        }
        else {
          rest_total += r.num_of_locs;
        } 
        i += 1;
    }
    let rest_total_pc = get_pc(rest_total, num_locs);

    sqlx::query(r#"INSERT INTO smm.country_distribution (vcode, vdate, country, num_of_locs, pc_of_locs) 
                        values($1, $2, $3, $4, $5)"#)
    .bind(v.vcode.clone())
    .bind(v.vdate)
    .bind("Remaining countries")
    .bind(rest_total)
    .bind(rest_total_pc)
    .execute(pool)
    .await?;
  
  Ok(())
}


pub async fn store_locs_count_distrib(v: &RorVersion, pool: &Pool<Postgres>) -> Result<(), AppError> {

  let sql = v.dvdd.clone() + r#"n_locs as count, count(id) as num_of_orgs, 
                     round(count(id) * 10000 :: float / "# + &(v.num_orgs.to_string()) + r#":: float)/100 :: float as pc_of_orgs
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