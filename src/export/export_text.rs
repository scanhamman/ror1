use sqlx::{Pool, Postgres};
use std::path::PathBuf;
use crate::AppError;
use std::fs::OpenOptions;
use std::io::prelude::*;
use super::export_structs::{VSummary, TypeRow, DistribRow, RankedRow, 
                            SingletonRow, Singleton, OrgAndLangCode, OrgAndRel};
use log::info;


pub async fn generate_text(output_folder : &PathBuf, output_file_name: &String, 
            data_version: &String, pool : &Pool<Postgres>) -> Result<(), AppError>
{
    // If data version and date not given explicitly derive them from the data version table
    // as being the version, date of the currently stored version

    let mut vcode: String = data_version.clone();
    if vcode == "" {
        let sql = "SELECT version as vcode from src.version_details;";
        vcode = sqlx::query_scalar(sql).fetch_one(pool).await?;
    }

    // Get path and set up file for writing
    
    let output_file_path: PathBuf = [output_folder, &PathBuf::from(output_file_name)].iter().collect();
    let output_file_str = output_file_path.to_str().unwrap();
    
    let singvals: Vec<Singleton> = collect_singleton_values(&vcode, pool).await?;
    write_header_and_summary(output_file_str, &vcode, pool).await?;
    write_name_info(output_file_str, &vcode, pool).await?;
    write_name_wolc_info(output_file_str, &vcode, pool, &singvals).await?;
    write_ror_name_details(output_file_str, &singvals).await?;
    write_ranked_name_info(output_file_str, &vcode, pool, &singvals).await?;
    write_type_details(output_file_str, &vcode, pool).await?;
    write_location_details(output_file_str, &vcode, pool).await?;
    write_links_and_extid_details(output_file_str, &vcode, pool).await?;
    write_relationship_details(output_file_str, &vcode, pool, &singvals).await?;
    write_domain_details(output_file_str, &vcode, pool).await?;

    info!("Content appended successfully");
    Ok(())
}

async fn collect_singleton_values(vcode: &String, pool: &Pool<Postgres>) -> Result<Vec<Singleton>, AppError> {

    let mut sstructs = Vec::with_capacity(16);
    let sql = "SELECT id, number, pc from smm.singletons WHERE vcode = \'".to_string() 
              + vcode  + "\' and id between 1 and 16 order by id;";
    let srows: Vec<SingletonRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    for r in srows { 
        let descriptor_line = match r.id {
            1 => "Non acronym (nacro) names not in English",
            2 => "Total names in a non latin script",
            3 => "Total names without a language code",
            4 => "Non-acronym names wolc",
            5 => "Non-acronym, non-company names wolc",
            6 => "Orgs both parent and child, number and pc of total orgs",
            7 => "Non-reciprocated parent-child relationships, number",
            8 => "Non-reciprocated 'related' relationships, number",
            9 => "Non-reciprocated pred-succ relationships, number",
            10 => "Labels that are designated ROR names, number",
            11 => "Labels that are not designated ROR names, number",
            12 => "Non-Label ROR names, number & pc of total orgs",
            13 => "ROR names in English, number & pc of total orgs",
            14 => "ROR names not in English, number & pc of total orgs",
            15 => "ROR names wolc, number & pc of total orgs",
            16 => "Non company ROR names wolc, number & pc of non-company orgs",
            _ => "",
        };
        let s = Singleton {
            description: descriptor_line.to_string(),
            number: r.number,
            pc: r.pc,
        };
        sstructs.push(s);
    }
    Ok(sstructs)

}

async fn write_header_and_summary(output_file_str: &str, vcode: &String, pool: &Pool<Postgres>) -> Result<(), AppError> {
    
    let sql = "SELECT * from smm.version_summary WHERE vcode = \'".to_string() + &vcode  + "\' ;";
    let summ: VSummary = sqlx::query_as(&sql).fetch_one(pool).await?;
    let header_txt = get_hdr_line("SUMMARY ORROR DATASET")
                   + "\n\n\tVersion: " + vcode  
                   + "\n\tDate: " + &summ.vdate.to_string() 
                   + "\n\tDays since 11/04/24: " + &summ.vdays.to_string();
    append_to_file(output_file_str, &header_txt)?;

    let summary_txt = "\n\n\tENTITY NUMBERS\n\t".to_string() 
                    + "----------------------------------------------------------------------------------" 
                + &get_data_line(47, "Organisations", summ.num_orgs) 
                + &get_data_line(47,"Names", summ.num_names) 
                + &get_data_line(47,"Types", summ.num_types) 
                + &get_data_line(47,"Links", summ.num_links) 
                + &get_data_line(47,"External Ids", summ.num_ext_ids) 
                + &get_data_line(47,"Relationships", summ.num_rels) 
                + &get_data_line(47,"Locations", summ.num_locations) 
                + &get_data_line(47,"Domains", summ.num_domains) 
                + "\n";
    append_to_file(output_file_str, &summary_txt)?;
    Ok(())
}

async fn write_name_info(output_file_str: &str, vcode: &String, pool: &Pool<Postgres>) -> Result<(), AppError> {

    append_to_file(output_file_str, &get_hdr_line("NAMES"))?;

    // Write name attribute summary - att_type 1

    let table_text = get_attrib_table(1, "Names", vcode, pool).await?;
    append_to_file(output_file_str, &table_text)?;

    // Write count distributions - count_type: names, labels, aliases, acronyms

    let table_text = get_distrib_table("names", "names", vcode, pool).await?;
    append_to_file(output_file_str, &table_text)?;

    let table_text = get_distrib_table("labels", "labels", vcode, pool).await?;
    append_to_file(output_file_str, &table_text)?;

    let table_text = get_distrib_table("aliases", "aliases", vcode, pool).await?;
    append_to_file(output_file_str, &table_text)?;

    let table_text = get_distrib_table("acronyms", "acronyms", vcode, pool).await?;
    append_to_file(output_file_str, &table_text)?;

    Ok(())
}

async fn write_name_wolc_info(output_file_str: &str, vcode: &String, pool: &Pool<Postgres>
                                                   , singvals: &Vec<Singleton>) -> Result<(), AppError> {

    append_to_file(output_file_str, &get_hdr_line("NAMES WITHOUT LANGUAGE CODE (WOLC)"))?;
           
    let mut wolc_total_text = "\n\t                                         number                        %age".to_string()
                        + "\n\t                                        of names                    total names";
    for r in 2..5 { 
        let s = &singvals[r];
        wolc_total_text = wolc_total_text + &get_singleton_line(47, &s.description, 30, s.number, s.pc)
    }
    append_to_file(output_file_str, &wolc_total_text)?;

    // Write name wolc attribute summary - att_type 11.

    let table_text = get_attrib_table(11, "Names without language codes (wolc)", vcode, pool).await?;
    append_to_file(output_file_str, &table_text)?;
   
    // org type and lang code data 

    let sql = r#"select org_type, name_type, names_num, names_wolc, names_wolc_pc 
                 from smm.org_type_and_lang_code where vcode = '"#.to_string() + vcode + r#"' order by 
                 org_type, name_type;"#;
    let rows: Vec<OrgAndLangCode> = sqlx::query_as(&sql).fetch_all(pool).await?;
    let mut tbl_text = "\n\n\tNumbers of name types without language codes for different organisational types:".to_string() 
                     + "\n\n\t                                          number           names            %age"
                       + "\n\torg type              name type           names            wolc             wolc  "
                       + "\n\t----------------------------------------------------------------------------------";
    for r in rows {
        tbl_text += &get_orglc_line(&r.org_type, &r.name_type, r.names_num, r.names_wolc, r.names_wolc_pc);
    }
    tbl_text += "\n";
    append_to_file(output_file_str, &tbl_text)?;

    Ok(())
}

async fn write_ranked_name_info(output_file_str: &str, vcode: &String, pool: &Pool<Postgres>, 
                                                       singvals: &Vec<Singleton>) -> Result<(), AppError> {
    
    append_to_file(output_file_str, &get_hdr_line("LANGUAGE AND SCRIPT USAGE"))?;

    // Singleton - Non acronym names that are not English, number and pc of total names, id 1, index 0

    let s = &singvals[0];
    let srow1_text ="\n\t                                         number                          %age".to_string()
                   +"\n\t                                        of names                      nacro names"
                   + &get_singleton_line(47, &s.description, 30, s.number, s.pc);
    append_to_file(output_file_str, &srow1_text)?;

    // Ranked languages other than English in use - Ranked Distributions, dist_type = 1.

    let tbl_hdr_text = "\n\n\t                                         number           %age           %age".to_string()
                       + "\n\tLanguage                                of names      NonEng names    nacro names"
                       + "\n\t----------------------------------------------------------------------------------";
    let tbl_text = get_ranked_distrib_table(1, vcode, pool).await?;
    append_to_file(output_file_str, &(tbl_hdr_text + &tbl_text))?;

    // Singleton - Names that are not in Latin script, number and pc of total names , id 2, index 1

    let s = &singvals[1];
    let srow2_text = "\n\n\n\t                                         number                          %age".to_string()
                       + "\n\t                                        of names                      total names"
                   + &get_singleton_line(47, &s.description, 30, s.number, s.pc);
    append_to_file(output_file_str, &srow2_text)?;

    // Ranked scripts other than Letin in use - Ranked Distributions, dist_type = 2.
    
    let tbl_hdr_text = "\n\n\t                                         number          %age           %age".to_string()
                       + "\n\tScript                                  of names     NonLtn names    total names"
                       + "\n\t---------------------------------------------------------------------------------";
    let tbl_text = get_ranked_distrib_table(2, vcode, pool).await?;
    append_to_file(output_file_str, &(tbl_hdr_text + &tbl_text))?;
    
    Ok(())
}


async fn write_ror_name_details(output_file_str: &str, singvals: &Vec<Singleton>) -> Result<(), AppError> {
    
    append_to_file(output_file_str, &get_hdr_line("ROR NAMES"))?;
    
    // Write out singleton values, go from ids 10 to 16, requires index range (9..16).

    let mut sng_text = "\n\t                                                           number       %age\n".to_string();
    for r in 9..16 { 
        let s = &singvals[r];
        sng_text = sng_text + &get_singleton_line(65, &s.description, 12, s.number, s.pc)
    }
    append_to_file(output_file_str, &sng_text)?;
    Ok(())
}

async fn write_type_details(output_file_str: &str, vcode: &String, pool: &Pool<Postgres>) -> Result<(), AppError> {
    
    append_to_file(output_file_str, &get_hdr_line("ORGANISATION TYPES"))?;

    // Write type attribute summary - att_type 2

    let table_text = get_attrib_table(2, "Organisation types", vcode, pool).await?;
    append_to_file(output_file_str, &table_text)?;
   
    // write count distributions - count_type: org_types

    let table_text = get_distrib_table("org_types", "organisation types", vcode, pool).await?;
    append_to_file(output_file_str, &table_text)?;

    Ok(())
}

async fn write_location_details(output_file_str: &str, vcode: &String, pool: &Pool<Postgres>) -> Result<(), AppError> {
       
    append_to_file(output_file_str, &get_hdr_line("LOCATIONS"))?;

    // Count distribution.

    let table_text = get_distrib_table("locs", "locations", vcode, pool).await?;
    append_to_file(output_file_str, &table_text)?;
   
    // Ranked countries - Ranked Distributions, dist_type = 3
 
    let tbl_hdr_text ="\n\n\tNumber of locations by country:".to_string()
                    + "\n\n\t                                         number           %age           %age"
                      + "\n\tCountry                                  of locs       NonUS locs     total locs"
                      + "\n\t---------------------------------------------------------------------------------";
    let tbl_text = get_ranked_distrib_table(3, vcode, pool).await?;
    append_to_file(output_file_str, &(tbl_hdr_text + &tbl_text))?;

    Ok(())
}

async fn write_links_and_extid_details(output_file_str: &str, vcode: &String, pool: &Pool<Postgres>) -> Result<(), AppError> {
    
    append_to_file(output_file_str, &get_hdr_line("EXTERNAL IDS AND LINKS"))?;

    // Write link attribute summary - att_type 4

    let table_text = get_attrib_table(4, "Links", vcode, pool).await?;
    append_to_file(output_file_str, &table_text)?;

    // Write count distributions.

    let table_text = get_distrib_table("links", "links", vcode, pool).await?;
    append_to_file(output_file_str, &table_text)?;
        
    // Write ext id attribute summary - att_type 3
    let table_text = get_attrib_table(3, "External Ids", vcode, pool).await?;
    append_to_file(output_file_str, &table_text)?;
   
    // Write count distribution.
    
    let table_text = get_distrib_table("ext_ids", "external ids", vcode, pool).await?;
    append_to_file(output_file_str, &table_text)?;

    Ok(())
}

async fn write_relationship_details(output_file_str: &str, vcode: &String, pool: &Pool<Postgres>, 
                                     singvals: &Vec<Singleton>) -> Result<(), AppError> {
   
    append_to_file(output_file_str, &get_hdr_line("RELATIONSHIPS"))?;

    // Write relationship attribute summary - att_type 5
    
    let table_text = get_attrib_table(5, "Relationships", vcode, pool).await?;
    append_to_file(output_file_str, &table_text)?;

    // Write count distributions.

    let table_text = get_distrib_table("parent orgs", "'has parent' relationships", vcode, pool).await?;
    append_to_file(output_file_str, &table_text)?;

    let table_text = get_distrib_table("child orgs", "'has child' relationships", vcode, pool).await?;
    append_to_file(output_file_str, &table_text)?;

    let table_text = get_distrib_table("related orgs", "'is related to' relationships", vcode, pool).await?;
    append_to_file(output_file_str, &table_text)?;
    
    let table_text = get_distrib_table("predecessor orgs", "'has predecessor' relationships", vcode, pool).await?;
    append_to_file(output_file_str, &table_text)?;

    let table_text = get_distrib_table("successor orgs", "'has successor' relationshipss", vcode, pool).await?;
    append_to_file(output_file_str, &table_text)?;

    // Write out singleton values, go from ids 6 to 9, requires index range (5..9).

    let mut sng_text = "\n\t                                                           number       %age\n".to_string();
    for r in 5..9 { 
        let s = &singvals[r];
        sng_text = sng_text + &get_singleton_line(65, &s.description, 12, s.number, s.pc)
    }
    append_to_file(output_file_str, &sng_text)?;

    // Org type and relationship data.

    let sql = r#"select org_type, rel_type, num_links, num_orgs, num_orgs_pc 
                 from smm.org_type_and_relationships 
                 where vcode = '"#.to_string() + vcode + r#"' order by org_type, rel_type;"#;

    let rows: Vec<OrgAndRel> = sqlx::query_as(&sql).fetch_all(pool).await?;
    let mut tbl_text = "\n\n\tNumbers of relationship links for different organisational types:".to_string() 
                     + "\n\n\t                                            number       number         %age"
                       + "\n\torg type              relationship          links         orgs        org type"
                       + "\n\t----------------------------------------------------------------------------------";
    for r in rows {
        tbl_text += &get_orgrel_line(&r.org_type, &r.rel_type, r.num_links, r.num_orgs, r.num_orgs_pc);
    }
    tbl_text += "\n";
    append_to_file(output_file_str, &tbl_text)?;

    Ok(())
}

async fn write_domain_details(output_file_str: &str, vcode: &String, pool: &Pool<Postgres>) -> Result<(), AppError> {
   
    append_to_file(output_file_str,  &get_hdr_line("DOMAINS"))?;
    
    let table_text = get_distrib_table("domains", "domains", vcode, pool).await?;
    append_to_file(output_file_str, &table_text)?;

    Ok(())
}


async fn get_attrib_table(att_type: i32, header_type: &str, vcode: &String, 
    pool: &Pool<Postgres>) -> Result<String, AppError> {

    let sql = r#"select name, number_atts, pc_of_atts, number_orgs, pc_of_orgs from smm.attributes_summary
    where vcode = '"#.to_string() + vcode + r#"' and att_type = "# + &att_type.to_string() + " order by id;";
    let rows: Vec<TypeRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    let mut tbl_text = "\n\n\t".to_string() + header_type + ", categories and numbers:"
          + "\n\n\t                              number         %age       number         %age"
            + "\n\tCategory                      in cat       all cats      orgs       total orgs"
            + "\n\t----------------------------------------------------------------------------------";
    for r in rows {
        tbl_text = tbl_text + &get_attrib_line(&r.name, r.number_atts, r.pc_of_atts, r.number_orgs, r.pc_of_orgs);
    }
    tbl_text = tbl_text + "\n";
    Ok(tbl_text + "\n")
}


fn get_attrib_line(category: &str, num_atts: i32, pc_atts: f32, num_orgs: i32, pc_orgs: f32)-> String {
    let spacer1 = " ".repeat(36 - category.chars().count() - num_atts.to_string().len());
    let pc1_as_string = format!("{:.2}", pc_atts);
    let spacer2 = " ".repeat(13 - pc1_as_string.len());
    let spacer3 = " ".repeat(13 - num_orgs.to_string().len());
    let pc2_as_string = format!("{:.2}", pc_orgs);
    let spacer4 = " ".repeat(14 - pc2_as_string.len());
    "\n\t".to_string() + category + &spacer1 + &num_atts.to_string() 
           + &spacer2 + &pc1_as_string + &spacer3 + &num_orgs.to_string() + &spacer4 + &pc2_as_string
}

async fn get_distrib_table(count_type: &str, header_type: &str, vcode: &String, 
                                             pool: &Pool<Postgres>) -> Result<String, AppError> {
let sql = r#"select count, num_of_orgs, pc_of_orgs from smm.count_distributions
                 where vcode = '"#.to_string() + vcode + r#"' and count_type = '"# + count_type + r#"' 
                 order by count;"#;
    let rows: Vec<DistribRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    let mut tbl_text = "\n\n\tNumbers of organisations with specified numbers of ".to_string() + header_type
                     + "\n\n\t                                            number       number         %age"
                       + "\n\t                                           entities       orgs       total orgs"
                       + "\n\t                                        ------------------------------------------";
    for r in rows {
        tbl_text = tbl_text + &get_distrib_line(r.count, r.num_of_orgs, r.pc_of_orgs);
    }
    Ok(tbl_text + "\n")
}

fn get_distrib_line(count: i32, num: i32, pc: f32)-> String {
    let count_as_string = " ".repeat(47 - count.to_string().len()) + &count.to_string();
    let spacer1 = " ".repeat(15 - num.to_string().len());
    let pc_as_string = format!("{:.2}", pc);
    let spacer2 = " ".repeat(15 - pc_as_string.len());
    "\n\t".to_string() + &count_as_string + &spacer1 + &num.to_string() 
           + &spacer2 + &pc_as_string
}

async fn get_ranked_distrib_table(dist_type: i32, vcode: &String, pool: &Pool<Postgres>) -> Result<String, AppError> {

    let sql = r#"SELECT entity, number, pc_of_entities, pc_of_base_set from smm.ranked_distributions 
                 where vcode = '"#.to_string() + vcode + r#"' and dist_type = "# + 
                 &dist_type.to_string() + " order by rank";
    let lang_rows: Vec<RankedRow> = sqlx::query_as(&sql).fetch_all(pool).await?;
    let mut tbl_text = "".to_string();
    for r in lang_rows {
        tbl_text = tbl_text + &get_ranked_distrib_line(&r.entity, r.number, r.pc_of_entities, r.pc_of_base_set);
    }
    Ok(tbl_text + "\n")
}

fn get_ranked_distrib_line(topic: &str, num: i32, pc1: f32, pc2: f32)-> String {
    let spacer1 = " ".repeat(47 - topic.chars().count() - num.to_string().len());
    let pc1_as_string = format!("{:.2}", pc1);
    let spacer2 = " ".repeat(15 - pc1_as_string.len());
    let pc2_as_string = format!("{:.2}", pc2);
    let spacer3 = " ".repeat(15 - pc2_as_string.len());
    "\n\t".to_string() + topic + &spacer1 + &num.to_string() 
           + &spacer2 + &pc1_as_string + &spacer3 + &pc2_as_string
}

fn get_hdr_line(topic: &str) -> String {
    "\n\n\t==================================================================================".to_string()
    + "\n\t" + topic 
    + "\n\t=================================================================================="
}

fn get_data_line(first_space: usize, topic: &str, num: i32) -> String {
    let spacer = " ".repeat(first_space - topic.len() - num.to_string().len());
    "\n\t".to_string() + topic + &spacer + &num.to_string() 
}

fn get_orglc_line(org_type: &str, name_type: &str, names_num: i32, names_wolc: i32, names_wolc_pc: f32) -> String {
    let spacer1 = " ".repeat(22 - org_type.chars().count());
    let spacer2 = " ".repeat(26 - name_type.chars().count()- names_num.to_string().len());
    let spacer3 = " ".repeat(15 - names_wolc.to_string().len());
    let pc_as_string = format!("{:.2}", names_wolc_pc);
    let spacer4 = " ".repeat(15 - pc_as_string.to_string().len());
    "\n\t".to_string() + org_type + &spacer1 + name_type + &spacer2 + &names_num.to_string() 
           + &spacer3 + &names_wolc.to_string() + &spacer4 + &pc_as_string
}

fn get_orgrel_line(org_type: &str, rel_type: &str, num_links: i32, num_orgs: i32, num_orgs_pc: f32) -> String {
    let spacer1 = " ".repeat(22 - org_type.chars().count());
    let spacer2 = " ".repeat(26 - rel_type.chars().count() - num_links.to_string().len());
    let spacer3 = " ".repeat(15 - num_orgs.to_string().len());
    let pc_as_string = format!("{:.2}", num_orgs_pc);
    let spacer4 = " ".repeat(15 - pc_as_string.to_string().len());
    "\n\t".to_string() + org_type + &spacer1 + rel_type + &spacer2 + &num_links.to_string() 
           + &spacer3 + &num_orgs.to_string() + &spacer4 + &pc_as_string
}

fn get_singleton_line(first_space: usize, topic: &str, second_space: usize, num: i32, pc: Option<f32>) -> String {
    let spacer1 = " ".repeat(first_space - topic.len() - num.to_string().len());
    if pc.is_some() {
        let pc_as_string = format!("{:.2}", pc.unwrap());
        let spacer2 = " ".repeat(second_space - pc_as_string.len());
        "\n\t".to_string() + topic + &spacer1 + &num.to_string() + &spacer2 + &pc_as_string
    }
    else {
        "\n\t".to_string() + topic + &spacer1 + &num.to_string() 
    }
}

fn append_to_file(output_file_path: &str, contents: &str) -> Result<(), AppError> {

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(output_file_path)?;

    match file.write_all(contents.as_bytes())
    {
        Ok(_) => Ok(()),
        Err(e) => Err(AppError::IoErr(e)),
    }
    
}
