use sqlx::{Pool, Postgres};
use std::path::PathBuf;
use crate::AppError;
use std::fs::OpenOptions;
use std::io::prelude::*;
use super::export_structs::{VSummary, SingletonRow};
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

    let wc = " WHERE vcode = \'".to_string() + &vcode  + "\';";

    // Get path and set up file for writing
    
    let output_file_path: PathBuf = [output_folder, &PathBuf::from(output_file_name)].iter().collect();
    let output_file_str = output_file_path.to_str().unwrap();
    
    write_header_and_summary(output_file_str, &wc, pool).await?;
    write_name_info(output_file_str, &vcode, pool).await?;
    write_ranked_name_info(output_file_str, &wc, pool).await?;
    write_ror_name_details(output_file_str, &wc, pool).await?;
    write_type_details(output_file_str, &wc, pool).await?;
    write_location_details(output_file_str, &wc, pool).await?;
    write_links_and_extid_details(output_file_str, &wc, pool).await?;
    write_relationship_details(output_file_str, &wc, pool).await?;
    write_domain_details(output_file_str, &wc, pool).await?;

    info!("Content appended successfully");
    Ok(())
}

async fn write_header_and_summary(output_file_str: &str, wc: &String, pool: &Pool<Postgres>) -> Result<(), AppError> {
    
    let sql = format!("SELECT * from smm.version_summary {};", wc);
    let summ: VSummary = sqlx::query_as(&sql).fetch_one(pool).await?;

    let header_txt = "\n\n\tROR DATA SUMMARY \n\t".to_string() + "------------------------------------\n\tVersion: " 
    + &summ.vcode  + "\n\tDate: " + &summ.vdate.to_string() + "\n\tDays since 11/04/24: " + &summ.vdays.to_string();
    append_to_file(output_file_str, &header_txt)?;

    let summary_txt = "\n\n\tTABLE RECORD NUMBERS \n\t".to_string() 
                + "------------------------------------" 
                + &get_data_line("Organisations", summ.num_orgs) 
                + &get_data_line("Names", summ.num_names) 
                + &get_data_line("Types", summ.num_types) 
                + &get_data_line("Links", summ.num_links) 
                + &get_data_line("External Ids", summ.num_ext_ids) 
                + &get_data_line("Relationships", summ.num_rels) 
                + &get_data_line("Locations", summ.num_locations) 
                + &get_data_line("Domains", summ.num_domains) 
                + "\n\t------------------------------------\n\n";
    append_to_file(output_file_str, &summary_txt)?;
    Ok(())
}

async fn write_name_info(output_file_str: &str, vcode: &String, pool: &Pool<Postgres>) -> Result<(), AppError> {
    
    let name_hdr_txt = "\tNAMES ".to_string() 
                + "\n\t------------------------------------\n\n\n";
    append_to_file(output_file_str, &name_hdr_txt)?;

    // Write name attribute summary - att_type 1

    let sql = "SELECT * from smm.singletons WHERE vcode = \'".to_string() + vcode  + "\' and id = 3;";
    let srow3: SingletonRow = sqlx::query_as(&sql).fetch_one(pool).await?;
    let sql = "SELECT * from smm.singletons WHERE vcode = \'".to_string() + vcode  + "\' and id = 4;";
    let srow4: SingletonRow = sqlx::query_as(&sql).fetch_one(pool).await?;
    let sql = "SELECT * from smm.singletons WHERE vcode = \'".to_string() + vcode  + "\' and id = 5;";
    let srow5: SingletonRow = sqlx::query_as(&sql).fetch_one(pool).await?;
    
    let wolc_total_text = "\n\t                                         number         %age".to_string()
                            + "\n\t                                        of names     total names"
                            + &get_singleton_line("Total names without a language code", srow3.number, srow3.pc)
                            + &get_singleton_line("Non-acronym names wolc", srow4.number, srow4.pc)
                            + &get_singleton_line("Non-acronym, non-company names wolc", srow5.number, srow5.pc);
    append_to_file(output_file_str, &wolc_total_text)?;

    // Singleton - 3	Names that do not have a language code, number and pc of total names
    // Singleton - 4	Non-acronym names that do not have a language code, number and pc of total non-acronym names
    // Singleton - 5	Non-acronym, non company names that do not have a language code, number and pc of non-acronym non company names

    // Write name attribute summary (with wolc) - att_type 1

    // write count distributions - count_type: names, labels, aliases, acronyms

    // org type and lang code data

    // TO DO
    Ok(())
}

async fn write_ranked_name_info(_output_file_str: &str, _wc: &String, _pool: &Pool<Postgres>) -> Result<(), AppError> {
    
    // Singleton - Non acronym names that are not English, number and pc of total names - id 1
    // Singleton - Names that are not in Latin script, number and pc of total names - id 2

    // Ranked languages other than English in use - Ranked Distributions, dist_type = 1

    // Ranked scripts other than Letin in use - Ranked Distributions, dist_type = 2
    
    // TO DO
    Ok(())
}


async fn write_ror_name_details(_output_file_str: &str, _wc: &String, _pool: &Pool<Postgres>) -> Result<(), AppError> {
    
    // Singleton - 10	Labels that are designated ROR names, number
    // Singleton - 11	Labels that are not designated ROR names, number
    // Singleton - 12	Any non-Labels that are designated ROR names, number and pc of total orgs
    // Singleton - 13	ROR names in English, number and pc of total orgs
    // Singleton - 14	ROR names not in English, number and pc of total orgs
    // Singleton - 15	ROR names without a language code, number and pc of total orgs
    // Singleton - 16	Non company ROR names without a language code, number and pc of total non-company orgs

    // TO DO
    Ok(())
}

async fn write_type_details(_output_file_str: &str, _wc: &String, _pool: &Pool<Postgres>) -> Result<(), AppError> {
    
    // Write type attribute summary - att_type 2

    // write count distributions - count_type: org_types

    // TO DO
    Ok(())
}

async fn write_location_details(_output_file_str: &str, _wc: &String, _pool: &Pool<Postgres>) -> Result<(), AppError> {

    // Ranked countries - Ranked Distributions, dist_type = 1

    // write count distributions - count_type: locs

    // TO DO
    Ok(())
}

async fn write_links_and_extid_details(_output_file_str: &str, _wc: &String, _pool: &Pool<Postgres>) -> Result<(), AppError> {
    
    // Write ext id attribute summary - att_type 3

    // write count distributions - count_type: ext_ids

    // Write link attribute summary - att_type 4

    // write count distributions - count_type: links

    // TO DO
    Ok(())
}

async fn write_relationship_details(_output_file_str: &str, _wc: &String, _pool: &Pool<Postgres>) -> Result<(), AppError> {
    
    // Write relationship attribute summary - att_type 5

    // relationship and type data

    // Singleton -6	Orgs both parent and child, number and pc of total orgs
    // Singleton -7	Non-reciprocated parent-child relationships, number
    // Singleton -8	Non-reciprocated 'related' relationships, number
    // Singleton -9	Non-reciprocated pred-succ relationships, number

    // TO DO
    Ok(())
}

async fn write_domain_details(_output_file_str: &str, _wc: &String, _pool: &Pool<Postgres>) -> Result<(), AppError> {

    // TO DO
    Ok(())
}


fn get_data_line(topic: &str, num: i32) -> String {
    let spacer = " ".repeat(28 - topic.len() - num.to_string().len());
    "\n\t".to_string() + topic + &spacer + &num.to_string() 
}

fn get_singleton_line(topic: &str, num: i32, pc: Option<f32>)-> String {
    let spacer1 = " ".repeat(46 - topic.len() - num.to_string().len());
    if pc.is_some() {
        let pc_as_string = format!("{:.2}", pc.unwrap());
        let spacer2 = " ".repeat(15 - pc_as_string.len());
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
