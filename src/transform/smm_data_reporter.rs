use sqlx::{Pool, Postgres};
use std::path::PathBuf;
use crate::AppError;

pub async fn report_on_data(_output_folder : &PathBuf, _output_file_name: &String, _pool : &Pool<Postgres>) -> Result<(), AppError>
{
    // Report data analysis into a text file
    
    let _output_file_path: PathBuf = [_output_folder, &PathBuf::from(_output_file_name)].iter().collect();
    
    /* 
    
    Total number of org records -> number of records in core data table

    Distribution by type (overlap) -> get numbers from group by...

    store numbers in table - convert to percentages (%)

    get numbers of orgs with 1 type
    2 types
    3 types 
    > 3 types

  select count(*) from src.admin_data where n_types = 0;
  select count(*) from src.admin_data where n_types = 1;
  select count(*) from src.admin_data where n_types = 2;
  select count(*) from src.admin_data where n_types = 3;
  select count(*) from src.admin_data where n_types > 3;
           
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

    Numbers of (total) names distribution - 
    
    select count(*) from src.admin_data where n_names = 0;
    select count(*) from src.admin_data where n_names = 1; 
    select count(*) from src.admin_data where n_names = 2;
    select count(*) from src.admin_data where n_names = 3;
    select count(*) from src.admin_data where n_names = 4;
    select count(*) from src.admin_data where n_names = 5;
    select count(*) from src.admin_data where n_names = 6;
    select count(*) from src.admin_data where n_names = 7;
    select count(*) from src.admin_data where n_names = 8;
    select count(*) from src.admin_data where n_names = 9;
    select count(*) from src.admin_data where n_names = 10;
    select count(*) from src.admin_data where n_names > 10;
    
    Number of orgs with 1, 2, 3 or more labels

    Number of orgs with 1, 2, 3 or more aliases

    Number of orgs with 1, 2, 3 or more acronyms

    select count(*) from src.admin_data where n_acronyms = 0;
    select count(*) from src.admin_data where n_acronyms = 1; 
    select count(*) from src.admin_data where n_acronyms = 2;
    select count(*) from src.admin_data where n_acronyms = 3;
    select count(*) from src.admin_data where n_acronyms > 3;
    
   
    select count(*) from src.admin_data where n_labels = 0;
    select count(*) from src.admin_data where n_labels = 1; 
    select count(*) from src.admin_data where n_labels = 2;
    select count(*) from src.admin_data where n_labels = 3;
    select count(*) from src.admin_data where n_labels > 3;
    
   
    select count(*) from src.admin_data where n_aliases = 0;
    select count(*) from src.admin_data where n_aliases = 1; 
    select count(*) from src.admin_data where n_aliases = 2;
    select count(*) from src.admin_data where n_aliases = 3;
    select count(*) from src.admin_data where n_aliases > 3;
   
     Number of orgs with labels equal to the ror name
     select count(*) from src.names n 
     where name_type = 5 and is_ror_name = true
     
     Number of orgs with labels not equal to the ror name

     select count(*) from src.names n 
     where name_type = 5 and is_ror_name = false
     
     Any ror names not labels?

     select count(*) from src.names n 
     where name_type <> 5 and is_ror_name = true

    Number of orgs with null lang codes for labels and aliases 
    (i.e. excluding acronyms, which generally have no lang code)


    Number of orgs with null lang codes for labels and aliases (excluding acronyms and commercial orgs)
    (difficult to apply a language to a commercial name)

    -----------------------------------

    Names not in local script...
    for selected countries


    set up script data in import process

    
    -----------------------------------
       
    Number of orgs with 1, 2, 3, or more isni numbers

    select count(*) from src.external_ids where id_type = 11;

    select count(*) from src.admin_data where n_isni = 0;
    select count(*) from src.admin_data where n_isni = 1; 
    select count(*) from src.admin_data where n_isni = 2;
    select count(*) from src.admin_data where n_isni = 3;
    select count(*) from src.admin_data where n_isni > 3;

    Number of orgs with 1, 2, 3, or more grid numbers

    select count(*) from src.external_ids where id_type = 13;

    select count(*) from src.admin_data where n_grid = 0;
    select count(*) from src.admin_data where n_grid = 1; 
    select count(*) from src.admin_data where n_grid = 2;
    select count(*) from src.admin_data where n_grid = 3;
    select count(*) from src.admin_data where n_grid > 3;

    Number of orgs with 1, 2, 3, or more fundref numbers

    select count(*) from src.external_ids where id_type = 14;
 
    select count(*) from src.admin_data where n_fundref = 0;
    select count(*) from src.admin_data where n_fundref = 1; 
    select count(*) from src.admin_data where n_fundref = 2;
    select count(*) from src.admin_data where n_fundref = 3;
    select count(*) from src.admin_data where n_fundref > 3;

    Number of orgs with 1, 2, 3, or more wikidata numbers

    elect count(*) from src.external_ids where id_type = 12;
   
    select count(*) from src.admin_data where n_wikidata = 0;
    select count(*) from src.admin_data where n_wikidata = 1; 
    select count(*) from src.admin_data where n_wikidata = 2;
    select count(*) from src.admin_data where n_wikidata = 3;
    select count(*) from src.admin_data where n_wikidata > 3;

    ---------------------------------
   
    Number of orgs with 1, 2, 3, or more wikipedia numbers

    select count(*) from src.links where link_type = 21;

    select count(*) from src.admin_data where n_wikipedia = 0;
    select count(*) from src.admin_data where n_wikipedia = 1; 
    select count(*) from src.admin_data where n_wikipedia = 2;
    select count(*) from src.admin_data where n_wikipedia = 3;
    select count(*) from src.admin_data where n_wikipedia > 3;
    
    Number of orgs with 1, 2, 3, or more website numbers

    select count(*) from src.links where link_type = 22;

    select count(*) from src.admin_data where n_website = 0;
    select count(*) from src.admin_data where n_website = 1; 
    select count(*) from src.admin_data where n_website = 2;
    select count(*) from src.admin_data where n_website = 3;
    select count(*) from src.admin_data where n_website > 3;

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

    Any orgs with domains?

    select count(*) from src.admin_data where n_sucrels > 0;

    -----------------------------------
    
    // set up some tables...

    // Set up some views??...

    // Use these to produce json

    // Use these to produce a text version

    // Create the res file. If it already exists overwrite it.

    let r = org_data_reporter::summarise_data(pool).await;
    match r {
        Ok(()) => {
            info!("Initial data imported to org tables"); 
            return Ok(())
        },
        Err(e) => {
            error!("Oops, an error occured while importing to the org tables: {}", e);
            return Err(AppError::SqErr(e))
            },
    }
    */

    Ok(())

}
