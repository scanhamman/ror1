// To check if ror data tables exist and have 
// correct number of records - using a fixed test sample source set
// Test samples should exist for each version of the v2 schema 
// to test that all file types can be correctly processed.

// `tokio::test` is the testing equivalent of `tokio::main`. 
// It also spares you from having to specify the `#[test]` attribute. 
// You can inspect what code gets generated using 
// `cargo expand --test health_check` (<- name of the test file) 

use ror1::run;
use std::env;
use std::ffi::OsString;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;
use chrono::NaiveDate;

use super::record_structs::{RorCoreData, RorRelationship, RorExternalId, 
                            RorName, RorLocation, RorLink, RorType, RorAdminData};
use super::data_access;


#[tokio::test] 
async fn import_v2_0_data_to_ror_and_check_org_numbers() {

    // Arrange     
    // Get database pool to allow interrogation of the DB
    let pool = data_access::fetch_db_pool("ror").await.unwrap();
    // Establish the arguments for running the database

    // Act 
    // Run the program with v2.0 test data
    let cd_path = env::current_dir().unwrap();
    println!("The current directory is {}", cd_path.display());
    let target_path : PathBuf = [cd_path, PathBuf::from("tests/test_data/")].iter().collect();
    let target_folder = target_path.to_str().unwrap();
    let target_file = "v2_test_data.json";
    let tdate = "2026-01-01";
    let args : Vec<&str> = vec!["target/debug/ror1.exe", "-f", target_folder, "-s", target_file, "-v", "v2", "-d", tdate, "-r", "-t"];
    let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
    run(test_args).await.unwrap();

    // Assert     
    // Check numbers of records
    let rec_number = data_access::fetch_record_num("core_data", &pool).await;
    assert_eq!(rec_number, 20);
}


#[tokio::test] 
async fn check_numbers_in_each_ror_table() {

    thread::sleep(Duration::from_secs(1));
    let pool = data_access::fetch_db_pool("ror").await.unwrap();

    let rec_number = data_access::fetch_record_num("names", &pool).await;
    assert_eq!(rec_number, 56);
    let rec_number = data_access::fetch_record_num("relationships", &pool).await;
    assert_eq!(rec_number, 25);
    let rec_number = data_access::fetch_record_num("external_ids", &pool).await;
    assert_eq!(rec_number, 59);
    let rec_number = data_access::fetch_record_num("links", &pool).await;
    assert_eq!(rec_number, 33);
    let rec_number = data_access::fetch_record_num("type", &pool).await;
    assert_eq!(rec_number, 30);
}


#[tokio::test] 
async fn check_ror_first_and_last_ids() {

    thread::sleep(Duration::from_secs(1));
    let pool = data_access::fetch_db_pool("ror").await.unwrap();

    // Check first and last record Ids
    let first_id = data_access::fetch_first_record_id(&pool).await;
    assert_eq!(first_id, "006jxzx88");

    let last_id = data_access::fetch_last_record_id(&pool).await;
    assert_eq!(last_id, "05s6t3255");
}


#[tokio::test] 
async fn check_ror_core_data() {

    thread::sleep(Duration::from_secs(1));
    let pool = data_access::fetch_db_pool("ror").await.unwrap();

    let id = "006jxzx88";

    let core_data: RorCoreData = data_access::fetch_core_data_record (id, &pool).await;
    assert_eq!(core_data.ror_full_id, "https://ror.org/006jxzx88");
    assert_eq!(core_data.status, "active");
    assert_eq!(core_data.established.unwrap(), 1987);
    
    let cr_dt = NaiveDate::parse_from_str("2018-11-14", "%Y-%m-%d").unwrap();
    let lm_dt= NaiveDate::parse_from_str("2024-05-13", "%Y-%m-%d").unwrap();
    let admin_data: RorAdminData = data_access::fetch_admin_data_record (id, &pool).await;
    assert_eq!(admin_data, RorAdminData{
                   created: cr_dt, cr_schema: "1.0".to_string(), 
                   last_modified: lm_dt, lm_schema: "2.0".to_string()});

    let id = "05s6t3255";
    
    let core_data: RorCoreData = data_access::fetch_core_data_record (id, &pool).await;
    assert_eq!(core_data.ror_full_id, "https://ror.org/05s6t3255");
    assert_eq!(core_data.status, "active");
    assert_eq!(core_data.established.unwrap(), 2012);

    let cr_dt = NaiveDate::parse_from_str("2023-07-27", "%Y-%m-%d").unwrap();
    let lm_dt= NaiveDate::parse_from_str("2024-12-11", "%Y-%m-%d").unwrap();
    let admin_data: RorAdminData = data_access::fetch_admin_data_record (id, &pool).await;
    assert_eq!(admin_data, RorAdminData{
        created: cr_dt, cr_schema: "1.0".to_string(), 
        last_modified: lm_dt, lm_schema: "2.1".to_string()});

}

#[tokio::test] 
async fn check_ror_relationship_data() {

    thread::sleep(Duration::from_secs(2));
    let pool = data_access::fetch_db_pool("ror").await.unwrap();

    let id = "03rd8mf35";
    let rels:Vec<RorRelationship> = data_access::fetch_relationship_records(id, &pool).await;
    assert_eq!(rels.len(), 1);
    assert_eq!(rels[0], RorRelationship{
        rel_type: "parent".to_string(), related_id: "05wwcw481".to_string(), 
        related_label: "Bournemouth University".to_string(),});

    let id = "04ttjf776";
    let rels:Vec<RorRelationship> = data_access::fetch_relationship_records(id, &pool).await;
    assert_eq!(rels.len(), 4);
    assert_eq!(rels[0], RorRelationship{
        rel_type: "child".to_string(), related_id: "004axh929".to_string(), 
        related_label: "RMIT Vietnam".to_string(),});
    assert_eq!(rels[1], RorRelationship{
        rel_type: "related".to_string(), related_id: "010mv7n52".to_string(), 
        related_label: "Austin Hospital".to_string(),});
}


#[tokio::test] 
async fn check_ror_external_id_data() {

    thread::sleep(Duration::from_secs(2));
    let pool = data_access::fetch_db_pool("ror").await.unwrap();

    let id = "04ttjf776";
    let extids:Vec<RorExternalId> = data_access::fetch_external_id_records(id, &pool).await;
    assert_eq!(extids.len(), 6);
    assert_eq!(extids[0], RorExternalId{
        id_type: "isni".to_string(), id_value: "0000 0001 2163 3550".to_string(), 
        is_preferred: Some(true)},);
    assert_eq!(extids[1], RorExternalId{
        id_type: "fundref".to_string(), id_value: "100008690".to_string(), 
        is_preferred: None},);

    let id = "02vsmry93";
    let extids:Vec<RorExternalId> = data_access::fetch_external_id_records(id, &pool).await;
    assert_eq!(extids.len(), 3);
    assert_eq!(extids[1], RorExternalId{
        id_type: "fundref".to_string(), id_value: "100020630".to_string(), 
        is_preferred: Some(true),});
    assert_eq!(extids[2], RorExternalId{
        id_type: "wikidata".to_string(), id_value: "Q374071".to_string(), 
        is_preferred: Some(true),});

}

#[tokio::test] 
async fn check_ror_location_data() {

    thread::sleep(Duration::from_secs(2));
    let pool = data_access::fetch_db_pool("ror").await.unwrap();

    let id = "006jxzx88";
    let locs:Vec<RorLocation> = data_access::fetch_location_records(id, &pool).await;
    assert_eq!(locs.len(), 1);
    assert_eq!(locs[0], RorLocation{
        geonames_id: 2165087, name: "Gold Coast".to_string(), 
        lat: Some(-28.073982), lng: Some(153.41649), 
        cont_code: None, cont_name: None, 
        country_code: "AU".to_string(), country_name:"Australia".to_string(), 
        csubdiv_code: None, csubdiv_name: None,});

    let id = "05s6t3255";
    let locs:Vec<RorLocation> = data_access::fetch_location_records(id, &pool).await;
    assert_eq!(locs.len(), 1);
    assert_eq!(locs[0], RorLocation{
        geonames_id: 2657896, name: "Zurich".to_string(), 
        lat: Some(47.36667), lng: Some(8.55), 
        cont_code: Some("EU".to_string()), cont_name: Some("Europe".to_string()), 
        country_code: "CH".to_string(), country_name:"Switzerland".to_string(), 
        csubdiv_code: Some("ZH".to_string()), csubdiv_name: Some("Zurich".to_string()),});

}

#[tokio::test] 
async fn check_ror_link_data() {

    thread::sleep(Duration::from_secs(2));
    let pool = data_access::fetch_db_pool("ror").await.unwrap();

    let id = "006jxzx88";
    let links:Vec<RorLink> = data_access::fetch_link_records(id, &pool).await;
    assert_eq!(links.len(), 2);
    assert_eq!(links[0], RorLink{
        link_type: "website".to_string(), value: "http://bond.edu.au/".to_string(),});
    assert_eq!(links[1], RorLink{
        link_type: "wikipedia".to_string(), value: "https://en.wikipedia.org/wiki/Bond_University".to_string(),});

    let id = "05s6t3255";
    let links:Vec<RorLink> = data_access::fetch_link_records(id, &pool).await;
    assert_eq!(links.len(), 2);
    assert_eq!(links[0], RorLink{
        link_type: "wikipedia".to_string(), value: "https://en.wikipedia.org/wiki/Food_Packaging_Forum".to_string(),});
    assert_eq!(links[1], RorLink{
        link_type: "website".to_string(), value: "https://www.foodpackagingforum.org".to_string(),});

}

#[tokio::test] 
async fn check_ror_type_data() {

    thread::sleep(Duration::from_secs(2));
    let pool = data_access::fetch_db_pool("ror").await.unwrap();

    let id = "006jxzx88";
    let types:Vec<RorType> = data_access::fetch_type_records(id, &pool).await;
    assert_eq!(types.len(), 2);
    assert_eq!(types[0], RorType{org_type: "education".to_string(),});
    assert_eq!(types[1], RorType{org_type: "funder".to_string(),});

    let id = "05s6t3255";
    let types:Vec<RorType> = data_access::fetch_type_records(id, &pool).await;
    assert_eq!(types.len(), 1);
    assert_eq!(types[0], RorType{org_type: "nonprofit".to_string(),});

}

#[tokio::test]
async fn check_ror_name_data() {

    thread::sleep(Duration::from_secs(2));
    let pool = data_access::fetch_db_pool("ror").await.unwrap();

    let id = "0198t0w55";
    let names:Vec<RorName> = data_access::fetch_name_records(id, &pool).await;
    assert_eq!(names.len(), 7);
    assert_eq!(names[0], RorName{
        value: "Institute of Reflective Investigation and Specialization".to_string(), name_type: "label".to_string(), 
        is_ror_name: Some(true), lang: Some("en".to_string()),});
    assert_eq!(names[4], RorName{
        value: "Інститут Рефлексивних Досліджень & Спеціалізації".to_string(), name_type: "label".to_string(), 
        is_ror_name: None, lang: Some("uk".to_string()),});

    let id = "052rpwb50";
    let names:Vec<RorName> = data_access::fetch_name_records(id, &pool).await;
    assert_eq!(names.len(), 7);
    assert_eq!(names[0], RorName{
        value: "Yamashita Kōjōsen Byōin".to_string(), name_type: "alias".to_string(), 
        is_ror_name: None, lang: None,});
    assert_eq!(names[4], RorName{
        value: "やまし た こうじょう せん びょう いん".to_string(), name_type: "alias".to_string(), 
        is_ror_name: None, lang: Some("ja".to_string()),});
}









