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

use sqlx::FromRow;


mod common;

#[tokio::test] 
async fn import_v2_0_data_to_ror_and_check_org_numbers() {

    // Arrange     
    // Get database pool to allow interrogation of the DB
    let pool = common::fetch_db_pool("ror").await.unwrap();
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
    let rec_number = common::fetch_record_num("core_data", &pool).await;
    assert_eq!(rec_number, 20);

}

#[tokio::test] 
async fn check_numbers_in_each_ror_table() {

    thread::sleep(Duration::from_secs(1));
    let pool = common::fetch_db_pool("ror").await.unwrap();

    let rec_number = common::fetch_record_num("names", &pool).await;
    assert_eq!(rec_number, 56);
    let rec_number = common::fetch_record_num("relationships", &pool).await;
    assert_eq!(rec_number, 25);
    let rec_number = common::fetch_record_num("external_ids", &pool).await;
    assert_eq!(rec_number, 59);
    let rec_number = common::fetch_record_num("links", &pool).await;
    assert_eq!(rec_number, 33);
    let rec_number = common::fetch_record_num("type", &pool).await;
    assert_eq!(rec_number, 30);

}

#[tokio::test] 
async fn check_first_and_last_ids() {

    thread::sleep(Duration::from_secs(1));
    let pool = common::fetch_db_pool("ror").await.unwrap();

    // Check first and last record Ids
    let first_id = common::fetch_first_record_id(&pool).await;
    assert_eq!(first_id, "006jxzx88");

    let last_id = common::fetch_last_record_id(&pool).await;
    assert_eq!(last_id, "05s6t3255");
   
}

#[tokio::test] 
async fn check_first_record_core_data() {

    thread::sleep(Duration::from_secs(1));
    let pool = common::fetch_db_pool("ror").await.unwrap();

    #[derive(Debug, Clone)]
    #[derive(FromRow)]
    struct CoreData {
        ror_full_id: String,
        status: String,
        established: Option<i32>,
    }

    let sql: &str  = "select * from ror.core_data where id = '006jxzx88' LIMIT 1";
    let row: CoreData = sqlx::query_as(sql).fetch_one(&pool).await.unwrap();
    //let rows = common::fetch_record(sql, &pool).await;

    let ror_full_id: String = row.ror_full_id;
    let status: String = row.status;
    let established: i32 = row.established.unwrap();

    assert_eq!(ror_full_id, "https://ror.org/006jxzx88");
    assert_eq!(status, "active");
    assert_eq!(established, 1987);
}


    // Check numbers, values of last org



    // Check location details



    // Check non latin names








