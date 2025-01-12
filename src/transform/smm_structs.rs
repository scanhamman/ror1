use chrono::NaiveDate;

pub struct RorVersion {
  pub vcode: String,
  pub vdate: NaiveDate,
  pub num_orgs: i64,
  pub dvdd: String,
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
pub struct CountryRow {
  pub vcode: String,
  pub vdate: NaiveDate,
  pub country: String,
  pub num_of_locs: i64,
  pub pc_of_locs: f64,
}

#[derive(sqlx::FromRow)]
pub struct LangCodeRow {
  pub vcode: String,
  pub vdate: NaiveDate,
  pub lang: String,
  pub num_of_names: i64,
  pub pc_of_ne_names: f64,
  pub pc_of_all_names: f64,
}

#[derive(sqlx::FromRow)]
pub struct ScriptCodeRow {
  pub vcode: String,
  pub vdate: NaiveDate,
  pub script: String,
  pub num_of_names: i64,
  pub pc_of_nl_names: f64,
  pub pc_of_all_names: f64,
}

