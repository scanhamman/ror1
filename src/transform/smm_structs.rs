use chrono::NaiveDate;

#[derive(sqlx::FromRow)]
pub struct FileParams {
    pub vcode: String,
    pub vdate_as_string: String,
}

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
    pub id: i32,
    pub name: String,
    pub number: i64,
    pub pc_of_atts: f64,
    pub pc_of_orgs: f64,
}