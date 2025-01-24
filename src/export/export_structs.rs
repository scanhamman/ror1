use chrono::NaiveDate;

#[derive(sqlx::FromRow)]
pub struct VSummary {
    pub vcode: String,
    pub vdate: NaiveDate,
    pub vdays: i32,
    pub num_orgs: i32,
    pub num_names: i32,
    pub num_types: i32,
    pub num_links: i32,
    pub num_ext_ids: i32,
    pub num_rels: i32,
    pub num_locations: i32,
    pub num_domains: i32,
}

#[derive(sqlx::FromRow)]
pub struct SingletonRow {
    pub number: i32,
    pub pc: Option<f32>,
}

/* 
#[derive(sqlx::FromRow)]
pub struct DistribRow {
  pub vcode: String,
  pub count: i32,
  pub num_of_orgs: i64,
  pub pc_of_orgs: f64,
}

#[derive(sqlx::FromRow)]
pub struct RankedRow {
  pub vcode: String,
  pub entity: String,
  pub number: i64,
  pub pc_of_entities: f64,
  pub pc_of_base_set: f64,
}

#[derive(sqlx::FromRow)]
pub struct TypeRow {
    pub vcode: String,
    pub id: i32,
    pub name: String,
    pub number_atts: i64,
    pub pc_of_atts: f64,
    pub number_orgs: i64,
    pub pc_of_orgs: f64,
}

#[derive(sqlx::FromRow)]
pub struct OrgRow {
    pub type_id: i32, 
    pub name: String,
    pub org_num: i64, 
}

*/



