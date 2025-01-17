use sqlx::{Pool, Postgres};
use crate::AppError;

pub async fn create_tables(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = r"
    SET client_min_messages TO WARNING; 
    create schema if not exists smm;
 
    drop table if exists smm.version_summary;
    create table smm.version_summary
    (    
          vcode             varchar     not null primary key
        , vdate             date        not null
        , num_orgs          int         null	
        , num_names         int         null	
        , num_types         int         null
        , num_links         int         null
        , num_ext_ids       int         null
        , num_rels          int         null
        , num_locations     int         null
        , num_domains       int         null
    );

    drop table if exists smm.name_summary;
    create table smm.name_summary
    (    
          vcode             varchar     not null primary key
        , vdate             date        not null
        , total             int         null 
        , num_label         int         null
        , num_alias         int         null
        , num_nacro         int         null
        , num_acronym       int         null
        , pc_label          real        null
        , pc_alias          real        null
        , pc_nacro          real        null
        , pc_acronym        real        null
        , num_nacro_ne      int         null
        , pc_nacro_ne       real        null
        , num_nltn          int         null
        , pc_nltn           real        null
    );


    drop table if exists smm.name_lang_code;
    create table smm.name_lang_code
    (    
          vcode             varchar     not null primary key
        , vdate             date        not null
        , total             int         null 
        , total_wolc        int         null
        , label_wolc        int         null
        , alias_wolc        int         null
        , acro_wolc         int         null
        , nacro_wolc        int         null
        , ncmpacr_wolc      int         null
        , pc_total_wolc     real        null
        , pc_label_wolc     real        null
        , pc_alias_wolc     real        null
        , pc_acro_wolc      real        null
        , pc_nacro_wolc     real        null
        , pc_ncmpacr_wolc   real        null
    );


    drop table if exists smm.name_ror;
    create table smm.name_ror
    (    
          vcode             varchar     not null primary key
        , vdate             date        not null
        , num_label_ror     int         null
        , num_label_nror    int         null
        , num_nlabel_ror    int         null
        , pc_nlabel_ror     real        null
        , num_en_ror        int         null
        , num_nen_ror       int         null
        , num_wolc_ror      int         null
        , num_ncmp_wolc_ror int         null
        , pc_en_ror         real        null
        , pc_nen_ror        real        null
        , pc_wolc_ror       real        null
        , pc_ncmp_wolc_ror  real        null
    );


    drop table if exists smm.name_count_distribution;
    create table smm.name_count_distribution
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , count             int         null
        , num_of_orgs       int         null
        , pc_of_orgs        real        null
    );

    drop table if exists smm.name_label_distribution;
    create table smm.name_label_distribution
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , count             int         null
        , num_of_orgs       int         null
        , pc_of_orgs        real        null
    );

    drop table if exists smm.name_alias_distribution;
    create table smm.name_alias_distribution
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , count             int         null
        , num_of_orgs       int         null
        , pc_of_orgs        real        null
    );

    drop table if exists smm.name_acronym_distribution;
    create table smm.name_acronym_distribution
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , count             int         null
        , num_of_orgs       int         null
        , pc_of_orgs        real        null
    );


    drop table if exists smm.ne_lang_code_distribution;
    create table smm.ne_lang_code_distribution
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , lang              varchar     null
        , num_of_names      int         null
        , pc_of_ne_names    real        null
        , pc_of_all_names   real        null
    );


    drop table if exists smm.nl_lang_script_distribution;
    create table smm.nl_lang_script_distribution
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , script            varchar     null
        , num_of_names      int         null
        , pc_of_nl_names    real        null
        , pc_of_all_names   real        null
    );

    drop table if exists smm.type_summary;
    create table smm.type_summary
    (    
          vcode             varchar     not null primary key
        , vdate             date        not null
        , num_types         int         null
        , government        int         null
        , education         int         null
        , healthcare        int         null
        , company           int         null
        , nonprofit         int         null
        , funder            int         null
        , facility          int         null
        , archive           int         null
        , other             int         null
        , government_pc     real        null
        , education_pc      real        null
        , healthcare_pc     real        null
        , company_pc        real        null
        , nonprofit_pc      real        null
        , funder_pc         real        null
        , facility_pc       real        null
        , archive_pc        real        null
        , other_pc          real        null
    );

    drop table if exists smm.type_by_orgs_summary;
    create table smm.type_by_orgs_summary
    (    
          vcode             varchar     not null primary key
        , vdate             date        not null
        , num_orgs          int         null
        , government        int         null
        , education         int         null
        , healthcare        int         null
        , company           int         null
        , nonprofit         int         null
        , funder            int         null
        , facility          int         null
        , archive           int         null
        , other             int         null
        , government_pc     real        null
        , education_pc      real        null
        , healthcare_pc     real        null
        , company_pc        real        null
        , nonprofit_pc      real        null
        , funder_pc         real        null
        , facility_pc       real        null
        , archive_pc        real        null
        , other_pc          real        null
    );

    drop table if exists smm.type_count_distribution;
    create table smm.type_count_distribution
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , count             int         null
        , num_of_orgs       int         null
        , pc_of_orgs        real        null
    );

    drop table if exists smm.type_name_lang_code;
    create table smm.type_name_lang_code
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , org_type          varchar     null
        , name_type         varchar     null
        , names_num         int         null
        , names_wlc         int         null
        , names_wolc        int         null
        , names_wlc_pc      real        null
        , names_wolc_pc     real        null
    );

    drop table if exists smm.ext_ids_summary;
    create table smm.ext_ids_summary
    (    
          vcode             varchar     not null primary key
        , vdate             date        not null
        , num_ids           int         null
        , isni              int         null
        , grid              int         null
        , fundref           int         null
        , wikidata          int         null
        , isni_pc           real        null
        , grid_pc           real        null
        , fundref_pc        real        null
        , wikidata_pc       real        null
        , num_orgs          int         null
        , isni_orgs         int         null
        , grid_orgs         int         null
        , fundref_orgs      int         null
        , wikidata_orgs     int         null
        , isni_pc_orgs      real        null
        , grid_pc_orgs      real        null
        , fundref_pc_orgs   real        null
        , wikidata_pc_orgs  real        null
    );

    drop table if exists smm.ext_ids_count_distribution;
    create table smm.ext_ids_count_distribution
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , count             int         null
        , num_of_orgs       int         null
        , pc_of_orgs        real        null
    );

    drop table if exists smm.links_summary;
    create table smm.links_summary
    (    
          vcode             varchar     not null primary key
        , vdate             date        not null
        , num_links         int         null
        , wikipedia         int         null
        , website           int         null
        , website_pc        real        null
        , wikipedia_pc      real        null
        , num_orgs          int         null
        , wikipedia_orgs    int         null
        , website_orgs      int         null
        , wikipedia_pc_orgs real        null
        , website_pc_orgs   real        null
    );

    drop table if exists smm.links_count_distribution;
    create table smm.links_count_distribution
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , count             int         null
        , num_of_orgs       int         null
        , pc_of_orgs        real        null
    );

    drop table if exists smm.relationships_summary;
    create table smm.relationships_summary
    (    
          vcode             varchar     not null primary key
        , vdate             date        not null
        , total_lnks        int         null
        , parent_lnks       int         null
        , child_lnks        int         null
        , rel_lnks          int         null
        , pred_lnks         int         null
        , succ_lnks         int         null
        , parent_lnks_pc    real        null
        , child_lnks_pc     real        null
        , rel_lnks_pc       real        null
        , pred_lnks_pc      real        null
        , succ_lnks_pc      real        null
        , total_orgs        int         null
        , parent_orgs       int         null
        , child_orgs        int         null
        , parch_orgs        int         null
        , rel_orgs          int         null
        , pred_orgs         int         null
        , succ_orgs         int         null
        , parent_orgs_pc    real        null
        , child_orgs_pc     real        null
        , parch_orgs_pc     real        null
        , rel_orgs_pc       real        null
        , pred_orgs_pc      real        null
        , succ_orgs_pc      real        null
        , non_recip_pc      int         null
        , non_recip_rr      int         null
        , non_recip_ps      int         null
    );

    drop table if exists smm.type_relationship;
    create table smm.type_relationship
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , org_type          varchar     null
        , org_type_total    int         null
        , rel_type          varchar     null
        , num_links         int         null
        , num_orgs          int         null
        , num_orgs_total    int         null
        , num_orgs_pc       real        null
    );
  
    drop table if exists smm.country_distribution;
    create table smm.country_distribution
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , country           varchar     null
        , num_of_locs       int         null
        , pc_of_locs        real        null
    );

    drop table if exists smm.locs_count_distribution;
    create table smm.locs_count_distribution
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , count             int         null
        , num_of_orgs       int         null
        , pc_of_orgs        real        null
    );

    SET client_min_messages TO NOTICE;";

    sqlx::raw_sql(sql).execute(pool).await?;
    Ok(())
    
}








