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

    drop table if exists smm.count_distributions;
    create table smm.count_distributions
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , count_type        varchar     not null
        , count             int         null
        , num_of_orgs       int         null
        , pc_of_orgs        real        null
    );

    
    drop table if exists smm.ranked_distributions;
    create table smm.ranked_distributions
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , dist_type         int         not null 
        , rank              int         not null 
        , entity            varchar     null
        , number            int         null
        , pc_of_entities    real        null
        , pc_of_base_set    real        null
    );


    drop table if exists smm.attributes_summary;
    create table smm.attributes_summary
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , att_type          int         null
        , att_name          varchar     null
        , id                int         null
        , name              varchar     null
        , number_atts       int         null
        , pc_of_atts        real        null
        , number_orgs       int         null
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

   
    drop table if exists smm.singletons;
    create table smm.singletons
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , description       varchar     null
        , number            int         null
        , pc                real        null
    );


    drop table if exists smm.type_relationship;
    create table smm.type_relationship
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , org_type          varchar     null
        , rel_type          varchar     null
        , num_links         int         null
        , num_orgs          int         null
        , num_orgs_total    int         null
        , num_orgs_pc       real        null
    );

    SET client_min_messages TO NOTICE;";

    sqlx::raw_sql(sql).execute(pool).await?;
    Ok(())
    
}








