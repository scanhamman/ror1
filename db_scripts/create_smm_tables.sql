
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
        , total_wolc        int         null
        , pc_wolc           real        null
        , num_label         int         null
        , num_alias         int         null
        , num_acronym       int         null
        , pc_label          real        null
        , pc_alias          real        null
        , pc_acronym        real        null
        , num_label_wolc    int         null
        , num_alias_wolc    int         null
        , num_acro_wolc     int         null
        , num_nacro_wolc    int         null
        , pc_label_wolc     real        null
        , pc_alias_wolc     real        null
        , pc_acro_wolc      real        null
        , pc_nacro_wolc     real        null
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
        , pc_en_ror         real        null
        , pc_nen_ror        real        null
        , pc_wolc_ror       real        null
    );

    drop table if exists smm.name_count_distribution;
    create table smm.name_count_distribution
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , count             int         null
        , count_num         int         null
        , count_pc          real        null
    );

    drop table if exists smm.name_label_distribution;
    create table smm.name_label_distribution
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , count             int         null
        , count_num         int         null
        , count_pc          real        null
    );

    drop table if exists smm.name_alias_distribution;
    create table smm.name_alias_distribution
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , count             int         null
        , count_num         int         null
        , count_pc          real        null
    );

    drop table if exists smm.name_acronym_distribution;
    create table smm.name_acronym_distribution
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , count             int         null
        , count_num         int         null
        , count_pc          real        null
    );

    drop table if exists smm.orgs_of_type_summary;
    create table smm.orgs_of_type_summary
    (    
          vcode             varchar     not null primary key
        , vdate             date        not null
        , total             int         null
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
        , count_num         int         null
        , count_pc          real        null
    );

    drop table if exists smm.type_name_lang_code;
    create table smm.type_name_lang_code
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , org_type          varchar     null
        , name_type         varchar     null
        , count_wlc         int         null
        , count_wolc        int         null
        , pc_count_wlc      real        null
        , pc_count_wolc     real        null
    );

    drop table if exists smm.ext_ids_summary;
    create table smm.ext_ids_summary
    (    
          vcode             varchar     not null primary key
        , vdate             date        not null
        , total             int         null
        , isni              int         null
        , grid              int         null
        , fundref           int         null
        , wikidata          int         null
        , isni_pc           real        null
        , grid_pc           real        null
        , fundref_pc        real        null
        , wikidata_pc       real        null
    );

    drop table if exists smm.ext_ids_count_distribution;
    create table smm.ext_ids_count_distribution
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , count             int         null
        , count_num         int         null
        , count_pc          real        null
    );

    drop table if exists smm.links_summary;
    create table smm.links_summary
    (    
          vcode             varchar     not null primary key
        , vdate             date        not null
        , total             int         null
        , wikipedia         int         null
        , website           int         null
        , website_pc        real        null
        , wikipedia_pc      real        null
    );

    drop table if exists smm.links_count_distribution;
    create table smm.links_count_distribution
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , count             int         null
        , count_num         int         null
        , count_pc          real        null
    );

    drop table if exists smm.relationships_summary;
    create table smm.relationships_summary
    (    
          vcode             varchar     not null primary key
        , vdate             date        not null
        , total_lnks        int         null
        , parent_lnks       int         null
        , child_lnks        int         null
        , related_lnks      int         null
        , predecessor_lnks  int         null
        , successor_lnks    int         null
        , total             int         null
        , parent            int         null
        , child             int         null
        , par_and_ch        int         null
        , related           int         null
        , predecessor       int         null
        , successor         int         null
        , parent_pc         real        null
        , child_pc          real        null
        , par_and_ch_pc     real        null
        , related_pc        real        null
        , predecessor_pc    real        null
        , successor_pc      real        null
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
        , count             int         null
        , count_distinct    int         null
        , count_pc          real        null
    );
  
    drop table if exists smm.country_top_20_distribution;
    create table smm.country_top_20_distribution
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , country           int         null
        , country_num       int         null
        , country_pc        real        null
    );

    drop table if exists smm.locs_count_distribution;
    create table smm.locs_count_distribution
    (    
          vcode             varchar     not null
        , vdate             date        not null
        , count             int         null
        , count_num         int         null
        , count_pc          real        null
    );

   SET client_min_messages TO NOTICE;








