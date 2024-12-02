use sqlx::{Pool, Postgres};

pub async fn recreate_lup_tables (_pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {

/*
    create table lup.org_types_in_ror (
        id int not null primary key, 
        name  varchar
    )
   
    insert into lup.org_types_in_ror(id, name) values (100, 'government');
    insert into lup.org_types_in_ror(id, name) values (200, 'education');
    insert into lup.org_types_in_ror(id, name) values (300, 'healthcare');
    insert into lup.org_types_in_ror(id, name) values (400, 'company');
    insert into lup.org_types_in_ror(id, name) values (500, 'nonprofit');
    insert into lup.org_types_in_ror(id, name) values (600, 'funder');
    insert into lup.org_types_in_ror(id, name) values (700, 'facility');
    insert into lup.org_types_in_ror(id, name) values (800, 'archive');
    insert into lup.org_types_in_ror(id, name) values (900, 'other');

    
    create table lup.countries_in_ror (
    code varchar primary key,
    name varchar
    )

    // or use independent list??

    insert into lup.countries_in_ror (code, name)
    select distinct country_code, country_name
    from src.locations
    order by country_code

    create table lup.name_types_in_ror (
        id int not null primary key, 
        name  varchar
    )
    
    insert into lup.name_types_in_ror(id, name) values (5, 'label');
    insert into lup.name_types_in_ror(id, name) values (7, 'alias');
    insert into lup.name_types_in_ror(id, name) values (10, 'acronym');

    create table lup.id_types_in_ror (
        id int not null primary key, 
        name  varchar
    )
    
    insert into lup.id_types_in_ror(id, name) values (11, 'isni');
    insert into lup.id_types_in_ror(id, name) values (12, 'wikidata');
    insert into lup.id_types_in_ror(id, name) values (13, 'grid');
    insert into lup.id_types_in_ror(id, name) values (14, 'fundref');


    create table lup.link_types_in_ror (
        id int not null primary key, 
        name  varchar
    )
    
    insert into lup.link_types_in_ror(id, name) values (21, 'wikipedia');
    insert into lup.link_types_in_ror(id, name) values (22, 'website');


    create table lup.org_rels_in_ror (
        id int not null primary key, 
        name  varchar
    )
    
    insert into lup.org_rels_in_ror(id, name) values (1, 'parent');
    insert into lup.org_rels_in_ror(id, name) values (2, 'child');
    insert into lup.org_rels_in_ror(id, name) values (3, 'related');
    insert into lup.org_rels_in_ror(id, name) values (4, 'predeceesor');
    insert into lup.org_rels_in_ror(id, name) values (5, 'successor');

    create table lup.lang_codes (
        code varchar not null primary key, 
        marc_code varchar,
        name  varchar
        major  bool,
        source varchar
    )

   insert into lup.lang_codes(code, marc_code, name, major, source) values ('', '', '', true, ISO-6391);  
   ... ... 

    create table lup.script_codes (
        id int not null primary key, 
        code  varchar,
        unicode  varchar,
        dir  varchar,
        chars   int,
        notes   varchar,
        hex_start   varchar,
        hex_end   varchar,
        ascii_start	   int,
        ascii_end   int,	
        source   varchar
    )     

    


    create table lup.org_rels_in_ror (
        id int not null primary key, 
        name  varchar
    )

*/
    Ok(())
}