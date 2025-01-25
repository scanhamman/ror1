A small program to process and summarise ROR organisation data, as made available by ROR 
on Zenodo (see https://ror.readme.io/docs/data-dump). A new version of the data is posted 
on a roughly monthly basis. The program processes and retains a single version at a time, 
but retains summaries of the key features of each version. 
The system uses the version 2 schema files as input, and so covers data made available from
April 2024 onwards. It can handle versions 2.0 and 2.1, the latter in use from December 2024.

The system is written in Rust, uses a command line interface (CLI) for control, and 
requires a Postgres database to provide the back-end datastore. 

<i>N.B. At the moment, the program is not yet available as a stand alone .exe or .lib file, 
though it is hoped to create these in the future. The current system therefore needs the 
source code to be downloaded from GitHub, and then run within a Rust development environment 
(see 'Pre-requisites' below).</i>

<h3>Purpose</h3>

The system was developed for three main reasons:

a) To provide a mechanism to quickly download and integrate the ROR data within other 
systems, on a regular basis (e.g. as each new version is published). The ROR data is available
both in its 'raw' ror state, i.e. with almost no additional processing applied (see 
'The base ror data schema' below), and in a lightly modified state, with a limited degree 
of processing applied (see 'The src data schema' below). The latter might be of more immediate
use in many use cases, or a better basis for additional processing.

b) To allow comparison of the different ROR data versions over time, allowing monitoring of 
the development of ROR data, and the easier identification of possible inconsistencies or 
anomalies in the data (to help with possible feedback to ROR).

c) To become more familiar with Rust, by using the language in a small but realistic development 
scenario, implementing features that would be necessary in most similar CLIs. These include 
extensive systems for data access and manipulation, processing of command line arguments and 
environmental variables (and interactions between the two), logging, file handling (of both JSON 
and text files), and unit and integration tests. The system is still 'basic' or beginners Rust, however, and does not use more advanced features of the language.

<h3>The base ror data schema</h3>

When a ROR data file is imported it is first transformed into tables that almost exactly mirror 
the original structure of the json file. These tables are all grouped within a schema called 'ror'. 
Only one set of ror schema tables exists at any one time, relating to a specific ROR data version.
The import process recreates the tables each time.

The id used to identify each ror entry, in all tables, is the last 9 characters of the full ROR id, 
i.e. the ROR URL with the prefix "https://ror.org/" removed. It is clearer to use only the variable 
part of the ROR URL rather than the full string, though that full string remains as a field in the core_data table. The core_data table contains only the id, full id (URL), status and year 
established. The other singleton data of the ror record, relating to date and schema of creation 
and last modification, are collected separately into an 'admin_data' table.

All other data potentially represents multiple entities for each organisation, even though at the 
moment (January 2025) all organisations only have a single location, and no organisation has any
associated domains. Field names are, in most cases, the same or have an obvious correspondence to 
the field names as listed in the ROR documentation.

The main exception to this is caused by the fact that 'type', whilst used in several places in the 
ROR definitions, is a reserved word in Rust (and many other languages), while 'TYPE' can cause issues 
in Postgres in some contexts. For safety and future compatibility the following changes are made:
<ul>
<li>'type' in names becomes 'name_type'.</li>
<li>'type' in types becomes 'org_type'.</li>
<li>'type' in external ids becomes 'id_type'.</li>
<li>'type' in links becomes 'link_type'.</li>
<li>'type' in relationships becomes'rel_type'.</li>
</ul>

The ror schema data also includes a timy single-row table ('version details') that holds the version and 
date of the data within the system. This means that these parameters need only be input once. 

The ror schema is used as the basis of later processes in this system, but is retained so as to be available 
for other transformations by other systems, if required.

<h3>The src data schema</h3>

After initial import into the 'ror' schema, the data can be processed to form a new set of tables, 
within the 'src' schema. The changes are limited but include:

a) Replacement of the strings of categorised values by integers. The integers are as given by
lookup tables (set up within the 'lup' or lookup schema) which effectively provide enumerations 
of these categorised values, e.g. the organisation, name, link, external id and relationship types. 
This is intended to make any future data processing quicker and future display more flexible.

b) The removal of duplicates from the names table. There are a small number of organisations that 
have two names with the same value. In most cases (currently 65) these are names of different types, usually both a label and an alias. In about half of these cases the name is also designated (usually for both versions) as a 'ror name'. In a further 9 cases (currently) the names are the same type but have two different language codes applied. These duplications are removed, according to the folowing rules:
<ul>
<li>If one of the duplicte pairs is a ror name and the other is not, the one that is not is removed.</li> 
<li>If one is a label and the other an alias or acronym, the alias or acronym is removed.</li> 
<li>If one is an alias and the other an acronym, the alias is removed, as the names in this group all appear to be acronyms.</li> 
<li>For the remaining 6 duplicated names, the language code least associated with the organisation's location, or if that is not clear that is referring to the more obscure language, is removed. This is an arbitrary decision but the choices are not difficult in practice.</li>
</ul>
While having a name categorised as two name types does seem a clear error, there is a possible debate about names that could be claimed to be genuinely the same in two or more languages. For the time being, however, it is simpler to remove all of the (very small number of) duplicates. 

c) The addition of script codes to the name data. Though most of the the names listed (apart 
from acronyms and company names) have language codes linked to them there is no explicit indication of 
the script being used. The great majority of the names use latin characters, but a substantial number 
use other script systems, such as Cyrilic, Greek, Arabic, Han, Hebrew or Gujarati. Details on scripts are 
provided by ISO 15924, which also provides the Unicode code pages on which each script can be found. 
Examining the Unicodes of the characters in the names allows the script to be readily identified, and this 
information is added to each name record, as being of potential value when selecting names for display.

d) The expansion of the admin_data table, to include for each organisation the numbers of entities 
of each type it is linked with, e.g. how many names (of various types), links and external ids (of 
various types), relationships (of various types), locations, etc. are included in the ror record.
This is to make it easier both to use and display the information, to support some of the 
production of summary data, and to more easily identify organisations that are missing certain 
types of data.

e) The renaming of a few field names to make them clearer, more consistent or simpler, e.g. country_subdivision_code becomes csubdiv_code, continent_code becomes cont_code, lang becomes lang_code, etc.
 
The src data is designed to be used as the basis for ad hoc SQL queries of the data. They are also used as 
the basis of the summary statistics described below, and are designed to provide a more useful set of base 
data when integrating ror data into other systems. Only one set of src data exists at any one time - the 
tables are recreated each time a version's data is transformed into them.

<h3>Summary data and the smm schema</h3>

The Summary (smm) schema includes a set of persistent tables that summarise various aspects of the ROR dataset. It includes records for all versions of the ROR data that have been imported (within each table the initial field is the data version, allowing easy selection of the summary data for any particular version). To make processing and export easier, many of the summary tables are aggregate, i.e. they hold data about different entities in the same table, because that data has the same structure. The tables are:

<ul>
<li>version_summary - Gives the number of organisations, and the numbers of linked entities (names, organisation types, locations, external ids, links, relationships, domains), for a specified version, equivalent to the record numbers in each of the tables in the src schemas when the version is processed. It also includes the version date, and the number of days that date represents since 11/04/2024, whern the ROR v2 schema was first published.</li>

<li>attributes_summary - Entities in the system have categorised attributes, e.g. the various types of name, organisation, relationship, external id and link. For each attribute value, this table provides the numbers found, and the percentage this represents of the total attributes of this type, plus the number of organisations with this attribute type, and the percentage this represents of all organisations. For names, additional rows are given for 'nacro' or non-acronym names, i.e. labels and aliases together, and also for names (of each type) that are without a language code ('wolc').</li>

<li>count_distributions - Indicates the numbers and percentage of organisations that are linked to different numbers (counts) of properties. This includes the numbers and percentages of organisations with 'n' names, labels, aliases, acronyms, organisational types, locations, external ids, and links, where n varies over whatever count values are found in the dataset. For instance, for organisational types n currently varies (January 2025) from 1 to 3, for names, from 1 to 28.</li>

<li>ranked_distributions - Three ranked distributions are provided: giving the usage of non English languages,  the usage of non-Latin scripts, and the countries listed in locations. In each case the numbers for the 25 most common (language / script / country) values are listed, with numbers for remaining languages, scripts and countries rolled up into a 26th 'remaining' entry. The percentages each entry represents of the property of interest (non English languages, non Latin scripts and countries other than the US) and the percentage of the 'base set' (names, names and locations respectively) are also provided. </li>

<li>org_type_and_lang_code - For each combination of organisational type and name type, gives the numbers and percentages of names with and without language codes.</li>

<li>org_type_and_relationships - For each combination of organisational type and relationship type, gives the numbers and percentages (of that organisational type) which include that relationship.</li>

<li>singletons - There are a variety of useful measures (currently 16) which do not easily fit into any of the tables listed above. They are provided as a table which includes an id and a description for each data point, the number found and where relevant a percentage (both defined in the description). The singleton data points include, for instance, the numbers of labels that are designated as the ROR name, labels not so designated, the numbers and percentages of English and non English ror names, and the ROR names without language codes, including and excluding company names. They also include the numbers and percentage of organisations that have both parents <i>and</i> child links, i.e. are part of a hierarchy of at least 3 levels, plus the numbers of any non-reciprocated relationship records.</li>
</ul>

<h3>Pre-requisites</h3>

1) The system assumes that any v2 ROR data file required has already been downloaded from the Zenodo site and placed on the machine running the program, in a designated 'data folder'.<br>
2) The system requires a postgres database for holding the data. By default, this database is assumed 
to be named 'ror', but this can be changed in the configuration file (see below). The database must be
created prior to the intial run of the system.<br> 
3) <i>At the moment</i> - a Rust development environment is also required, as the system is most easily run from that environment. This means installing Rust and an IDE. VS Code is recommended as - like Rust itself - it is available free of charge. A means of inspecting the Postgres database is also necessary - PgAdmin and / or DBeaver (Community edition) are both free and very capable systems for this purpose.</i>

<h3>Operation and arguments</h3>

<i>Configuration using Environmental varables</i>

Once the pre-requisites are installed and the source code is downloaded, a .env file must be added to the
system's source folder, i.e. in the same folder as the cargo.toml file. This .env file, which should not 
be added to any public source control system, acts as a configuration file for the system (it does not 
change the system's environmental values). It must contain values against the following settings, though 
in several cases sensible defaults are provided:
<ul>
<li>The database server name, as 'db_host'. This defaults to 'localhost'</li>
<li>The database user name, as 'db_user'. No default value.</li>
<li>The password for that user, as 'db_password'. No default value.</li>
<li>The database port, as 'db_port'. This defaults to '5432', the standard Postgres port.</li>
<li>The database name, as 'db_name'. This defaults to 'ror'.</li>
<li>The full path of the folder in which the souce JSON file can be found, as 'data_folder_path'.</li>
<li>The full path of the folder where logs should be written, as 'log_folder_path'. If missing the data_folder_path is used.</li>
<li>The full path of the folder where output text files should be written, as 'output_folder_path'. If missing the data_folder_path is used.</li>
</ul>

The following are normally supplied by command line arguments, which will always over-write values in the configuration file. During testing and development however, against a fixed source file, it can be easier to include them in the .env file instead.
<ul>
<li>The name of the souce JSON file, as 'src_file_name'.</li>
<li>The name of the output file, as 'output_file_name'. If missing the system will construct a name based on the source file and date-time.</li>
<li>The version of the file to be imported, as 'data_version'. A string, with a 'v' followed by a set of numbers in a semantic versioning format, e.g. 'v1.45.1', 'v1.57'.
<li>The date of the file to be imported, as 'data_date'. This should be in the YYYY-mm-DD ISO format.
</ul>

In future versions this configuration file will be installed in an OS-specific configuration folder.

<i>Set-up and initial run</i>

The system needs to have the lookup tables (filled with their data) and summary tables (empty) in place before an import and further processing can take place. It should therefore first be run with an '-i' command line argument, which ensures that the lup schema tables are created and filled, and the smm tables are created. In normal circumstances this should only need doing once. 
In the context of a rust development environment, using cargo, the program's arguments must be distinguished from cargo's own arguments by a double hyphen. The command is therefore:
<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<b>cargo run -- -i</b>

<i>Command line arguments</i>

The folowing command line arguments are available:

<i><b>-s</b></i>&nbsp;&nbsp;&nbsp;&nbsp;[or -S, -source]. Followed by a double quoted string representing the source file name, including the '.json' extension.

<i><b>-f</b></i>&nbsp;&nbsp;&nbsp;&nbsp;[or -F, -folder]. Followed by a double quoted string representing the full path to the source data folder. Usually provided as a configuration variable, but the CLI argument will over-write that if present.

<i><b>-v</b></i>&nbsp;&nbsp;&nbsp;&nbsp;[or -data_version]. Followed by a double quoted string representing a version number, e.g. "v1.52". In many circumstances can be derived from the surce file name. (Note that 'V' is <i>not</i> an option in this case, as it reserved by the command line processing system).

<i><b>-d</b></i>&nbsp;&nbsp;&nbsp;&nbsp;[or -D, -date]. Followed by a double quoted string in ISO YYYY-mm-DD format, representing the date of the data. In many circumstances can be derived from the surce file name.

<i><b>-r</b></i>&nbsp;&nbsp;&nbsp;&nbsp;[or -R, -import]. A flag that causes import of the specified source data to ror schema tables. The source file, data version and data date must be specified.  

<b><i>Note that if the source file name follows a simple convention (described below) it is possible for the system to derive the version and date from the name. The file as named by ROR follows this convention, so in most cases, unless the file is renamed in an entirely different way, it is not necessary to specify the data'a version and date separately.</b></i>

<i><b>-p</b></i>&nbsp;&nbsp;&nbsp;&nbsp;[or -P, -process]. A flag that causes processing and summarising of the data in the ror schema tables to the src and smm schema tables. 

<i><b>-t</b></i>&nbsp;&nbsp;&nbsp;&nbsp;[or -T, -report]. A flag that causes production of a text file summarising the main features of a version currently held within the system's summary tables. The version can be specified explicitly using the -v flag. If not specified the 'current' version is used, i.e. the last imported one, which has its data in the ror and src schema. The name of the output file is normally constructed from the version and the date-time of the run, but can be specified in the configuration file, e.g. during testing. 

<i><b>-a</b></i>&nbsp;&nbsp;&nbsp;&nbsp;[or -A, -all]. Equivalent to -r -p -t, i.e. run all main processes, in that order. The source file, data version and data date must be specified, but the latter two can usually be derived from the first.

<i><b>-x</b></i>&nbsp;&nbsp;&nbsp;&nbsp;[or -X, -export]. A flag that causes production of a small collection of csv files, representing the data in the summary tables for the specified version. The version can be specified explicitly using the -v flag. If not specified the 'current' version is used, i.e. the last imported one, which has its data inthe rior and src schema. The name of the files are constructed from the version and the date-time of the run.

<i><b>-i</b></i>&nbsp;&nbsp;&nbsp;&nbsp;[or -I, -install].  Equivalent to -c -m, i.e. initialise the permanent data tables.

<i><b>-c</b></i>&nbsp;&nbsp;&nbsp;&nbsp;[or -C, -context]. A flag that causes the re-establishment of the lookup tables. Useful after any revision of those tables or the data within them.

<i><b>-m</b></i>&nbsp;&nbsp;&nbsp;&nbsp;[or -M, -summsetup]. A flag that causes the re-establishment of the summary tables in the smm schema. NOTE - ANY EXISTING DATA IN THOSE TABLES WILL BE DESTROYED. It may therefore be necessary to re-run against source files if a series of data points over time needs to be re-established.

<h4>File name convention and deriving version and data</h4>

If the file name starts with a 'v' followed by a semantic versioning string, followed by a space or a hyphen and then the date in ISO format, either with spaces or without, then (whatever any following text in the name) the system is able to extract the data date and version from the file name. It is then no longer necessary to provide the data version and date separately. 

File names such as <b>v1.58-2024-12-11-ror-data_schema_v2.json, v1.51-20240821.json, v1.48 20240620.json</b>, and <b>v1.47 2024-05-30.json</b> all follow the required pattern. The first is the form of the name supplied by ROR, so renaming the file is not necessary (though it can help to simplify it by removing the '-ror-data_schema_v2.json' tail).

<h4>Routine use</h4>

The system is designed to be as flexible as possible, but has reasonable defaults to make routine use very straightforward.

TO DO

<h4>Development environment</h4>

The system was developed on a Windows 11 machine, using Rust 1.80.1, Postgres 17, VS Code and 
DBeaver. Efforts have been / will be made to keep the system cross-platform, though this has not yet 
been tested.
