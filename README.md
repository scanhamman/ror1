A small program to process and summarise ROR organisation data, as made available by ROR, 
on Zenodo (see https://ror.readme.io/docs/data-dump). A new version of the data is posted 
on a roughly monthly basis. The program processes and retains a single version at a time, 
but retains summaries of the key features of each version. 
The system uses the version 2 schema files as input, and so covers data made available from
April 2024 onwards. It can handle versions 2.0 and 2.1, the latter in use from December 2024.

The system is written in Rust, uses a command line interface (CLI) for control, and 
requires a Postgres database to provide the back-end datastore. 

<i>N.B. At the moment, the program is not yet available as a stand alone .exe or .lib file, 
though it is hoped to create these soon. The current system therefore needs the source code 
to be downloaded from GitHub, and then run within a Rust development environment (see 
'Pre-requisites' below).</i>

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
and text files), and unit and integration tests.

<h3>The base ror data schema</h3>

When a ROR data file is imported it is first transformed into tables that almost exactly mirror 
the original structure of the json file. These tables are all grouped within a schema called 'ror'. 
Only one set of ror schema tables exists at any one time, relating to a specific ROR data version.
The import process recreates the tables each time.

The id used to identify each ror entry, in all tables, is the last 9 characters of the full ROR id, 
i.e. the ROR URL with the prefix "https://ror.org/" removed. It is clearer to use only the non 
constant part of the ROR URL rather than the full string, though that full string remains as a field 
in the core_data table. The core_data table contains only the id, full id (URL), status and year 
established. The other singleton data of the ror record, relating to date and schema of creation 
and last modification, are collected separately into an 'admin_data' table.

All other data potentially represents multiple entities for each organisation, even though at the 
moment (January 2025) all organisations only have a single location, and no organisation has any
associated domains. Field names are, in most cases, the same or have an obvious correspondence to 
the field names as listed in the ROR documentation.

The main exception to this is caused by the fact that 'type', whilst used in several places in the 
ROR definitions, is a reserved word in Rust (and many other languages), while TYPE can cause issues 
in Postgres in some contexts. For safety and future compatibility the following changes are made:
<ul>
<li>'type' in names becomes 'name_type'.</li>
<li>'type' in types becomes 'org_type'.</li>
<li>'type' in external ids becomes 'id_type'.</li>
<li>'type' in links becomes 'link_type'.</li>
<li>'type' in relationships becomes'rel_type'.</li>
</ul>

The ror schema data represents the initial import into a staging schema. It is used as the basis of 
later processes in this system, but is retained so as to be available for other transformations by
other systems, if required.

<h3>The src data schema</h3>

After initial import into the 'ror' schema, the data can be processed to form a new set of tables, 
within the 'src' schema. The changes are limited but include:

a) Replacement of the strings of categorised values by integers. The integers are as given by
lookup tables (set up within the 'lup' or lookup schema) which effectively provide enumerations 
of these categorised values, e.g. the organisation, name, link, external id and relationship types. 
This is designed to make any future data processing quicker and more flexible.

b) The expansion of the admin_data table, to include for each organisation the numbers of entities 
of each type it is linked with, e.g. how many names (of various types), links and external ids (of 
various types), relationships (of various types), locations, etc. are included in the ror record.
This is to make it easier both to use and display the information, to support some of the 
production of summary data, and to more easily ientify organisations that are missing certain types of data.

c) The addition of script codes to the name data. Though most of the the names listed (apart 
from acronyms) have language codes linked to them there is no explicit indication of the script being 
used. The great majority of the names are in latin but a substantial number use other script 
systems, such as Cyrilic, Greek, Arabic, Han, Hebrew or Gujarati. Full details are given by ISO 15924, 
which also provides the Unicode code pages on which each script can be found. Examining the Unicodes of 
the characters in the names allows the script to be readily identified, and this information is added 
to each name record, as being of potential value when displaying the data.

d) The simplification of a few field names to make them easier to use, e.g. country_subdivision_code 
becomes csubdiv_code, and continent_code becomes cont_code.
 
The src data is designed to be used as the basis for ad hoc SQL queries of the 
data, and for an organisation data system UI, allowing data display and editing. They are also
used as the basis of the summary statistics described below, and are designed to be the base data
when integrating ror data into other systems. Only one set of src data exists at any one time - the tables 
are recreated each time a version's data is transformed into them.

<h3>Summary data and the smm schema</h3>





<h3>Pre-requisites</h3>

1) The system assumes that any v2 ROR data file required has already been downloaded from the Zenodo site 
and placed on the machine running the program, in a designated 'data folder'. It can be useful to simplify 
the name of this file (see Operation and arguments below).<br>
2) The system requires a postgres database for holding the data. By default, this database is assumed 
to be named 'ror', but this can be changed in the configuration file (see below). The database must be
created prior to the intial run of the system. <br> 
3) <i>At the moment</i> - a Rust development environment is also required, as the system is most easily 
run from that environment. This means installing Rust and an IDE. VS Code is recommended as - like Rust 
itself - it is free of charge. A means of inspecting the Postgres database is also necessary - PgAdmin 
and / or DBeaver (Community edition) are free and very capable systems for this purpose.</i>

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

The following are normally supplied by command line arguments, which will always over-write values in the configuration file. 
During testing and development however, against a fixed source file, it can be much easier to include them in the .env file instead.
<ul>
<li>The name of the souce JSON file, as 'src_file_name'.</li>
<li>The name of the output file, as 'output_file_name'. If missing the system will construct a name based on the source file and date-time.</li>
<li>The version of the file to be imported, as 'data_version'. A string, in a semantic versioning format, e.g. '1.45.1', '1.57'
<li>The date of the file to be imported, as 'data_date='. This should be in YYYY-mm_DD ISO format.
</ul>

In future versions this configuration file will need to be installed in an OS-specific configuration folder.

<i>Set-up and initial run</i>

The system needs to have the lookup (filled with their data) and summary tables (empty) in place before an import and further 
processing can take place. It should therefore first be run with an '-i' command line argument, which ensures that the lup schema 
tables are created and filled, and the smm tables are created. In normal circumstances this shopuld only need doing once. 
In the context of a rust development environment, using cargo, the program's arguments must be distinguished from cargo's own 
arguments by a double hyphen. The command is therefore:<br>
<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<b>cargo run -- -i</b>

<i>Command line arguments</i>

The folowing command line arguments are available:

<i><b>-s</b></i>&nbsp;&nbsp;&nbsp;&nbsp;[or -S, -source]. Followed by a double quoted string representing the source file name, 
including the '.json' extension.

<i><b>-f</b></i>&nbsp;&nbsp;&nbsp;&nbsp;[or -F, -folder]. Followed by a double quoted string representing the full path to the source data folder.

<i><b>-v</b></i>&nbsp;&nbsp;&nbsp;&nbsp;[or -data_version]. Followed by a double quoted string representing a version number, e.g. "1.52".

<i><b>-d</b></i>&nbsp;&nbsp;&nbsp;&nbsp;[or -D, -date]. Followed by a double quoted string in ISO YYYY-mm-DD format, representing the date of the data.

<i><b>-r</b></i>&nbsp;&nbsp;&nbsp;&nbsp;[or -R, -import]. A flag that causes import of the specified source data to ror schema tables. The source file, 
data version and data date must be specified. 

<b><i>Note that if the source file name follows a simple convention (described below) it is possible for the system to derive the version and date from 
the name. The file as named by ROR follows this convention, so in most cases, unless the file is renamed in an entirely different way, it is not necessary 
to specify the data'a version and date separately.</b></i>

<i><b>-p</b></i>&nbsp;&nbsp;&nbsp;&nbsp;[or -P, -process]. A flag that causes processing and summarising of the data in the ror schema tables to the src and smm schema tables. 

<i><b>-x</b></i>&nbsp;&nbsp;&nbsp;&nbsp;[or -X, -export]. A flag that causes production of a text file summarising the main features of the version currently held within the system. The name of the file is normally constructed from the version and the date-time of the run, but can be specified innthe configuration file, e.g. during testing.

<i><b>-a</b></i>&nbsp;&nbsp;&nbsp;&nbsp;[or -A, -all]. Equivalent to -r -p -x, i.e. run all main processes, in that order. The source file must be specified.

<i><b>-i</b></i>&nbsp;&nbsp;&nbsp;&nbsp;[or -I, -install].  Equivalent to -c -m, i.e. initialise permanent data tables.

<i><b>-c</b></i>&nbsp;&nbsp;&nbsp;&nbsp;[or -C, -context]. A flag that causes the re-establishment of the lookup tables. Useful after any revision of those tables or the data within them.

<i><b>-m</b></i>&nbsp;&nbsp;&nbsp;&nbsp;[or -M, -summsetup]. A flag that causes the re-establishment of the summary tables in the smm schema. NOTE - ANY EXISTING DATA IN THOSE TABLES WILL BE DESTROYED. It may therefore be necessayr to re-run against different source files if a series of data points over time needs to be re-established.

<i><b>-t</b></i>&nbsp;&nbsp;&nbsp;&nbsp;[or -T, -test]. A flag applied from within integration tests in the system, to suppress log creation and restrict spurious activity. It is not available to users.

<h4>File name convention and deriving version and data</h4>

If the file name starts with a 'v' followed by a semantic versioning string, followed by a space or a hyphen and then the date in ISO format, either with spaces or without, then (whatever any following text in the name) the system is able to extract the data date and version from the file name. It is then no longer necessary to provide the data version and date separately. 

File names such as <b>v1.58-2024-12-11-ror-data_schema_v2.json, v1.51-20240821.json, v1.48 20240620.json</b>, and <b>v1.47 2024-05-30.json</b> all follow the required pattern. The first is the form of the name supplied by ROR, so renaming of the file is not necessary (though it can help to simplify it!).

<h4>Routine use</h4>



<h4>Development environment</h4>

The system was developed on a Windows 11 machine, using Rust 1.80.1, Postgres 17, VS Code and 
DBeaver. Efforts have been / will be made to keep the system cross-platform, though this has not yet 
been tested.
