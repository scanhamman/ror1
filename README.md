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
'type' in names becomes 'name_type'
'type' in external ids becomes 'id_type'.
'type' in links becomes 'link_type'
'type' in relationships becomes'rel_type'

The ror schema data represents the initial import into a staging schema. It is used as the basis of 
later processes in this system, but is retained so as to be available for other transformations by
other systems.

<h3>The src data schema</h3>

After initial import into the 'ror' schema, the data can be transformed into 
he data is processed to form a new set of tables, incorporating additional
information, e.g. summary records for each organisation, and using integer categories rather 
than string values for several of the data points. This data is stored in a different
DB schema, 'src'. This data is designed to be used as the basis for ad hoc SQL queries of the 
data, and for an organisation data system UI, allowing data display and editing. They are also
used as the basis of the summary statistics described below, and are designed to be the base data
when integrating ror data into other systems.

<h3>Summary data and the smm schema</h3>



<h3>Pre-requisites</h3>

1) The system assumes that any v2 ROR data file required has been downloaded from the Zenodo site and 
placed on the machine running the program, in a designated 'data folder'. It can be useful to simplify 
the name of this file (see Operation and arguments below).
2) The system requires a postgres database for holding the data. By default, this database is assumed 
to be named 'ror', but this can be changed in the configuration file (see below). 
3) <i>At the moment</i> - a Rust development environment is also required, as the system is most easily 
run from that environment. This means installing Rust and an IDE. VS Code is recommended as - like Rust 
itself - it is free of charge. A means of inspecting the Postgres database is also necessary - PgAdmin and / or DBeaver (Community edition) are free and very capable systems for this purpose.</i>

<h3>Operation and arguments</h3>

<i>Set-up</i>

environmental and command line variables are read, and database access
established.

<i>Environmental varables</i>


<i>Command line arguments</i>



<h4>Developoment environment</h4>

The system was developed on a Windows 11 machine, using Rust 1.80.1, Postgres 17, VS Code and 
DBeaver. Efforts have been / will be made to keep the system cross-platform, though this has not yet 
been tested.
