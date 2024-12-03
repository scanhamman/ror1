A small program to process the ROR organisation data, as made available in a JSON file 
download from ROR, and load that data into a series of tables in a Postgres database. 

There are various phases to the system's operation, though command line flags allow
some to be skipped when desired.

1) Setup - where environmental and command line variables are read, and database access
established.
2) Initial download - The data is read from the file and transformed to matching structs
with very little additional processing. These are then stored in the DB. The resulting tables, 
collected in a schema called 'ror', therefore mirror the source ROR data very closely.
3) Processing - The data is processed to form a new set of tables, incorporating additional
information, e.g. summary records for each organisation, and using integer categories rather 
than string values for several of the data points. This data is stored in a different
DB schema, 'src'. This data is designed to be used as the basis for ad hoc SQL queries of the 
data, and for an organisation data system UI, allowing data display and editing. They are also
used as the basis of the summary statistics described below, and are designed to be the base data
when integrating ror data into other systems.
4) Summarising - Key summary statistics about the processed data are derived and stored in a collection of 
permanent tables. There is one set of statistics per downloaded file. These tables can then be 
interrogated to examine the changing characteristics of the ror data over time.
5) Reporting - The key summary statistics of the data can also be reported (for any one downloaded file)
as both a JSON and a text file.


The program was built partly as a self-directed learning exercise to become more familiar with Rust.
It is therefore designed to illustrate various aspects of a 'real' data oriented system: 
database access, logging, use of environment variables, use of command line parameters, 
unit and integration tests and comprehensive error handling. 
It was intended as a self-directed learning exercise.