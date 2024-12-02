A small program to process the ROR organisation data, as made available in a JSON file 
download by ROR, and load that data into a series of tables in a Postgres database. 
 
The program is designed to illustrate various aspects of a 'real' data oriented systems 
in Rust, including database access, logging, use of environment variables, use of command 
line parameters, unit and integration tests and comprehensive error handling. 
It is intended as a self-directed learning exercise.

There are three phases to the system. 
1) Setup - where environmental and command line variables are read, and database access
established.
2) Initial download - The data is read from the file and transformed to matching structs
with very little additional processing. These are then stored in the DB. The resulting tables
therefore mirror the source ROR data very closely.
3) Processing - The data is processed to form a new set of tables, incorporating additional
information, e.g. summary records for each organisation. This data is stored in a different
DB schema. This data is designed to be used as the basis for an organisation data, system,
allowing its display, editing and incorporation into other systems.
