/***************************************************************************
 * Module uses clap crate to read command line arguments. These include 
 * possible A, S and T flags, and possible strings for the data folder and 
 * source file name. If no flags 'S' (= import data) is returned by default.
 * Folder and file names return an empty string ("") rather than null if not 
 * present. 
 ***************************************************************************/

use clap::{command, Arg, ArgMatches};

#[derive(Debug)]
pub struct CliPars {
    pub data_folder: String,
    pub source_file: String,
    pub data_date: String,
    pub import_source: bool,
    pub process_source: bool,
    pub create_context: bool,
}

pub fn fetch_valid_arguments() -> CliPars
{
    let parse_result = parse_args ();

    // these 3 parameters guaranteed to unwrap OK as all have a default value of "" 
    let folder = parse_result.get_one::<String>("data_folder").unwrap();
    let data_date = parse_result.get_one::<String>("data_date").unwrap();
    let srce = parse_result.get_one::<String>("src_file").unwrap();

    // flag values are false if not present, true if present.
    let a_flag = parse_result.get_flag("a_flag");
    let s_flag = parse_result.get_flag("s_flag");
    let t_flag = parse_result.get_flag("t_flag");
    let c_flag = parse_result.get_flag("c_flag");

    let mut import = true;
    let mut process = false;
    if a_flag == true  // 'a' (do all) flag set
    {
        process = true;  // import already true
    }
    else 
    {
        // none, one or both s and t flags set
        // if neither set, use initial default values,
        // else use values as provided

        if !(s_flag == false && t_flag == false) {
            import = s_flag;
            process = t_flag;
        }
    }

    CliPars {
        data_folder: folder.clone(),
        source_file: srce.clone(),
        data_date: data_date.clone(),
        import_source: import,
        process_source: process,
        create_context: c_flag,
    }

}


fn parse_args () -> ArgMatches {

    command!()
        .about("Imports data from ROR json file (v2) and imports it into a database")
        .arg(
            Arg::new("data_folder")
           .short('f')
           .long("folder")
           .visible_aliases(["data folder"])
           .help("A string with the data folder name (over-rides environment setting")
           .default_value("")
       )
        .arg(
             Arg::new("src_file")
            .short('s')
            .long("source")
            .visible_aliases(["source file"])
            .help("A string with the source file name (over-rides environment setting")
            .default_value("")
        )
        .arg(
            Arg::new("data_date")
           .short('d')
           .long("date")
           .required(false)
           .help("A string with a date in ISO format that gives the date of the data")
           .default_value("")
        )
        .arg(
            Arg::new("a_flag")
           .short('A')
           .long("A-flag")
           .required(false)
           .help("A flag signifying run the entire program")
           .action(clap::ArgAction::SetTrue)
         )
        .arg(
            Arg::new("s_flag")
           .short('S')
           .long("S-flag")
           .required(false)
           .help("A flag signifying import from source file to src tables only")
           .action(clap::ArgAction::SetTrue)
       )
        .arg(
             Arg::new("t_flag")
            .short('T')
            .long("T-flag")
            .required(false)
            .help("A flag signifying process source table data and analyse results")
            .action(clap::ArgAction::SetTrue)
        )
        .arg(
            Arg::new("c_flag")
           .short('C')
           .long("C-flag")
           .required(false)
           .help("A flag signifying that context tables need to be rebuilt")
           .action(clap::ArgAction::SetTrue)
       )
    .get_matches()

}




