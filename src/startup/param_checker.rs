use clap::{command, Arg, ArgMatches};

#[derive(Debug)]
pub struct CliPars {
    pub data_folder: String,
    pub source_file: String,
    pub import_source: bool,
    pub process_source: bool,
}

pub fn fetch_valid_arguments() -> CliPars
{
    let parse_result = parse_args ();

    // guaranteed to unwrap OK as a default value set of "" 
    let folder = parse_result.get_one::<String>("data_folder").unwrap();

    // guaranteed to unwrap OK as a default value set of "" 
    let srce = parse_result.get_one::<String>("src_file").unwrap();

    let a_flag = parse_result.get_flag("a_flag");
    let s_flag = parse_result.get_flag("s_flag");
    let t_flag = parse_result.get_flag("t_flag");
    
    let mut import = true;
    let mut process = false;
    if a_flag == true  // 'a' (do all) flag set
    {
        process = true;  // import alrteasdy true
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
        import_source: import,
        process_source: process,
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
    .get_matches()

}




