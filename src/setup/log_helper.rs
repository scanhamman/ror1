/***************************************************************************
 * Establishes the log for the programme's operation using log and log4rs, 
 * and includes various helper functions.
 * Once established the log file apperars to be accessible to any log
 * statement within the rest of the program.
 ***************************************************************************/

use chrono::Local;
use std::path::PathBuf;

use log::{info, LevelFilter};
use log4rs::{
    append::{
        console::{ConsoleAppender, Target},
        file::FileAppender,
    },
    config::{Appender, Config, Root},
    encode::pattern::PatternEncoder,
};


pub fn get_log_file_name(source_file_name : &String) -> String{
    
    // Called from within the setup module to get the log file name

    let datetime_string = Local::now().format("%m-%d-%H-%M-%S").to_string();
    let source_file = &source_file_name[..(source_file_name.len() - 5)];
    format!("Import - {} - from {}.log", datetime_string, source_file)
}

pub fn setup_log (log_file_path: &PathBuf) {
    
    // Called from within the setup module to establish the logger mechanism
    // Initially establish a pattern for each log line

    let log_pattern = "{d(%d/%m %H:%M:%S)}  {h({l})}  {({M}.{L}):>35.45}:  {m}\n";

    // Define a stderr logger, as one of the 'logging' sinks or 'appender's.

    let stderr = ConsoleAppender::builder().encoder(Box::new(PatternEncoder::new(log_pattern)))
        .target(Target::Stderr).build();

    // Define a second logging sink or 'appender' - to a log file (provided path will place it in the current data folder).

    let logfile = FileAppender::builder().encoder(Box::new(PatternEncoder::new(log_pattern)))
        .build(log_file_path).unwrap();

    // Configure and build log4rs instance, using the two appenders described above

    let config = Config::builder()
        .appender(Appender::builder()
                .build("logfile", Box::new(logfile)),)
        .appender(Appender::builder()
                .build("stderr", Box::new(stderr)),)
        .build(Root::builder()
                .appender("logfile")
                .appender("stderr")
                .build(LevelFilter::Info),
        )
        .unwrap();

    let _handle = log4rs::init_config(config).unwrap();  
}


pub fn log_startup_params (folder_name: &String, source_file_name: &String, results_file_name: &String, 
    import_source: bool, process_source: bool ) {
    
    // Called at the end of set up to record the input parameters

    info!("PROGRAM START");
    info!("");
    info!("************************************");
    info!("");
    info!("folder_name: {}", folder_name);
    info!("source_file_name: {}", source_file_name);
    info!("results_file_name: {}", results_file_name);
    info!("import_source: {}", import_source);
    info!("process_source: {}", process_source);
    info!("");
    info!("************************************");
    info!("");
}