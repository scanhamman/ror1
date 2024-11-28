use std::fmt;
use std::error::Error;

//#[derive(Debug)]
pub enum AppError {
    DeErr(dotenv::Error),
    SqErr(sqlx::Error),
    IoErr(std::io::Error),
    CfErr(CustomFileError),
}

impl std::error::Error for AppError {}

impl fmt::Display for AppError { // Error message for users.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            AppError::DeErr(ref err) => write!(f, "environment error: {}", err),
            AppError::SqErr(ref err) => write!(f, "sqlx error: {}", err),
            AppError::IoErr(ref err) => write!(f, "io error: {}", err),
            AppError::CfErr(ref err) => write!(f, "file error: {}", err),
        }
    }
}

impl std::fmt::Debug for AppError { // Error message for programmers.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{self}")?;
        if let Some(e) = self.source() { // <-- Use source() to retrive the root cause.
            writeln!(f, "\tCaused by: {e:?}")?;
        }
        Ok(())
    }
}

impl From<dotenv::Error> for AppError {
    fn from(err: dotenv::Error) -> AppError {
        AppError::DeErr(err)
    }
}

impl From<sqlx::Error> for AppError {
    fn from(err: sqlx::Error) -> AppError {
        AppError::SqErr(err)
    }
}

impl From<std::io::Error> for AppError {
    fn from(err: std::io::Error) -> AppError {
        AppError::IoErr(err)
    }
}

impl From<CustomFileError> for AppError {
    fn from(err: CustomFileError) -> AppError {
        AppError::CfErr(err)
    }
}



#[derive(Debug)]
pub struct CustomFileError {
    message: String,
}

impl CustomFileError {
    pub fn new(message: &str) -> CustomFileError {
        CustomFileError {
            message: message.to_string(),
        }
    }
}

impl fmt::Display for CustomFileError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for CustomFileError {}

