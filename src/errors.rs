use std::fmt;
use std::error::Error;

//#[derive(Debug)]
pub enum AppError {
    DeErr(dotenv::Error),
    SqErr(sqlx::Error),
    IoErr(std::io::Error),
    SdErr(serde_json::Error),
    LgErr(log::SetLoggerError),
    CsErr(CustomError),
}

impl std::error::Error for AppError {}

impl fmt::Display for AppError { // Error message for users.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            AppError::DeErr(ref err) => write!(f, "environment error: {}", err),
            AppError::SqErr(ref err) => write!(f, "sqlx error: {}", err),
            AppError::IoErr(ref err) => write!(f, "io error: {}", err),
            AppError::SdErr(ref err) => write!(f, "serde json error: {}", err),
            AppError::LgErr(ref err) => write!(f, "log set config error: {}", err),
            AppError::CsErr(ref err) => write!(f, "file error: {}", err),
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

impl From<serde_json::Error> for AppError {
    fn from(err: serde_json::Error) -> AppError {
        AppError::SdErr(err)
    }
}

impl From<log::SetLoggerError> for AppError {
    fn from(err: log::SetLoggerError) -> AppError {
        AppError::LgErr(err)
    }
}

impl From<CustomError> for AppError {
    fn from(err: CustomError) -> AppError {
        AppError::CsErr(err)
    }
}


#[derive(Debug)]
pub struct CustomError {
    message: String,
}

impl CustomError {
    pub fn new(message: &str) -> CustomError {
        CustomError {
            message: message.to_string(),
        }
    }
}

impl fmt::Display for CustomError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for CustomError {}

