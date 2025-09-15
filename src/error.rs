//! # error
//!
//! Basic error struct for handling non-fatal errors. 


use std::error::Error;

/// Used to incur non-zero exit code while continuing to process data.
#[derive(Debug)]
pub struct NonFatalError(pub String);

impl std::fmt::Display for NonFatalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Non-fatal: {}", self.0)
    }
}

impl Error for NonFatalError {}
