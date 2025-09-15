//! # mrfy (murphy)
//!
//! Program to process machine readable files and extract negotiated price information.
//! 
//! Currently only supports Aetna Signature Administrators.

mod query;
mod asa;
mod error;

use clap::Parser;

/// Handle user input 
#[derive(Parser)]
pub struct Cli {
    /// The path to the query input file 
    pub input_path: std::path::PathBuf,
    /// The path to the datafile to process
    pub data_path: std::path::PathBuf,
    /// Optional buffer size in kb
    pub buff_size: Option<usize>,
}


fn main() -> Result<(), Box<dyn std::error::Error>> {

    let args = Cli::parse();

    // Use a default buffer size if none specified
    const DEFAULT_BUFF_SIZE: usize = 1024 * 1024 * 128; // 128 MiB
    let buff_size: usize = args.buff_size.unwrap_or(DEFAULT_BUFF_SIZE);

    let mut q = query::read_input(&args.input_path).unwrap();

    asa::run(&mut q, &args.data_path, buff_size, std::io::stdout())?;

    q.warn_not_recorded();

    Ok(())
}
