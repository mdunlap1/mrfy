//! # query
//!
//! Handles parsing user input file.
//! Converts input to a Query struct which contains a Vec of Code structs and a Vec of Provider
//! structs.

use anyhow::{Context, Result};
use std::io::BufReader;
use std::io::BufRead;
use std::fs::File;
use std::collections::{HashSet, HashMap};



/// Used to hold billing code information specified in the query.
#[derive(Clone, Debug, PartialEq)]
pub struct Code {
    pub code_type: String,
    pub value:     String,
    pub seen:      bool,
    pub recorded:  bool,
}
impl Code {
    /// Creates a new Code struct with cloned code type and code value stored in its fields. 
    /// Sets boolean fields to false. 
    pub fn new(c_type: &String, c_value: &String) -> Self {
        Self {
            code_type: c_type.clone(), 
            value:     c_value.clone(),
            seen:      false,
            recorded:  false,
        }
    }

    /// Prints basic information about code to stderr. Used to warn about codes
    /// that didn't have matches in datafile.
    pub fn eprint_no_match(&self) {
        eprintln!("\nWARNING: No match found for Code\n  Type: {}   Value: {}", self.code_type, self.value);
    }
}


/// Used to hold medical provider npi specified in the input.
#[derive(Clone, Debug, PartialEq)]
pub struct Provider {
    pub npi:       u64, 
    pub group_id:  Option<u64>, 
    pub tin_type:  Option<String>,
    pub tin_value: Option<String>,
    pub needs_tin: bool,
    pub needs_gid: bool,
    pub recorded:  bool,
}
impl Provider {
    /// Creates a new Provider struct with the given npi and all Option values None and all boolean
    /// values false.
    pub fn new(npi_val: u64) -> Self {
        Self {
            npi: npi_val,
            group_id: None,
            tin_type: None,
            tin_value: None,
            needs_tin: false,
            needs_gid: false,
            recorded:  false,
        }
    }

    /// Used to warn user that there were no matches for the given npi, group id, tin type, tin
    /// value. Does so by printing warning to standard error.
    pub fn eprint_no_match(&self) {
        eprintln!("\nWARNING: \
        No match found for Provider\n  npi: {}\n  group_id: {:?}\n  tin_type: {:?}\n  tin_value: {:?}\n", 
        self.npi, self.group_id, self.tin_type, self.tin_value);
    }
}

/// Holds Vectors of Provider and Code structs to represent the user query.
#[derive(Clone, Debug, PartialEq)]
pub struct Query {
    pub providers: Vec<Provider>,
    pub codes    : Vec<Code>,
}
impl Query {
    /// Creates a new Query struct with codes and providers set to empty Vectors. 
    pub fn new() -> Self {
        Self { 
            codes: Vec::new(),
            providers: Vec::new(),
        }
    }

    /// Creates a HashSet of all NPIs in self.providers
    pub fn make_npi_set(&mut self) -> HashSet<u64> {
        let mut npi_set = HashSet::new();
        for p in self.providers.iter() {
            npi_set.insert(p.npi);
        }

        npi_set
    }

    /// Creates a HashSet of all the codes the user specified (independent of code_type).
    /// NOTE:Converts the codes to ascii uppercase
    ///      ASSERTION: The codes will all be valid in ascii
    /// Returns a HashSet of codes from codes Vec where the code is converted to ASCII uppercase
    /// Purpose is to expedite processing of in_network objects.
    pub fn make_code_set(&mut self) -> HashSet<String> {
        let mut codeset = HashSet::new();
        for c in self.codes.iter() {
            let code = c.value.clone().to_ascii_uppercase();
            codeset.insert(code);

        }

        codeset
    }

    // Returns a HashMap with
    //           Key (u64): provider_ref (aka group_id)
    // Value (Vec<String>): "{npi},{tin_type},{tin_value}"
    //
    /// Returns a HashMap that maps provider references (group ids) to corresponding
    /// tuples of npi, tin_type and tin_value. These are stored as strings and intended for 
    /// use in writing output in csv format. 
    /// Purpose is to expedite processing for in_network objects and writting of records
    /// (We only need to walk the providers Vec when we mark which ones had a record.)
    pub fn make_ref_map(&mut self) -> HashMap<u64, Vec<String>> {
        let mut ref_map = HashMap::new();
        for p in self.providers.iter() {
            match p {
                Provider{ npi, group_id: Some(group_id), tin_value: Some(tin_value), tin_type: Some(tin_type),..} => {
                    let val = format!("{},{},{}", npi, tin_type, tin_value);
                    if !ref_map.contains_key(group_id) {
                        let mut v = Vec::new();
                        v.push(val);
                        ref_map.insert(*group_id, v);
                    }
                    else {
                        if let Some(v) = ref_map.get_mut(group_id) {
                            v.push(val);
                        }
                    }
                }
                _ => {} // Skip Providers with missing data
            }
        }
        
        ref_map
    }

    /// Sets recorded to true for all Provider stucts with matching gid in Query.providers
    /// Intended to be used to track parts of query that had a match in the dataset. 
    pub fn log_ref(&mut self, gid: u64) {

        for provider in self.providers.iter_mut() {
            match provider.group_id {
                Some(id) if id == gid => {
                    provider.recorded = true;
                }
                _ => {}
            }
        }

    }

    /// Used to log the codes from our query that had a record in the data.
    /// If code type is '*' will match on all code types.
    /// Matches are done assuming code and code type are ascii.
    pub fn log_code(&mut self, c: &String, c_type: &String) {

        for code in self.codes.iter_mut() {
            match code {
                Code { value, code_type,.. } => {
                    /*
                    if value == c && code_type == c_type {
                        code.recorded = true;
                    }
                    else if value == c && code_type == "*" {
                        code.recorded = true;
                    }
                    */
                    if value.eq_ignore_ascii_case(c) && code_type.eq_ignore_ascii_case(c_type) {
                        code.recorded = true;
                    }
                    else if value.eq_ignore_ascii_case(c) && code_type == "*" {
                        code.recorded = true;
                    }
                    else if value == "*" && code_type == "*" {
                        code.recorded = true;
                    }
                    else if value == "*" && code_type.eq_ignore_ascii_case(c_type) {
                        code.recorded = true;
                    }
                }
            }
        }

    }

    /// Warns the users if any of the Provider structs in self.proviers has recorded set to false.
    /// Also warns the user if any npi in the input query had absolutely no matches in the dataset.
    /// Warns the user if any of the codes in self.codes has no matches in the data set. 
    pub fn warn_not_recorded(&mut self) {
        // Warn about npi, g_id, tin information that didn't have matches.
        // Make HashMap to track if an npi has absolutely no matches and warn about that as well.
        let occur_idx = 0;
        let recor_idx = 1;
        let mut npi_map = HashMap::new();
        for p in self.providers.iter() {
            if !npi_map.contains_key(&p.npi) {
                let mut v = Vec::new();
                v.push(1); // Occurences at idx 0
                v.push(0); // Recorded at idx 1
                npi_map.insert(p.npi, v);
            }

            if let Some(v) = npi_map.get_mut(&p.npi) {
                v[occur_idx] += 1;
                if p.recorded {
                    v[recor_idx] += 1;
                }
                else {
                    p.eprint_no_match();
                }
            }
        }

        for npi in npi_map.keys() {
            if let Some(v) = npi_map.get(&npi) {
                if v[recor_idx] == 0 {
                    eprintln!("WARNING: Zero matches found for npi: {}", &npi);
                }
            }
        }

        // Warn about codes without matches.
        for c in self.codes.iter() {
            if !c.recorded {
                c.eprint_no_match();
            }
        }
    }

    /// Returns true if at least one provider in self.providers has a group_id
    /// Otherwise returns false. Can be used to exit early.
    pub fn stat_providers(&self) -> bool {
        let mut stat: bool = false;

        for p in self.providers.iter() {
            if p.group_id.is_some() {
                stat = true;
                break;
            }
        }

        stat

    }
 
}



/// Reads the user supplied input and returns the necessary data structures to process the query
pub fn read_input(input_path: &std::path::PathBuf) -> Result<Query, Box<dyn std::error::Error>> {
    enum State {
        BillingCode,
        Npi,
        Undefined,
    }

    let mut state = State::Undefined;
    let mut c_type: Option<String> = None; 

    // To hold the data for the query
    let mut query = Query::new();

    let f = File::open(&input_path).with_context(
        || format!("could not read file'{}'", input_path.display()))?;
    let f = BufReader::new(f);

    for line in f.lines() {

        let line = line?;

        // Skip empty lines
        if line.len() == 0 {
            continue;
        }

        // Process npi or billing codes based on State
        if line.chars().nth(0) == Some(' ') { 
            let line = line.trim();
            match state {
                State::BillingCode => {
                    //let c = Code::new(c_type.as_ref().unwrap(), (*line).to_string());
                    let c = Code::new(c_type.as_ref().unwrap(), &line.to_string());
                    query.codes.push(c);
                }
                State::Npi => {
                    let npi_val: u64 = line.trim().parse().expect("Error: Failed to parse npi");
                    let p = Provider::new(npi_val);
                    query.providers.push(p);
                }
                State::Undefined => {
                    panic!("Input file not formatted correctly, codes came before either billing type or npi specifier");
                }
            }
                     
        }
        // Change state for new billing code type or NPI mode
        else {
            let line = line.trim();
            if line == "npi" {
                state = State::Npi;
            }
            else {
                c_type = Some(String::from(line.trim()));
                state = State::BillingCode;
            }

        }
        
    }

    Ok(query) 

}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_input_read() {
        // Tests a non-defective basic input
        let path_str = "testfiles/input_testfiles/query_basic_input";

        let filepath: std::path::PathBuf = std::path::PathBuf::from(path_str);
            
        let result = read_input(&filepath);

        let mut q = Query::new();
        let p = Provider::new(1234567);
        q.providers.push(p);

        let c1 = Code::new(&String::from("cpt"), &String::from("99995"));
        let c2 = Code::new(&String::from("cpt"), &String::from("0001"));
        let c3 = Code::new(&String::from("Efile"), &String::from("123"));
        q.codes.push(c1);
        q.codes.push(c2);
        q.codes.push(c3);

        let q_from_file = result.unwrap();

        assert_eq!(q_from_file, q);
            
    }

    #[test]
    #[should_panic(expected = "Input file not formatted correctly, codes came before either billing type or npi specifier")]
    fn test_input_file_codes_ambiguous() {
        // Tests a case where the user forgot to specifc code type for NPIs
        // Note: The user might forget specification in a way that won't be caught.
        // For example:
        //
        //     CPT
        //         12335
        //
        //         9999912
        //
        // Where the second code should have been of a different type
        let path_str = "testfiles/input_testfiles/query_npi_not_specified";
        let filepath: std::path::PathBuf = std::path::PathBuf::from(path_str);
        let _ = read_input(&filepath);
    }

}


