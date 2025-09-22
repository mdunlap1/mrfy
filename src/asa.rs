//! # asa
//!
//! Stream parses gz compressed JSON mrf file for Aetna Signature Administrators.
//!
//! Prints data that matches query to stdout. (Might allow user choice in future.)
//!
//! Will report unsupported keys encountered in JSON file. 
//!
//! Will return Error to main upon any and all fatal errors and in the event that a user
//! specified npi is in the dataset but missing the group id. 
//! (Might remove all non-fatal errors in the future.)  
//!
//! The program assumes the following basic format of the JSON
//! ```
//! {
//!    "reporting_entity_name": "Aetna Signature Administrators",
//!    "reporting_entity_type": "Third Party Vendor",
//!    "last_updated_on":"2025-04-05",
//!    "version":"1.3.1",
//!    "provider_references":[
//!        {"provider_group_id":1,
//!         "provider_groups":[
//!             {"npi":[],"tin":{"type":"ein","value":""}}
//!         ]
//!        }
//!    ],
//!    "in_network":[
//!        {"negotiation_arrangement":"alpha",
//!         "name":"Item 1",
//!         "billing_code_type":"Type 1",
//!         "billing_code_type_version":"2022",
//!         "billing_code":"Code 1",
//!         "description":"Item 1",
//!         "negotiated_rates":[
//!             {
//!                 "provider_references":[1,2,3],
//!                 "negotiated_prices":[
//!                     {"negotiated_type":"neg type 1",
//!                     "negotiated_rate":9.99,
//!                      "expiration_date":"9999-12-31",
//!                      "service_code":["A", "B", "C"],
//!                      "billing_class":"class 1",
//!                      "billing_code_modifier": "Mod"}
//!                 ]
//!             }
//!        ]
//!      }
//!    ]
//!}
//!```
//!
//! Key ordering is *not* assumed. Unsupported keys have handling for them.



#![allow(non_camel_case_types)] // TODO remove when done
use crate::query::{Query, Provider};
use crate::error::{NonFatalError}; // TODO remove non-fatal errors?

use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::collections::{HashSet, HashMap};

use std::cell::RefCell; 

use flate2::read::GzDecoder;
use json_event_parser::{ReaderJsonParser, JsonEvent};

use indicatif::{ProgressBar};

// Used to track keys in the JSON that we didn't expect
// User will not be told if the key happens in more than one object type.
// The notification will happen only where the key is first discovered. 
thread_local! {
    static UNSUPPORTED_KEYS: RefCell<HashSet<String>> = RefCell::new(HashSet::new());
}

#[derive(Debug)]
struct Meta {
    reporting_entity_name: Option<String>,
    reporting_entity_type: Option<String>,
    last_updated_on: Option<String>,
    version: Option<String>,
    count: u8,
    num_fields: u8,
}

/// Tracks the JSON file metadata
impl Meta {
    /// Creates a Meta struct 
    fn new() -> Self {
        Self {
            reporting_entity_name: None,
            reporting_entity_type: None,
            last_updated_on: None,
            version: None,
            count: 0,
            num_fields: 4, // This should match the number of Option<String> fields
        }
    }

    /// Displays the JSON file metadata by printing to stderr
    fn e_print(&mut self) {
        if let Some(s) = &self.reporting_entity_name {
            eprintln!("reporting_entity_name: {}", s);
        }
        if let Some(s) = &self.reporting_entity_type {
            eprintln!("reporting_entity_type: {}", s); 
        }
        if let Some(s) = &self.last_updated_on {
            eprintln!("last_updated_on: {}", s);
        }
        if let Some(s) = &self.version {
            eprintln!("version: {}", s);
        }
    }


    /// Gets the metadata value that corresponds to key and adds it to the
    /// appropriate field in the Meta struct. 
    /// Panics if given a bad key 
    /// Prints to stderr when all fields have been added.
    fn add(&mut self, key: &str, value: &str) -> Result<(), Box<dyn std::error::Error>> {

        // Use the key to store the value in the Meta struct
        // Panic if the key isn't supported
        
        if key == "reporting_entity_name" {
            self.reporting_entity_name = Some(String::from(value));
            self.count += 1;
        }
        else if key == "reporting_entity_type" {
            self.reporting_entity_type = Some(String::from(value));
            self.count += 1;
        }
        else if key == "last_updated_on" {
            self.last_updated_on = Some(String::from(value));
            self.count += 1;
        }
        else if key == "version" {
            self.version = Some(String::from(value));
            self.count += 1;
        }
        else {
            panic!("Assertion broken");
        }

        if self.count == self.num_fields {
            self.e_print();
        }

        Ok(())

    } // End add for Meta

} // End impl for Meta

/// Holds information for a negotiated price
// TODO: add billing_code_modifier support
#[derive(Debug,PartialEq,Clone)]
struct Price {
    negotiated_type: String,
    negotiated_rate: String,
    expiration_date: String,
    service_code: String,
    billing_class: String,
    billing_code_modifier: String,
}
impl Price {
    /// Creates a Price struct
    fn new () -> Self {
        Self {
            negotiated_type: String::from(""),
            negotiated_rate: String::from(""),
            expiration_date: String::from(""),
            service_code: String::from(""),
            billing_class: String::from(""),
            billing_code_modifier: String::from(""),
        }
    }

    /// Returns a new Price struct where all fields are set to the String "null".
    fn new_null() -> Self {
        Self {
            negotiated_type: String::from("null"),
            negotiated_rate: String::from("null"),
            expiration_date: String::from("null"),
            service_code: String::from("null"),
            billing_class: String::from("null"),
            billing_code_modifier: String::from("null"),
        }
    }

    /// Calls clear on all the fields in the Price struct.
    fn clear_fields(&mut self) {
        self.negotiated_type.clear();
        self.negotiated_rate.clear();
        self.expiration_date.clear();
        self.service_code.clear();
        self.billing_class.clear();
        self.billing_code_modifier.clear();
    }

    /// Fills empty values in a Price struct with "null"
    fn push_defaults(&mut self) {
        let default = "null";
        if self.negotiated_type == "" {
            self.negotiated_type.push_str(default);
        }
        if self.negotiated_rate == "" {
            self.negotiated_rate.push_str(default);
        }
        if self.expiration_date == "" {
            self.expiration_date.push_str(default);
        }
        if self.service_code == "" {
            self.service_code.push_str(default);
        }
        if self.billing_class == "" {
            self.billing_class.push_str(default);
        }
        if self.billing_code_modifier == "" {
            self.billing_code_modifier.push_str(default);
        }

    }

    /// Prints out the fields of a Price struct to out.
    /// Assumes that the format is consistend with print_header.
    fn print_out(&self, out: &mut impl Write) -> Result<(), std::io::Error> {
        write!(out, "{}",self.negotiated_type)?;
        write!(out, ",")?;
        write!(out, "{}",self.negotiated_rate)?;
        write!(out, ",")?;
        write!(out, "{}",self.expiration_date)?;
        write!(out, ",")?;
        write!(out, "{}",self.service_code)?;
        write!(out, ",")?;
        write!(out, "{}",self.billing_class)?;
        write!(out, ",")?;
        write!(out, "{}",self.billing_code_modifier)?;
        Ok(())

    }

}

/// Holds negotiated price information and providers the associated provider references for which
/// those prices are valid. Progam will filter provider_refercnes to only those that are relevant. 
#[derive(Debug,Clone,PartialEq)]
struct Rate {
    provider_references: Vec<u64>,
    negotiated_prices: Vec<Price>, 
}
impl Rate {
    /// Creates a new Rate struct.
    fn new() -> Self {
        Self {
            provider_references: Vec::new(),
            negotiated_prices: Vec::new(),
        }
    }
    /// Calls clear on the fields in the Rate struct.
    fn clear_fields(&mut self) {
        self.provider_references.clear();
        self.negotiated_prices.clear();
    }
}

/// Used to hold the basic in-network billing information and a reference to negotiated-rates
#[derive(Debug)]
struct Network {
    negotiation_arrangement: String,
    name: String,
    billing_code_type: String,
    billing_code_type_version: String,
    billing_code: String,
    description: String,
    negotiated_rates: Option<Vec<Rate>>,
}

impl Network {
    /// Creates a new Network struct
    fn new() -> Self {
        Self {
            negotiation_arrangement: String::from(""),
            name: String::from(""),
            billing_code_type: String::from(""),
            billing_code_type_version: String::from(""),
            billing_code: String::from(""),
            description: String::from(""),
            negotiated_rates: None ,
        }
    }

    /// Calls clear on all String fields in the Network struct and sets negotiated_rates to None
    fn clear_entries(&mut self) {
        self.negotiation_arrangement.clear();
        self.name.clear();
        self.billing_code_type.clear();
        self.billing_code_type_version.clear();
        self.billing_code.clear();
        self.description.clear();
        self.negotiated_rates = None;
    }

    /// Sets all fields in Network struct to String: "null" except billing_code and
    /// negotiated_rates.
    /// NOTE: billing_code and negotiated_rates are NOT optional
    ///       and do NOT get default values
    /// Used to accomodate missing or null values 
    fn push_defaults(&mut self) {
                
        let default = "null";

        if self.negotiation_arrangement == "" {
            self.negotiation_arrangement.push_str(default);
        }
        if self.name == "" {
            self.name.push_str(default);
        }
        if self.billing_code_type == "" {
            self.billing_code_type.push_str(default);
        }
        if self.billing_code_type_version == "" {
            self.billing_code_type_version.push_str(default);
        }
        if self.description == "" {
            self.description.push_str(default);
        }
    }

    /// Prints the fields of Network struct to out.
    /// Assumes consistency with print_header.
    fn print_out(&self, out: &mut impl Write) -> Result<(), std::io::Error> {
        write!(out, "{}",self.negotiation_arrangement)?;
        write!(out, ",")?;
        write!(out, "{}",self.name)?;
        write!(out, ",")?;
        write!(out, "{}",self.billing_code_type)?;
        write!(out, ",")?;
        write!(out, "{}",self.billing_code_type_version)?;
        write!(out, ",")?;
        write!(out, "{}",self.billing_code)?;
        write!(out, ",")?;
        write!(out, "{}",self.description)?;
        Ok(())
    }
}


/// Prints the header for records to out.
fn print_header(out: &mut impl Write) -> Result< (), std::io::Error> {

    // From ref map
    write!(out, "npi,tin_type,tin_value")?; // From ref_map
    write!(out, ",")?;

    write!(out, "group_id")?; // Include the provider reference (group id)
    write!(out, ",")?;

    // From Network struct
    write!(out, "negotiation_arrangement,name,billing_code_type,billing_code_type_version,billing_code,description")?;
    write!(out, ",")?;

    // From Price stuct
    write!(out, "negotiated_type,negotiated_rate,expiration_date,service_code,billing_class,billing_code_modifier")?;
    write!(out, "\n")?;
    out.flush()?;

    Ok(())
}

/// Used to write matching data records to out.
/// Assumes consistency with print_header.
/// Uses print_out from Network and Price implementations.
/// Includes information from ref_map such as npi, group id, tin type and tin value. 
/// Calls log_ref and log_code from Query implementation.
fn print_record(network: &Network, 
                query: &mut Query,
                ref_map: &HashMap<u64, Vec<String>>,
                out: &mut impl Write,
                ) -> Result<(), Box<dyn std::error::Error>> {
    
    // For each rate in network.rates
    //     For each ref in rate.refs
    //        log refs in providers
    //        for each price in rate.prices
    //            print ref_map[ref]
    //            print network
    //            print price
    // log code in codes if code and code type case insensitive match
    // OR if codes case insensitive match and code type in struct in '*'
    //
    // EXIT

    let neg_rates = network.negotiated_rates.as_ref().unwrap();

    for rate in neg_rates.iter() {
        for reference in rate.provider_references.iter() {
            query.log_ref(*reference);
            for prov in ref_map.get(reference).unwrap().iter(){
                for price in rate.negotiated_prices.iter() {
                    write!(out, "{}", prov)?;
                    write!(out, ",")?;
                    write!(out, "{}", reference)?;
                    write!(out, ",")?;
                    network.print_out(out)?;
                    write!(out, ",")?;
                    price.print_out(out)?;
                    write!(out, "\n")?;

                }
            }
        }
    }

    out.flush()?;

    query.log_code(&network.billing_code, &network.billing_code_type);

    Ok(())
}

/// Handles data in pegotiated_prices array
/// Returns Vec<Price> with either the data OR if no data found Vec<Price> containing
/// a sinlge Price struct with all default values ("null").
/// WARNING: Doesn't handle dupe keys at all
/// NOTE: Uses ' ' delimited string for service codes
fn process_negotiated_prices<R: Read>(parser: &mut ReaderJsonParser<R>,
                                     ) -> Result< Vec<Price>, Box<dyn std::error::Error> > {

    // Used to handle price object keys 
    #[derive(PartialEq)]
    enum State {
        negotiated_type,
        negotiated_rate,
        expiration_date,
        service_code,
        billing_class,
        billing_code_modifier,
        undefined,
    }

    let mut state = State::undefined;
    let mut sq = 0;
    let mut cb = 0;

    let mut prices: Vec<Price> = Vec::new();
    let mut price = Price::new();

    loop {
        let event = {parser.parse_next()?};
        match event {
            JsonEvent::StartObject => {
                cb += 1;
            }
            JsonEvent::EndObject => {
                cb -= 1;
                if cb == 0 {
                    price.push_defaults();
                    prices.push(price.clone());
                    price.clear_fields();
                }
            }
            JsonEvent::StartArray => {
                sq += 1;
            }
            JsonEvent::EndArray => {
                sq -= 1;
                if sq == 0 {
                    break;
                }
            }
            JsonEvent::ObjectKey(key) => {
                if key == "negotiated_type" {
                    state = State::negotiated_type;
                }
                else if key == "negotiated_rate" {
                    state = State::negotiated_rate;
                }
                else if key == "expiration_date" {
                    state = State::expiration_date;
                }
                else if key == "service_code" {
                    state = State::service_code;
                }
                else if key == "billing_class" {
                    state = State::billing_class;
                }
                else if key == "billing_code_modifier" {
                    state = State::billing_code_modifier;
                }
                else {
                    UNSUPPORTED_KEYS.with(|set| {
                        if !set.borrow().contains(key.as_ref()) {
                            eprintln!("Unsupported key {} found in Price", key.as_ref());
                        }
                        set.borrow_mut().insert(String::from(key.as_ref()));
                    });
                    bypass_key(parser)?;
                }
            } // End ObjectKeys

            // TODO make decision to handle duplicate keys here 
            JsonEvent::String(s) => {
                if state == State::negotiated_type {
                    price.negotiated_type.push_str(s.as_ref());
                }
                // negotiated_rate is a Number, we will leave this for edge cases
                // in inconsistently formatted data though
                else if state == State::negotiated_rate {
                    price.negotiated_rate.push_str(s.as_ref());
                }
                else if state == State::expiration_date {
                    price.expiration_date.push_str(s.as_ref());
                }
                else if state == State::service_code {
                    price.service_code.push_str(s.as_ref());
                    price.service_code.push_str(" ");
                }
                else if state == State::billing_class {
                    price.billing_class.push_str(s.as_ref());
                }
                else if state == State::billing_code_modifier {
                    price.billing_code_modifier.push_str(s.as_ref());
                }
                else if state == State::undefined {
                    panic!("Unsupported key encountered in asa::process_negotiated_prices");
                }
            } // End String

            // ASSERTION: this will always be the price
            JsonEvent::Number(num) => {
                if state != State::negotiated_rate {
                    // TODO error? Warn? ignore ?
                }
                price.negotiated_rate.push_str(num.as_ref());
            }

            _ => {
            }

        } // End Match

    } // End loop

    // If no prices were found we fill in with "null" default price
    if prices.len() == 0 {
        prices.push(Price::new_null());
    }

    Ok(prices)


}


/// Processes the negotiated_rates array in the objects found in the in_network array. 
/// Uses a helper for negotiated_prices array.
/// If no relevant data (matching query) is found, returns Ok(None)
fn process_negotiated_rates<R: Read>(parser: &mut ReaderJsonParser<R>,
                                     ref_map: &HashMap<u64, Vec<String>>,
                                     ) -> Result< Option<Vec<Rate>>, Box<dyn std::error::Error> > {


    let mut rates: Vec<Rate> = Vec::new();
    let mut rate: Rate = Rate::new(); 

    let mut cb = 0;
    let mut sq = 0;
    
    loop {
        let event = {parser.parse_next()?};
        match event {
            JsonEvent::StartObject => {
                cb += 1;
            }
            JsonEvent::EndObject => {
                cb -= 1;
                if cb == 0 {
                    if rate.provider_references.len() != 0 {
                        rates.push(rate.clone());
                    }
                    rate.clear_fields();
                }
            }
            JsonEvent::StartArray => {
                sq += 1;
            }
            JsonEvent::EndArray => {
                sq -= 1;
                if sq == 0 {
                    break;
                }
                // ASSERTION this will always be at end of provider_references array
                else {
                    if rate.provider_references.len() == 0 {
                        ff_to_next_obj(parser, &mut cb, &mut sq)?;
                    }
                }
            }
            JsonEvent::ObjectKey(key) => {
                if key == "provider_references" {
                    continue; // TODO Should I use an enum here too? 
                }
                else if key == "negotiated_prices" {
                    let prices = {process_negotiated_prices(parser)?};
                    rate.negotiated_prices = prices;
                }
                else {
                    UNSUPPORTED_KEYS.with(|set| {
                        if !set.borrow().contains(key.as_ref()) {
                            eprintln!("Unsupported key {} found in \
                                      negotiated_rates", key.as_ref());
                        }
                        set.borrow_mut().insert(String::from(key.as_ref()));
                    });
                    bypass_key(parser)?;
                }
            }
            JsonEvent::Number(num) => {
                let ref_num: u64 = num.as_ref().parse().expect("Failed to parse provider reference");
                if ref_map.contains_key(&ref_num) {
                    rate.provider_references.push(ref_num);
                }
            }

            JsonEvent::Eof => {
                panic!("FATAL ERROR: Eof encountered in asa::process_negotiated_rates");
            }
            
            _ => {}

            
        } // End of match

    } // End of loop

    if rates.len() == 0 {
        return Ok(None);
    }
    
    Ok(Some(rates))
}

/// Used to bypass unsupported keys. 
fn bypass_key<R: Read>(parser: &mut ReaderJsonParser<R>,
                      ) -> Result<(), Box<dyn std::error::Error>> {

    let event = {parser.parse_next()?};

    match event {
        JsonEvent::StartObject => {
            let mut cb = 1;
            let mut sq = 0;
            ff_to_next_obj(parser, &mut cb, &mut sq)?;
        }
        JsonEvent::StartArray => {
            skip_array(parser, 1)?;
        }
        JsonEvent::Eof => {
            panic!("FATAL ERROR: Eof found in bypass_key.");
        }
        JsonEvent::EndObject => {
        }
        JsonEvent::EndArray => {
            panic!("FATAL ERROR: Malformed JSON");
        }
        _ => {}
    }

    Ok(())
}

/// Used to skip objects that have been partially processed and found not to match query.
fn ff_to_next_obj<R: Read>(parser: &mut ReaderJsonParser<R>,
                           cb: &mut u64,
                           sq: &mut u64,
                          ) -> Result<(), Box<dyn std::error::Error>> {


    loop {
        let event = {parser.parse_next()?};
        match event {
            JsonEvent::StartObject => {
                *cb += 1;
            }
            JsonEvent::EndObject => {
                *cb -= 1;
                if *cb == 0 {
                    break;
                }
            }
            JsonEvent::StartArray => {
                *sq += 1;
            }
            JsonEvent::EndArray => {
                *sq -= 1;
            } 
            JsonEvent::Eof => {
                panic!("EOF encountered when trying to skip object!");
            }
            _ => {}
        }

    }

    Ok(())
}


/// Handles the data in the in_network array.
/// Uses helper functions process_negotiated_rates which then chains a call to
/// process_negotiated_prices.
fn process_in_network<R: Read>(parser: &mut ReaderJsonParser<R>,
                               query: &mut Query,
                               out: &mut impl Write,
                               ) -> Result<(), Box<dyn std::error::Error>> {

    // Make codeset hashset
    // Make reference hasmap (prov ref) -> Vec[ (npi,tintype,tinvalue) ]
    // ! Make sure we can do case insensitive checks wtih contains
    //   so maybe make these all lowercase then check against lowercase.
    //
    // Outer struct (name?? Network)
    //   negotiated_rates is Vec of Inner struct (name?? Rate)
    //
    //   Inner struct (name??) has Vec of negotiated price structs (Price?)

    // loop from start to end of array
    // "negotiated_rates" needs a helper
    // IF code not in code set fastforward to next object (HELPER FUNCTION)
    // (Make sure match is case insensitive.)
    // IF "negotiated_rates" helper returns None fastforward to next object
    // 
    // IF we hit end of object with cb (curly brace count) == 0, 
    // ASSERT this only happens if "negotiated_rates" returned Some and code matched.
    // Hence: Write results to stdout,
    //        Iterate through providers Vec and mark recorded for each group id as we write.
    //        Iterate trhough codes Vec and mark recorded is code and code type match OR code
    //        type is '*' in struct. 
    
    let mut obj_count: u64 = 0;
    const INCR: u64 = 100;
    const APPRX_TOTAL_OBJS: u64 = 148400;
    let _progress = ProgressBar::new(APPRX_TOTAL_OBJS);
    eprintln!("Progress bar based on estimate of {} total objects", APPRX_TOTAL_OBJS);
    eprintln!("Progress bar will update after every {} objects", INCR);
    

    let mut header_written: bool = false;

    let mut network = Network::new();

    // Used to keep track of keys that have String values we want to keep
    #[derive(PartialEq)]
    enum State {
        negotiation_arrangement,
        name,
        billing_code_type,
        billing_code_type_version,
        billing_code,
        description,
        undefined,
    }

    let mut state = State::undefined;

    let codeset = query.make_code_set();
    let ref_map = query.make_ref_map();

    let mut sq = 0;
    let mut cb = 0;

    loop {
        let event = {parser.parse_next()?};
        match event {
            JsonEvent::StartObject => {
                cb += 1;
            }
            JsonEvent::EndObject => {
                cb -= 1;
                // ASSERTION: We will reach this only when we have something to write.
                if cb == 0  && network.billing_code != "" && network.negotiated_rates.is_some() {
                    if header_written == false {
                        print_header(out)?;
                        header_written = true;
                    }

                    network.push_defaults();
                        
                    print_record(&network,
                                 query,
                                 &ref_map,
                                 out)?;
                    
                }
                // Clear for re-use
                network.clear_entries();
            }
            JsonEvent::StartArray => {
                sq += 1;
            }
            JsonEvent::EndArray => {
                sq -= 1;
                if sq == 0 {
                    break;
                }
            }
            JsonEvent::ObjectKey(key) => {
                if key == "negotiation_arrangement" {
                    state = State::negotiation_arrangement;
                }
                else if key == "name" {
                    state = State::name;
                }
                else if key == "billing_code_type" {
                    state = State::billing_code_type;
                }
                else if key == "billing_code_type_version" {
                    state = State::billing_code_type_version;
                }
                else if key == "billing_code" {
                    state = State::billing_code;
                }
                else if key == "description" {
                    state = State::description;
                }
                else if key == "negotiated_rates" {
                    // Reset state
                    state = State::undefined;
                    let rates = process_negotiated_rates(parser, &ref_map);
                    match rates {
                        Ok(Some(rates)) => {
                            network.negotiated_rates = Some(rates);
                        }
                        Ok(None) => {
                            network.clear_entries();
                            ff_to_next_obj(parser, &mut cb, &mut sq)?;
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }
                }
                else { 
                    UNSUPPORTED_KEYS.with(|set| {
                        if !set.borrow().contains(key.as_ref()) {
                            eprintln!("Unsupported key {} found in Network",
                                     key.as_ref());
                        }
                        set.borrow_mut().insert(String::from(key.as_ref()));
                    });
                    bypass_key(parser)?;
                }
            }
            JsonEvent::String(s) => {
               if state == State::negotiation_arrangement {
                   network.negotiation_arrangement.push_str(s.as_ref());
               }
               else if state == State::name {
                   network.name.push_str(s.as_ref());
               }
               else if state == State::billing_code_type {
                   network.billing_code_type.push_str(s.as_ref());
               }
               else if state == State::billing_code_type_version {
                   network.billing_code_type_version.push_str(s.as_ref());
               }
               else if state == State::billing_code {

                   network.billing_code.push_str(s.as_ref());

                   
                   obj_count += 1;
                   if obj_count % INCR == 1 {

                       #[cfg(not(test))] {
                           _progress.set_position(obj_count);
                       }
                   }
                   

                   network.billing_code.make_ascii_uppercase();

                   if !codeset.contains(&network.billing_code) &&
                      !codeset.contains("*") {
                       network.clear_entries();
                       ff_to_next_obj(parser, &mut cb, &mut sq)?;
                   }
               }
               else if state == State::description {
                   network.description.push_str(s.as_ref());
               }
               else if state == State::undefined {
                   panic!("String encountered in asa::process_in_network with unsupported key");
               }

               // Reset the state
               state = State::undefined;

            }
            _ => {}

        }
    }

    #[cfg(not(test))] {
        _progress.finish_with_message("Done processing in_network.");
    }
    
    Ok(())
}


/// Helper function to process_provider_refs. Works on the provider_groups array.
fn process_provider_groups<R: Read>(parser: &mut ReaderJsonParser<R>,
                                    query: &mut Query,
                                    //providers: &mut Vec<Provider>,
                                    ) -> Result<(), Box<dyn std::error::Error>> {

    // To hold the tin type and tin values temporarily. 
    let mut t_type: Option<String> = None;
    let mut t_value: Option<String> = None;
    #[derive(PartialEq)]
    enum CaptureState {
        Ttype,
        Value,
        Npi,
        Undefined,
    }

    let mut state = CaptureState::Undefined;

    let mut sq = 0; // For "[" counting
    let mut cb = 0; // For "{" counting

    let npi_set = query.make_npi_set();

    let providers = &mut query.providers;

    // Read from the parser
    loop {
        let event = {parser.parse_next()?};
        match event {
            JsonEvent::StartObject => {
                cb += 1;
            }
            JsonEvent::EndObject => {
                cb -= 1;
                if cb == 0 {
                    // check for missing values assign "null" if None
                    if t_type.is_none() {
                        //eprintln!("WARNING: Missing tin_type for match in data, will use 'null'");
                        t_type = Some(String::from("null"));
                    }
                    if t_value.is_none() {
                        //eprintln!("WARNING: Missing tin_value for match in data, will use 'null'");
                        t_value = Some(String::from("null"));
                    }
                    // write tin_type and tin_values
                    for p in providers.iter_mut() {
                        if p.needs_tin == true {
                            p.tin_value = t_value.clone();
                            p.tin_type  = t_type.clone();
                            p.needs_tin = false;
                        }
                    }

                    // reset them
                    t_type = None;
                    t_value = None;
                }
            }
            JsonEvent::StartArray => {
                sq += 1;
            }
            JsonEvent::EndArray => {
                sq -= 1;
                if sq == 0 { 
                    // End of the provider_groups array
                    break;
                }
            }
            JsonEvent::ObjectKey(key) => {
                if key == "npi" {
                    state = CaptureState::Npi;
                }
                else if key == "type" {
                    state = CaptureState::Ttype;
                }
                else if key == "value" {
                    state = CaptureState::Value;
                }
                else if key == "tin" {
                    continue
                }
                else {
                    UNSUPPORTED_KEYS.with(|set| {
                        if !set.borrow().contains(key.as_ref()) {
                            eprintln!("Unsupported key {} found in \
                                      provider_groups",key.as_ref());
                        }

                        set.borrow_mut().insert(String::from(key.as_ref()));
                    });
                    bypass_key(parser)?;
                }
            
            }
            // Processes tin_type and tin_value
            // if state is not Ttype or Value this is an error 
            JsonEvent::String(value) => {
                if state == CaptureState::Ttype {
                    t_type = Some(String::from(value.as_ref()));
                    state = CaptureState::Undefined;
                }
                else if state == CaptureState::Value {
                    t_value = Some(String::from(value.as_ref()));
                    state = CaptureState::Undefined;
                }
                else {
                    panic!("Error: string value encountered in invalid state in asa:process_provider_groups");
                }

            }
            // Should be in state Npi here, and process the npi's
            JsonEvent::Number(num) => {
                let curr_npi: u64 = num.as_ref().parse().expect("Failed to parse npi");
                if state != CaptureState::Npi {
                    panic!("Error: number value encountered in invalid state in asa::process_provider_groups");
                }


                // Speed up in case of lots of NPIs 
                if !npi_set.contains(&curr_npi) {
                    continue;
                }

                for i in (0..providers.len()).rev() {
                    if providers[i].npi != curr_npi {
                        continue;
                    }
                    else if providers[i].npi == curr_npi {
                        match providers[i] {
                            // TODO check assertion will not miss tin
                            Provider {group_id: None, tin_value: None,.. } => {
                                providers[i].needs_gid = true;
                                providers[i].needs_tin  = true;
                                break;
                            }
                            _ => {
                                let mut p = Provider::new(curr_npi);
                                p.needs_gid = true;
                                p.needs_tin = true;
                                providers.push(p);
                                break;
                            }
                        }
                    } // End npi match section
                } // End provider vector loop
            } // End JsonEvent::Number
            _ => {}
        }
    }

    Ok(())

}





/// Processes the provider_renferences array in the JSON file.
/// Works to get tin_type and tin_value and provicder references.
/// Creates new entries in the Vec if an NPI has more than one provider group,
/// and/or tin. Calls the helper process_provider_groups.
fn process_provider_refs<R: Read>(parser: &mut ReaderJsonParser<R>,
                                  //providers: &mut Vec<Provider>,
                                  query: &mut Query,
                                  ) -> Result<(), Box<dyn std::error::Error>> {

    let mut non_fatal_err_seen: bool = false;

    // To hold the provider_group_id number 
    let mut pg_id: Option<u64> = None; 

    // To count '{' and '['
    let mut cb = 0;
    let mut sq = 0;
    
    loop {
        let event = {parser.parse_next()?};
        match event {
            JsonEvent::StartObject => {
                cb += 1;
            }
            JsonEvent::EndObject => {
                cb -= 1;
                if cb == 0 {
                    if pg_id.is_none() {
                        eprintln!("Error: provider group id not specified in datafile!");
                        non_fatal_err_seen = true;
                    }
                    for p in query.providers.iter_mut() {
                        if p.needs_gid == true {
                            p.group_id = pg_id.clone();
                            p.needs_gid = false;
                        }
                    }
                }

            }
            JsonEvent::StartArray => {
                sq += 1;
            }
            JsonEvent::EndArray => {
                sq -= 1;
                if sq == 0 {
                    break;
                }
            }
            JsonEvent::ObjectKey(key) => {
                if key == "provider_group_id" {
                    continue;
                }
                else if key == "provider_groups" {
                    let res = process_provider_groups(parser, 
                                                      //providers,
                                                      query);
                    
                    match res {
                        Ok(_) => {}
                        Err(e) => {
                            if e.downcast_ref::<NonFatalError>().is_some() {
                                non_fatal_err_seen = true;
                            }
                            else {
                                return Err(e);
                            }
                            
                        }
                    }
                    
                }
                else {
                    UNSUPPORTED_KEYS.with(|set| {
                        if !set.borrow().contains(key.as_ref()) {
                            eprintln!("Unsupported key {} found in \
                                      provider_references", key.as_ref());
                        }
                        set.borrow_mut().insert(String::from(key.as_ref()));
                    });
                    bypass_key(parser)?;

                }
            }

            // Assertion: This will always be the provider group id. Maybe try to check this?
            JsonEvent::Number(num) => {
                pg_id = Some(num.as_ref().parse().expect("Group ID cannot convert to u64"));
            }

            _ => {}
        }
    }

    if non_fatal_err_seen == true {
        return Err(Box::new(NonFatalError("Non fatal error in asa::process_provider_refs".to_string())));
    }

    Ok(())
}

/// Assumes that we have not yet entered the array but are going to do this next
/// Consider putting this in a general JSON tool module? 
fn skip_array<R: Read>(parser: &mut ReaderJsonParser<R>,
                       mut sq: u64) -> Result<(), Box<dyn std::error::Error>> {

    //let mut sq = 0;

    loop {
        let event = {parser.parse_next()?};
        match event {
            JsonEvent::StartArray => {
            sq += 1;
            }
            JsonEvent::EndArray => {
                sq -= 1;
                if sq == 0 {
                    break;
                }
            }
            _ => {}
        }
    }

    Ok(())
        
}


/// Processes query by looking for matching records in file specified by data_path.
/// buff_size is used to determine the buffer size to use when stream parsing the gz compressed JSON
/// file.
/// Prints status and error messages to stderr
/// Print results (as CSV) to out.
pub fn run(query: &mut Query, 
           data_path: &std::path::PathBuf, 
           buff_size: usize,
           mut out: impl Write) -> Result<(), Box<dyn std::error::Error>> {

    let mut file = File::open(data_path)?;
    let mut gz = GzDecoder::new(file);
    let mut reader = BufReader::with_capacity(buff_size, &mut gz);

    let mut parser = ReaderJsonParser::new(reader);
    let mut depth = 0;

    let mut metadata = Meta::new();
    let mut meta_key: Option<String> = None;

    // If we hit in_network before provider_references
    // we flag needs_reset, then when we hit provider_references
    // we process and then reset.
    // We always exit after processing in_network (nevermind metadata?)
    // We will reset ONLY if, in_network is seen first and provider_references
    // if seen second, if either, or both are missing, program will exit with
    // no results found. 
    let mut providers_seen: bool = false;
    let mut network_seen: bool = false;
    let mut needs_reset: bool = false;

    loop {
        let event = {parser.parse_next()?};
        match event {
            JsonEvent::StartObject => {
                depth += 1;
            }
            JsonEvent::EndObject => {
                 depth -= 1;

                 // By the time we reach the end of the object we should
                 // have seen both provider_references and in_network
                 if providers_seen == false {
                     eprintln!("FATAL ERROR: provider_references not found in file.");
                 }

                 if network_seen == false {
                     eprintln!("FATAL ERROR: in_network not found in file.");
                 }

                 if providers_seen == false || network_seen == false {
                     panic!("");
                 }

                 if depth == 0 && needs_reset == false {
                     break; 
                 }
                 else if depth == 0 && needs_reset == true {
                     // RESET
                    
                     eprintln!("Resetting...");

                     file = File::open(data_path)?;
                     gz = GzDecoder::new(file);
                     reader = BufReader::with_capacity(buff_size, &mut  gz);

                     parser = ReaderJsonParser::new(reader);
                     
                     needs_reset = false;
                 }
            }
            JsonEvent::ObjectKey(key) => {
                if depth != 1 {
                    continue;
                }

                if key == "reporting_entity_name" || 
                   key == "reporting_entity_type" ||
                   key == "last_updated_on"       ||
                   key == "version"
                {
                    meta_key = Some(String::from(key));
                }

                else if key == "provider_references" {
                    providers_seen = true;
                    eprintln!("Processing provider_references...");
                    process_provider_refs(&mut parser, query)?;

                    // Exit early is there is nothing left to process
                    let stat: bool = query.stat_providers();
                    if !stat {
                        eprintln!("No providers from query found in file.");
                        eprintln!("Exiting early...");
                        return Ok(());
                    }
                                    
                } // End provider_references key

                else if key == "in_network" {
                    network_seen = true;
                    if providers_seen == false {
                        // Skip
                        eprintln!("in_network seen first... skipping...");
                        skip_array(&mut parser, 0)?;
                        needs_reset = true;
                        continue;
                    }
                    else {
                        eprintln!("Processing in_network...");
                        process_in_network(&mut parser, query, &mut out)?;
                    }
                }

                else {
                    UNSUPPORTED_KEYS.with(|set| {
                        if !set.borrow().contains(key.as_ref()) {
                            eprintln!("Unsupported key {} found in \
                                       top level", key.as_ref());
                        }
                        set.borrow_mut().insert(String::from(key.as_ref()));
                    });
                    bypass_key(&mut parser)?;
                }

            }

            JsonEvent::String(value) => {
                if depth != 1 {
                    continue;
                }
                if meta_key.is_none() {
                    panic!("Key for field not saved correctly in asa::run");
                }

                metadata.add(&meta_key.unwrap(), value.as_ref())?;
                meta_key = None;
            }

            JsonEvent::Eof => {
                panic!("ERROR: Either the programming logic is wrong or, the JSON is corrupted!");
            }

            _ => {}
        }
    }

    UNSUPPORTED_KEYS.with(|set| {
        let is_empty = set.borrow().is_empty();
        if !is_empty {
            println!("Found the following unsupported keys");
            for key in set.borrow().iter() {
                eprintln!("{}", key);
            }
        }
    });

    Ok(())

}

#[cfg(test)]
mod test_asa {
    use super::*;
    use std::io::Cursor; // For testing
    use crate::query::{Code};

    #[test]
    fn test_process_provider_refs_basic() {
        // Text for JSON
        let json = r#"
            [{"provider_group_id":12345,
             "provider_groups":[{"npi":[21437,3118434,4107354],
                                 "tin":{"type":"ein","value":"881109921"}
                                }
                               ]
             },
             {"provider_group_id":7777777,
             "provider_groups":[{"npi":[200000000,3118434,4107354],
                                 "tin":{"type":"ein","value":"999999999"}
                                }
                               ]
             },
             {"provider_group_id":1000000000,
             "provider_groups":[{"npi":[1221,345027,3547931],
                                 "tin":{"type":"ein","value":"555555555"}
                                }
                               ]
             },
             {"provider_group_id":22222,
             "provider_groups":[{"npi":[11111,51027,1701],
                                 "tin":{"type":"ein","value":"9000"}
                                },
                                {"npi":[1701],
                                 "tin":{"type":"ein","value":"3030"}
                                }
                               ]
             },
             {"provider_group_id":1789,
             "provider_groups":[{"npi":[55,5555,3001],
                                 "tin":{"type":"ein","value":null}
                                }
                                
                               ]
             }

        ]"#;

        let cursor = Cursor::new(json);

        let mut parser = ReaderJsonParser::new(cursor);

        // Make query structs
        let p_ = Provider::new(3001); // Case missing tin_value
        let p0 = Provider::new(100001000); // Case not in data
        let p1 = Provider::new(1701);
        let p2 = Provider::new(21437);
        let p3 = Provider::new(3118434);

        let mut p_test: Vec<Provider> = Vec::new();
        p_test.push(p_);
        p_test.push(p0);
        p_test.push(p1);
        p_test.push(p2);
        p_test.push(p3);

        let mut q_test = Query::new();

        q_test.providers = p_test;

        // Process via function call to mutate
        let _ = process_provider_refs(&mut parser, &mut q_test);

        // Make Structs to compare to

        let mut c_ = Provider::new(3001); // Case missing tin value
        let     c0 = Provider::new(100001000); // Case not in data
        let mut c1 = Provider::new(1701); // Case same gid different tin values
        let mut c2 = Provider::new(21437); // In data one match
        let mut c3 = Provider::new(3118434); // Case different gid
        let mut c4 = Provider::new(3118434);
        let mut c5 = Provider::new(1701); // Case same gid different tin values


        let t_type = Some(String::from("ein"));


        c_.tin_type = t_type.clone();
        c1.tin_type = t_type.clone();
        c2.tin_type = t_type.clone();
        c3.tin_type = t_type.clone();
        c4.tin_type = t_type.clone();
        c5.tin_type = t_type.clone();

        c_.group_id = Some(1789);
        c_.tin_value = Some(String::from("null"));

        c1.tin_value = Some(String::from("9000"));
        c1.group_id  = Some(22222);

        c2.tin_value = Some(String::from("881109921"));
        c2.group_id  = Some(12345);

        c3.tin_value = Some(String::from("881109921"));
        c3.group_id  = Some(12345);

        c4.tin_value = Some(String::from("999999999"));
        c4.group_id  = Some(7777777);

        c5.tin_value = Some(String::from("3030"));
        c5.group_id  = Some(22222);

        let mut check = Vec::new();
        check.push(c_);
        check.push(c0);
        check.push(c1);
        check.push(c2);
        check.push(c3);
        check.push(c4);
        check.push(c5);

        assert_eq!(check, q_test.providers);

    }

    #[test]
    fn test_log_code() {
        let c = String::from("99481");
        let t = String::from("CPT");

        let mut codes: Vec<Code> = Vec::new();
        let mut check: Vec<Code> = Vec::new();

        let c0: Code = Code::new(&t, &c); // Code and Code type match.
        let c1: Code = Code::new(&String::from("LOCAL"), &c); // Code matches, but type does not.
        let c2: Code = Code::new(&String::from("cpT"), &c); // Code and code type match (wrong case)
        let c3: Code = Code::new(&String::from("*"), &c); // Code matches, type is wildcard '*'
        let c4: Code = Code::new(&String::from("CPT"), &String::from("10003")); // Should not match at all

        // Push copies to check vector
        check.push(c0.clone());
        check.push(c1.clone());
        check.push(c2.clone());
        check.push(c3.clone());
        check.push(c4.clone());

        // Manually mark recorded or not
        check[0].recorded = true;
        check[2].recorded = true;
        check[3].recorded = true;

        // Push codes to code vec
        codes.push(c0);
        codes.push(c1);
        codes.push(c2);
        codes.push(c3);
        codes.push(c4);

        let mut q = Query::new();
        q.codes = codes; // Move to Query to call log_code

        // Mutate codes to mark recorded 
        let _ = q.log_code(&c, &t);

        assert_eq!(q.codes, check);

    }

    #[test]
    fn test_proc_neg_prices() {
        let json = r#"[
             {
             "negotiated_type":"alpha",
             "negotiated_rate":498.21,
             "expiration_date":"9999-12-31",
             "service_code":["21","31"],
             "billing_class":"institutional"
             },

             {
             "negotiated_type":"beta",
             "negotiated_rate":505.77,
             "expiration_date":"9999-12-31",
             "service_code":[],
             "billing_class":null
             },

             {
             "negotiated_type":"gamma",
             "expiration_date":"9999-12-31",
             "service_code":["77"],
             "billing_class":"nope",
             "billing_code_modifier":"FSX"
             }

        ]"#;

        let cursor = Cursor::new(json);
        let mut parser = ReaderJsonParser::new(cursor);

        let res = process_negotiated_prices(&mut parser);
        let prices = res.unwrap();

        let mut check: Vec<Price> = Vec::new();

        let mut p0 = Price::new();
        p0.negotiated_type.push_str("alpha");
        p0.negotiated_rate.push_str("498.21");
        p0.expiration_date.push_str("9999-12-31");
        p0.service_code.push_str("21 31 ");
        p0.billing_class.push_str("institutional");

        let mut p1 = Price::new();
        p1.negotiated_type.push_str("beta");
        p1.negotiated_rate.push_str("505.77");
        p1.expiration_date.push_str("9999-12-31");
        p1.service_code.push_str("null");
        p1.billing_class.push_str("null");

        let mut p2 = Price::new();
        p2.negotiated_type.push_str("gamma");
        p2.negotiated_rate.push_str("null");
        p2.expiration_date.push_str("9999-12-31");
        p2.service_code.push_str("77 ");
        p2.billing_class.push_str("nope");
        p2.billing_code_modifier.push_str("FSX");

        p0.push_defaults();
        p1.push_defaults();
        p2.push_defaults();

        check.push(p0);
        check.push(p1);
        check.push(p2);

        assert_eq!(prices, check);
        
    }


    #[test]
    fn basic_test_of_run_and_reset() {

        // Simple basic expected output
        let mut expected_out = String::from("");
        expected_out.push_str("npi,tin_type,tin_value,group_id,negotiation_arrangement,name,billing_code_type,billing_code_type_version,billing_code,description,negotiated_type,negotiated_rate,expiration_date,service_code,billing_class,billing_code_modifier\n");

        expected_out.push_str("1701,ein,101,11,alpha,Item 1,Type 1,2022,CODE 1,Item 1,neg type 1,9.99,9999-12-31,A B C ,class 1,null\n");


        // Case normal input
        let path_str1 = "testfiles/data_files/basic_test.json.gz";
        let filepath1: std::path::PathBuf = std::path::PathBuf::from(path_str1);

        let mut buffer = Vec::new();

        // Input
        let c = Code::new(&String::from("*"), &String::from("Code 1"));
        let p = Provider::new(1701);
        let mut q = Query::new();

        q.codes.push(c);
        q.providers.push(p);

        let mut q2 = q.clone();

        let res = run(&mut q, &filepath1, 256, &mut buffer);
        match res {
            Err(_) => {
                eprintln!("ERROR when processing run!");
            }
            _ => {}
        }

        let output = String::from_utf8(buffer).unwrap();
        
        assert_eq!(output, expected_out);

        let mut buffer2 = Vec::new();

        // Case 2 in_network and provider_references are swapped
        let path_str2 = "testfiles/data_files/backward_basic.json.gz";
        let filepath2: std::path::PathBuf = std::path::PathBuf::from(path_str2);

        let res2 = run(&mut q2, &filepath2, 256, &mut buffer2);
        match res2 {
            Err(_) => {
                eprintln!("ERROR when processing run!");
            }
            _ => {}
        }

        let output2 = String::from_utf8(buffer2).unwrap();
        
        assert_eq!(output2, expected_out);

    }

}
