# mrfy
An MRF parsing tool. (Currently only supports Aetna Signature Administrators.)

## Background 
Insurance companies publish negotiated price information in MRFs (Machine 
Readable Files), pursuant to the Transparency in Coverage Final Rule. These
files are not readily useable by people as they are difficult to work with.


## Overview
This project aims to provide a tool that pulls data out of MRFs, subject to the
constraints that the user has: limited compute resources, limited disk space 
(only enough to hold the MRF and the output), and does not want to use cloud 
services.

Consequently the aim is not to convert to say parquet format for efficient 
querying, rather to stream parse the data and extract relevant records. 


## Use
### Writing an input query file.
The input file should contain an un-indented line that says "npi" followed by
one or more lines under it, each specifying an npi and being indented with one
or more spaces (not tabs).

Billing codes are similarly specified, by a non-indented line giving the billing
code type, followed by one or more billing codes of that type, each written on 
its own line and indented with one or more spaces (not tabs). Instead of a 
billing code type, one can put an asterisk.

NOTE: The program will pull data that matches the code *regardless* of 
      the code type. The code type is specified for readability and for
      a final printout that tells which (code, code type) pair didn't have 
      billing records for the given NPIs. If using asterisk for type, the 
      failure to match is reported only if there were absolutely no matches 
      *of any billing code type* for that code. 


For example:
```
npi
  12345678
  23345678
cpt
  90000
  90101
*
  70071
```

One can also use asterisk for the billing code. In that case all billing codes
of all types for the given NPIs will match. 


### Accessing the target data file. 
Currently this program only supports the In-network Rates & Allowed Amounts File
for Aetna Signature Administrators.

Specifically it has been devleoped and tested on version 1.3.1 of that MRF.

For now, it will be left to the user to find the data. 

### Running the program
The program is currently run from the project directory with:
```
cargo run <INPUT_PATH> <DATA_PATH> [BUFF_SIZE]
```

- INPUT\_PATH is to the input file mentioned earlier.
- DATA\_PATH  is to the MRF file
- BUFF\_SIZE  optionally change the buffer size for buffer used to read the file data file (default is 128 MiB).

Status updates will print to stdrr. Any billing records that match the query
will be printed to stdout in csv file format (a header will also print). 

When the program is done processing the file it will report (to stderr) any 
part of the query that didn't have a match. More specifically a code will be
reported as having no matches if none of the NPIs had a billing record for it.
To accommodate the possibility that an NPI might have more than one tin associated with it, any (npi,tin) pair found in the dataset that didn't match on at
least one billing code will be reported. Any NPI for which none of the user
supplied billing codes turned up a result will be reported as having zero 
matches. 

#### Aetna Signature Administrators
The Aetna Signature Administrators MRF is available as a compressed JSON file.
The compressed size if about 5 GiB. If fully de-compressed it would be about 
200 GiB. This program decompresses in chunks, and parses a stream of data, 
instead of decompressing the whole file at once. 

(See asa.rs for more assertions about the format.)


## Disclaimer
This program is the work of one individual. There is no guaruntee of fitness for
any purpose. The author has no affiliation or endorsement of any kind for this
program. Anyone that uses this takes responsibility for the results. 

The program has unit tests, and some unoffical integration tests have been done.
However, more testing is still needed.
