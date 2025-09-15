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

The Aetna Signature Administrators MRF is available as a compressed JSON file.
The compressed size if about 5 GiB. If fully de-compressed it would be about 
200 GiB. This program decompresses in chunks, and parses a stream of data, 
instead of decompressing the whole file. 


## Use
### Writing an input query file.
The file will specify one or more NPIs (National Provider Numbers) and one or 
more billing codes. 

We need a line that says npi under which we will have one NPI listed per
line indented with one or more spaces. 

The billing codes will be indented with one more spaces and listed under a line
containing either the billing codes type or an asterisk if we do not care (or
know) what the type is.


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

NOTE: The program will pull data that matches the code *regardless* of 
      the code type. The code type is specified for readability and for
      a final printout that tells which codes were not found. 

### Accessing the target data file. 
Currently this program only supports the In-network Rates & Allowed Amounts File
for Aetna Signature Administrators.

Specifically it has been devleoped and tested on the file with metadata:

```
reporting_entity_name: Aetna Signature Administrators
reporting_entity_type: Third Party Vendor
last_updated_on: 2025-04-05
version: 1.3.1
```

### Running the program
The program is currently run with:
```
Usage: mrfy <INPUT_PATH> <DATA_PATH> [BUFF_SIZE]
```

input path is to the input file mentioned earlier
data path is to the .gz compressed MRF file
buff size allows for optional changing of the buffer size (default is 128 MiB).

The program will print out the metadata for the file and give status updates
once it finds the "provider references" array and the "in network" array. 
If no NPIs from the input query match it will exit early. 

Once it start processing the "in network" array a progress bar will display
giving an estimate of how much more data there is to process. 

Any data records that match the input query get printed to stdout as a CSV file.


## Considerations
The program took a little less than eight hours to run to completion on a 14 
year old laptop. It might be best to run with tmux and come back to it. 


## Disclaimer
This program is the work of one individual. There is no guaruntee of fitness for
any purpose. The author has no affiliation or endorsement of any kind for this
program. Anyone that uses this takes responsibility for the results. 

The program has unit tests, and some unoffical integration tests have been done.
However, more testing is still needed. 
