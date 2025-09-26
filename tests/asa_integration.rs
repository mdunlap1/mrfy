use assert_cmd::prelude::*; // Add methods on commands
//use predicates::prelude::*; // Used for writing assertions
use std::process::Command; // Run programs

#[test]
fn intermediate_integration_test() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("mrfy")?;
    cmd.arg("tests/testfiles/input_testfiles/input_for_intermediate").arg("tests/testfiles/data_files/intermediate.json.gz");


    cmd.assert()
       .success()
       .stdout(String::from("\
npi,tin_type,tin_value,group_id,negotiation_arrangement,name,billing_code_type,billing_code_type_version,billing_code,\
description,negotiated_type,negotiated_rate,expiration_date,service_code,billing_class,billing_code_modifier\n\
1,ein,123,2,alpha,Item 1,Type 1,2022,CODE 1,Item 1,neg type 1,100.99,9999-12-31,A B C ,class 1,null\n\
1,ein,246,2,alpha,Item 1,Type 1,2022,CODE 1,Item 1,neg type 1,100.99,9999-12-31,A B C ,class 1,null\n\
4,ein,777,5,alpha,Item 3,Type 3,2022,CODE 3,Item 3,neg type 3,3000.17,9999-12-31,A ,class 3,null\n\
1,ein,123,2,beta,Item 4,Type 4,null,CODE 4,Item 4,neg type 4,4120.99,9999-12-31,A Z ,class 4,null\n\
1,ein,246,2,beta,Item 4,Type 4,null,CODE 4,Item 4,neg type 4,4120.99,9999-12-31,A Z ,class 4,null\n\
1,ein,123,2,alpha,Item 5,Type 5,2022,CODE 5,Item 5,null,null,null,null,null,null\n\
1,ein,246,2,alpha,Item 5,Type 5,2022,CODE 5,Item 5,null,null,null,null,null,null\n\
2,ein,44,3,alpha,Item 5,Type 5,2022,CODE 5,Item 5,null,null,null,null,null,null\n\
3,ein,55,3,alpha,Item 5,Type 5,2022,CODE 5,Item 5,null,null,null,null,null,null\n\
1,ein,123,2,alpha,Item 7,Type 7,2022,CODE 7,Item 7,neg type 7,739.99,3030-10-31,A B C ,class 7,null\n\
1,ein,246,2,alpha,Item 7,Type 7,2022,CODE 7,Item 7,neg type 7,739.99,3030-10-31,A B C ,class 7,null\n\
2,ein,44,3,alpha,Item 7,Type 7,2022,CODE 7,Item 7,neg type 7,739.99,3030-10-31,A B C ,class 7,null\n\
3,ein,55,3,alpha,Item 7,Type 7,2022,CODE 7,Item 7,neg type 7,739.99,3030-10-31,A B C ,class 7,null\n\
4,ein,777,5,gamma,Item 8,Type 8,2017,CODE 8,Item 8,neg type 8,89.17,9999-12-31,A ,class 8,null\n\
4,ein,777,5,gamma,Item 8,Type 8,2017,CODE 8,Item 8,neg type 8 deluxe,8.45,9999-12-31,Z ,class 8 deluxe,DX\n"));
       

    Ok(())
}
