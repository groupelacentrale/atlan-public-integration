# About
This repository contains several scripts for automating asset integration into Atlan. It is used when the asset format is not currently supported in Atlan (for example, DynamoDb). If the asset type is supported, it is recommended to use the native support in Atlan.

# Project Contents:
* **atlanapi/** : directory containing the class modules used by the scripts.
* **config/** : directory containing the configuration files for making the API requests.
* **source_files/** : directory containing the source files to generate dictionaries for DynamoDB tables. A source file is maintained 
   by the team who is in charge of the DynamoDB table and the original version is stored on Git. This file contains the 
   minimum set of information needed to generate the Atlan dictionary files for import.
   For more details on this file, please visit the page on NOE 
* **test/** : directory containing unit tests (TO-DO)
* **create_atlan_table_py** : A script to create a DynamoDB table definition in Atlan from a set of arguments.
# Using the Scripts

## Compatibility:
1. Python 3.7 Tested OK
2. Python 3.10 Tested KO (because of cffi==1.14.6 see [cffi documentation](https://cffi.readthedocs.io/en/release-1.14/installation.html))

## Preparations:
### Repo setup
1. git clone the repo locally
2. Import the project using your Python IDE (I use PyCharm IDE, community version), setting the virtual environment using Python v3.9.
3. run: pip install -r requirements.txt in your shell.

See also: https://note.nkmk.me/en/python-pip-install-requirements

### API Authentication Setup
1. Run 
````python
python create-api-config.py -a <API_KEY>
````

This will generate the configuration file used to authenticate all requests when using
the scripts described below. 

Requirements : an api key generated using an Admin account on Atlan

The script is run using arguments:
-a : API key
-i : (Optional) Server name of the Atlan instance. Use only if instance name differs from default).

## Running the Scripts
The typical workflow when creating a new asset such as a DynamoDb table is to run the following scripts in order (appending the required or optional arguments for each script as detailed in the subsections below):

1. validate-atlan-source-file.py
2. create-atlan-dynamodb-entity.py
3. create-atlan-columns.py
4. create-atlan-column-lineage.py

The script, "delete-table-and-all-columns.py" is for when you want to fully remove a table and all its columns from the Atlan catalog.

### validate-atlan-source-file.py
A script to validate a source file generated from the Atlan documentation template.

The script is run simply using arguments:
-t : name of the table

Example:
````python
python validate-atlan-source-file.py -t pn-da-all
````

### create-atlan-dynamodb-entity.py
A script to create a DynamoDB table definition in Atlan from a set of arguments.

Requirements : the name of the DynamoDB table and its description.

The script is run simply using arguments:
-t : name of the DynamoDB table
-d : description of the table

Example: 
````python
python create-atlan-dynamodb-entity.py -t pn-da-all -e PNDAClassified -d "DynamoDB table containing information about all annonces Promoneuve"
````

### create-atlan-columns.py
A script to read table metadata from a user-specified source file.

Preparations:
1. Place a copy of the source file (Format = CSV and Name = DynamoDB table name) to transform in the directory: ./source_files.* The source file is maintained by the teams and
   stored on Git, and contains the minimum set of information needed to generate the Collibra dictionary files for import.
   For more details on this file, please visit the page on NOE 

The script is run using the following arguments:
-t : Name of the DynamoDB table

Example: 
````python
python create-atlan-columns.py -t pn-da-all
````

### create-atlan-column-lineage.py
A script to define source / target links (lineage) between columns

Preparations:
1. Place a copy of the source file (Format = CSV and Name = DynamoDB table name) to transform in the directory: ./source_files.* The source file is maintained by the teams and
   stored on Git, and contains the minimum set of information needed to generate the Collibra dictionary files for import.
   For more details on this file, please visit the page on NOE 

The script is run using the following arguments:
-t --table : name of the database table where the relationships are defined.
-i --integration_type: name of the integration type for the table where the relationships are defined (e.g., DynamoDb, Glue)

Example: 
````python
python create-atlan-column-lineage.py -t "test-table-source" -i "DynamoDb"
````

Notes: 
* current integration lineages supported are DynamoDb -> DynamoDb and DynamoDb -> glue
* both source and target columns must exist already in Atlan.

### delete-table-and-all-columns.py
A script to locate an Atlan table using its schema and table name, then retrieve and delete
all columns, and finally delete the table.

The script is run using the following arguments:
-s --schema : name of the database schema (redshift or glue) or dynamodb table
-t --table : name of the database table (redshift or glue) or dynamodb entity
-d --database : name of the database
-i --integration_type (e.g., glue, dynamodb, redshift)

Examples: 

Delete table and columns: redshift
````python
ATLAN_API_KEY=<API_KEY> ATLAN_INSTANCE=<ATLAN_INSTANCE_URL> ATLAN_REDSHIFT_SERVER_URL=<REDSHIFT_URL> /usr/local/bin/python3.9 delete-table-and-all-columns.py -s redshift_schema_test -t redshift_table_test -d test_database -i redshift
````

Delete table and columns: glue
````python
ATLAN_API_KEY=<API_KEY> ATLAN_INSTANCE=<ATLAN_INSTANCE_URL> ATLAN_PROD_AWS_ACCOUNT_ID=<AWS_ACCOUNT_ID> /usr/local/bin/python3.9 delete-table-and-all-columns.py -s datalake_test -t test_glue_integration -d glue -i glue
````

Delete table and columns: dynamodb
````python
ATLAN_API_KEY=<API_KEY> ATLAN_INSTANCE=<ATLAN_INSTANCE_URL> /usr/local/bin/python3.9 delete-table-and-all-columns.py -s test-table-lineage -t test -d dynamo_db -i dynamodb
````

### delete-schema.py
A script to delete an Atlan Schema if no connected tables are present.  
NOTE: Originally this was integrated into the script to delete-table-and-all-columns.py, but because 
of the delay to delete the table, and the schema will not be deleted if there are connected tables 
detected, at this point the two scripts remain separate.

The script is run using the following arguments:
-s --schema : name of the database schema
-d --database : name of the database
-i --integration_type (e.g., glue, dynamodb, redshift)

Examples: 

Delete schema: redshift
````python
ATLAN_API_KEY=<API_KEY> ATLAN_INSTANCE=<ATLAN_INSTANCE_URL> ATLAN_REDSHIFT_SERVER_URL=<REDSHIFT_URL> /usr/local/bin/python3.9 delete-shema.py -s redshift_schema_test -d redshift_database_test redshift_table_test -i redshift
````

Delete schema: glue
````python
ATLAN_API_KEY=<API_KEY> ATLAN_INSTANCE=<ATLAN_INSTANCE_URL> ATLAN_PROD_AWS_ACCOUNT_ID=<AWS_ACCOUNT_ID> /usr/local/bin/python3.9 delete-schema.py -s datalake_test -t test_glue_integration -d glue -i glue
````

Delete schema: dynamodb
````python
ATLAN_API_KEY=<API_KEY> ATLAN_INSTANCE=<ATLAN_INSTANCE_URL> ATLAN_PROD_AWS_ACCOUNT_ID=<AWS_ACCOUNT_ID> /usr/local/bin/python3.9 delete-schema.py -s test-table-lineage -t test -d dynamo_db -i dynamodb
````