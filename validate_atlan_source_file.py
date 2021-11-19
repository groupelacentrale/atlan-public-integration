#!/usr/bin/env python3

"""
A script to validate a source file generated from the Atlan documentation template.

Usage Options:
-t --table : name of the DynamoDB table with the metadata to read
-i --integration_type : Atlan source integration type: ('DynamoDb', 'glue'). To come: 'Redshift', 'Tableau'). Default=DynamoDb"
-d --delimiter : Source file csv delimiter (default = ',')
"""

import os
import sys

from atlanapi.atlanutils import AtlanConfig, AtlanSourceFile, SourceFileValidator
from optparse import OptionParser

def validate_atlan_source_file(args):

    parser = OptionParser(usage='usage: %prog [options] arguments')
    parser.set_defaults(integration_type="DynamoDb", delimiter=",")
    parser.add_option("-t", "--table", help="Name of the DynamoDB table -> Atlan Schema")
    parser.add_option("-i", "--integration_type", choices=['DynamoDb', 'glue'], help="Atlan source integration type: ('DynamoDb', 'glue'). To come: 'Redshift', 'Tableau'). Default=DynamoDb")
    parser.add_option("-d", "--delimiter", help="Source file csv delimiter (default = ',')")
    (options, args) = parser.parse_args()

    logging.info("Loading header template...")
    template_conf = AtlanConfig(os.path.join("config/template_source_file.yaml"))
    template_conf.load_yaml_configs()

    logging.info("Load table definition...")
    source_data = AtlanSourceFile(os.path.join("source_files", "{}.csv".format(options.table)), options.delimiter)
    source_data.validate_csv_column_length(template_conf.params["Headers"])
    print("OK: source csv for table {} has the expected number of columns for each row".format(options.table))
    source_data.load_csv()

    logging.info("Validating source file headers")
    validation_source_file = SourceFileValidator(source_data.assets_def)
    validation_source_file.validate_headers(template_conf.params["Headers"])
    print("OK: source file headers match expected for table {}".format(options.table))

    # Add conditions for different integration types as they become supported (e.g., glue)
    logging.info("Validating data type values for source columns")
    #TODO: add validation step for other integration types as they become supported
    if options.integration_type == 'DynamoDb':
        validation_source_file.validate_data_type_values(template_conf.params["DynamoDbDataTypes"])
        print("OK: source file datatypes are all valid values for {}".format(options.integration_type))
    else:
        pass


if __name__ == '__main__':
    import logging
    if not os.path.isdir("logs"):
        os.makedirs("logs")
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(message)s',
                        filename=os.path.join('logs', 'validate-atlan-source-file.log'))
    validate_atlan_source_file(sys.argv[1:])