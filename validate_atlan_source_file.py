#!/usr/bin/env python3

"""
A script to validate a source file generated from the Atlan documentation template.

Usage Options:
-t --table : name of the DynamoDB table with the metadata to read
-i --integration_type : Atlan source integration type: ('DynamoDb', 'glue'). To come: 'Redshift', 'Tableau'). Default=DynamoDb"
-d --delimiter : Source file csv delimiter (default = ',')
"""

import os
import logging
from atlanapi.atlanutils import AtlanConfig, AtlanSourceFile, SourceFileValidator
from optparse import OptionParser

logger = logging.getLogger('main_logger')


def validate_atlan_source_file(path_table_doc, integration_type, delimiter=","):
    logger.info("Loading header template...")
    template_conf = AtlanConfig(os.path.join("/opt/app/config/template_source_file.yaml"))
    template_conf.load_yaml_configs()

    logger.info("Load table definition...")
    source_data = AtlanSourceFile(path_table_doc, delimiter)
    source_data.validate_csv_column_length(template_conf.params["Headers"])
    logger.info("OK: source csv for table {} has the expected number of columns for each row".format(path_table_doc))
    source_data.load_csv()

    logger.info("Validating source file headers")
    validation_source_file = SourceFileValidator(source_data.assets_def)
    validation_source_file.validate_headers(template_conf.params["Headers"])
    logger.info("OK: source file headers match expected for table {}".format(path_table_doc))

    # Add conditions for different integration types as they become supported (e.g., glue)
    logger.info("Validating data type values for source columns")
    # TODO: add validation step for other integration types as they become supported
    if integration_type == 'dynamodb':
        validation_source_file.validate_data_type_values(template_conf.params["DynamoDbDataTypes"])
        print("OK: source file datatypes are all valid values for {}".format(integration_type))
    else:
        pass


if __name__ == '__main__':
    parser = OptionParser(usage='usage: %prog [options] arguments')
    parser.set_defaults(integration_type="DynamoDb", delimiter=",")
    parser.add_option("-p", "--path", help="path of table definition of the DynamoDB table -> Atlan Schema")
    parser.add_option("-i", "--integration_type", choices=['dynamodb', 'glue'],
                      help="Atlan source integration type: ('dynamodb', 'glue'). To come: 'redshift', 'tableau'). "
                           "Default=dynamodb")
    parser.add_option("-d", "--delimiter", help="Source file csv delimiter (default = ',')")
    (options, args) = parser.parse_args()

    validate_atlan_source_file(options.path, options.integration_type, options.delimiter)
