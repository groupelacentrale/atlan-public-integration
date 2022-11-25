#!/usr/bin/env python3

"""
A script to validate a source file generated from the Atlan documentation template.

Usage Options:
-t --table : name of the DynamoDB table with the metadata to read
-i --integration_type : Atlan source integration type: ('DynamoDb', 'glue'). To come: 'Redshift', 'Tableau'). Default=DynamoDb"
-d --delimiter : Source file csv delimiter (default = ',')
"""

import logging

import utils
import os

from atlanapi.atlanutils import AtlanConfig, AtlanSourceFile, SourceFileValidator
from optparse import OptionParser

from constants import INTEGRATION_TYPE_DYNAMO_DB
from utils import get_template_source_file

logger = logging.getLogger('main_logger')


def validate_atlan_source_file(schema_name, table_or_entity_name, integration_type, delimiter=","):
    logger.debug("Loading header template...")
    template_conf = AtlanConfig(get_template_source_file())
    template_conf.load_yaml_configs()

    logger.debug("Load table definition...")
    integration_type = integration_type
    path_csv_table = utils.get_path(integration_type, schema_name, table_or_entity_name)
    source_data = AtlanSourceFile(path_csv_table, delimiter)

    source_data.load_csv()

    logger.info("Validating source file headers")
    validation_source_file = SourceFileValidator(source_data.assets_def)
    validation_source_file.validate_headers(template_conf.params["Headers"])
    logger.info("OK: source file headers match expected for file {}".format(path_csv_table))

    # Add conditions for different integration types as they become supported (e.g., glue)
    logger.info("Validating data type values for source columns")
    # TODO: add validation step for other integration types as they become supported
    if integration_type == INTEGRATION_TYPE_DYNAMO_DB:
        validation_source_file.validate_data_type_values(template_conf.params["DynamoDbDataTypes"])
        logger.info("OK: source file datatypes are all valid values for integration type: '{}'".format(integration_type.lower()))
    else:
        pass


if __name__ == '__main__':
    parser = OptionParser(usage='usage: %prog [options] arguments')
    parser.set_defaults(integration_type="dynamodb", delimiter=",")
    parser.add_option("-s", "--schema", help="Name of the DynamoDB table -> Atlan Schema")
    parser.add_option("-t", "--table", help="Name of the DynamoDB entity -> Atlan Table")
    parser.add_option("-i", "--integration_type", choices=['dynamodb', 'glue'],
                      help="Atlan source integration type: ('dynamodb', 'glue'). To come: 'redshift', 'tableau'). "
                           "Default=dynamodb")
    parser.add_option("-d", "--delimiter", help="Source file csv delimiter (default = ',')")
    (options, args) = parser.parse_args()

    validate_atlan_source_file(options.schema, options.table, options.integration_type, options.delimiter)
