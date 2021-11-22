#!/usr/bin/env python3

"""
A script to validate a source file generated from the Atlan documentation template.

Usage Options:
-t --table : name of the DynamoDB table with the metadata to read
"""

import os
import logging
from atlanapi.atlanutils import AtlanConfig, AtlanSourceFile, SourceFileValidator
from optparse import OptionParser
logger = logging.getLogger('main_logger')

def validate_atlan_source_file(path_table_doc, delimiter=","):
    logger.info("Loading header template...")
    template_conf = AtlanConfig(os.path.join("/opt/app/config/template_source_file.yaml"))
    template_conf.load_yaml_configs()

    logger.info("Load table definition...")
    source_data = AtlanSourceFile(path_table_doc, delimiter)
    source_data.validate_csv_column_length(template_conf.params["Headers"])
    logger.info("OK: source csv for table {} has the expected number of columns for each row".format(path_table_doc))
    source_data.load_csv()

    validation_source_file = SourceFileValidator(source_data.assets_def)
    validation_source_file.validate_headers(template_conf.params["Headers"])
    logger.info("OK: source file headers match expected for table {}".format(path_table_doc))


if __name__ == '__main__':
    parser = OptionParser(usage='usage: %prog [options] arguments')
    parser.set_defaults(delimiter=",")
    parser.add_option("-p", "--path", help="path of table definition of the DynamoDB table -> Atlan Schema")
    parser.add_option("-d", "--delimiter", help="Source file csv delimiter (default = ',')")
    (options, args) = parser.parse_args()

    validate_atlan_source_file(options.path, options.delimiter)