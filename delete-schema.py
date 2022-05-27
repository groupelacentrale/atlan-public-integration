#!/usr/bin/env python3

"""
A script to locate an Atlan schema and delete it if no connected tables are defined.
Usage Options:
-s --schema : name of the database schema
-i --integration_type : name of the Atlan integration type (e.g., dynamodb)
"""

import os
import sys

from optparse import OptionParser

from atlanapi.delete_asset import delete_asset
from atlanapi.searchAssets import get_asset_guid_by_qualified_name
from constants import INTEGRATION_TYPE_DYNAMO_DB, INTEGRATION_TYPE_GLUE, INTEGRATION_TYPE_REDSHIFT
from model.Asset import Schema


def delete_schema(args):

    parser = OptionParser(usage='usage: %prog [options] arguments')
    parser.set_defaults(integration_type=INTEGRATION_TYPE_DYNAMO_DB)
    parser.add_option("-s", "--schema", help="Name of the DynamoDB table -> Atlan Schema")
    parser.add_option("-i", "--integration_type", choices=[INTEGRATION_TYPE_DYNAMO_DB, INTEGRATION_TYPE_GLUE,
                                                           INTEGRATION_TYPE_REDSHIFT],
                      help="Atlan source integration type: ('DynamoDb', 'glue') à venir: 'Redshift', 'Tableau')")
    parser.add_option("-d", "--database", help="Database name")
    (options, args) = parser.parse_args()

    logging.info("Loading API configs...")

    schema = Schema(database_name=options.database, schema_name=options.schema,
                    integration_type=options.integration_type)
    table_info = get_asset_guid_by_qualified_name(schema.get_qualified_name(), 'AtlanTable')

    # If no tables are found belonging to the schema, delete schema
    if "guid" not in table_info:
        print("No tables found attached to schema. Deleting schema.")

        logging.info("Deleting schema: {}".format(options.schema))
        print(schema.get_qualified_name())
        schema_info = get_asset_guid_by_qualified_name(schema.get_qualified_name(), schema.get_atlan_type_name())
        print("deleting schema with guid {}".format(schema_info["guid"]))
        delete_asset(schema_info["guid"])

    else:
        print("There are still tables attached to the schema, so skipping the step to delete the schema: {}".format(options.schema))
        #TODO: print out list of attached tables.


def validate_single_return_response(response_count):
    """
    Function to test that there is one and only one response
    PARAMS:
        response_count: the list of entities in the query response text (e.g., response_text["approximateCount"])
    """
    if response_count > 1:
        print("More than one result found. Please refine your search criteria.")
        sys.exit(1)
    elif response_count == 0:
        print("No results found. Please verify your search criteria and/or that the asset is still present in Atlan.")
        sys.exit(1)
    else:
        return None


if __name__ == '__main__':
    import logging
    if not os.path.isdir("logs"):
        os.makedirs("logs")

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(message)s',
                        filename=os.path.join('logs', 'delete-schema.log'))
    delete_schema(sys.argv[1:])