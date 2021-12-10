#!/usr/bin/env python3

"""
A script to locate an Atlan schema and delete it if no connected tables are defined.

Usage Options:
-s --schema : name of the database schema
-i --integration_type : name of the Atlan integration type (e.g., dynamodb)
"""

import json
import os
import sys

from atlanapi.createquery import AtlanQuery, AtlanQuerySerializer
from atlanapi.atlanutils import AtlanApiRequest, AtlanConfig
from optparse import OptionParser

def delete_schema(args):

    parser = OptionParser(usage='usage: %prog [options] arguments')
    parser.set_defaults(integration_type="dynamodb")
    parser.add_option("-s", "--schema", help="Name of the DynamoDB table -> Atlan Schema")
    parser.add_option("-i", "--integration_type", choices=['dynamodb', 'glue'], help="Atlan source integration type: ('DynamoDb', 'glue') à venir: 'Redshift', 'Tableau')")
    (options, args) = parser.parse_args()

    logging.info("Loading API configs...")
    api_conf = AtlanConfig(os.path.join("config/api_config.yaml"))
    api_conf.load_yaml_configs()

    headers = {
        'Content-Type': 'application/json;charset=utf-8',
        'APIKEY': api_conf.params["api_key"]
    }

    # If no tables remain for the schema, then delete schema
    qual_name_prefix = construct_qualified_name_prefix(options.integration_type)
    schema_tables_qual_name = qual_name_prefix.format(options.schema)
    query_schema_tables = AtlanQuery(schema_tables_qual_name, asset_type='AtlanTable', operator="startsWith")
    query_schema_tables_payload = AtlanQuerySerializer()
    query_schema_tables_url = "https://{}/api/metadata/atlas/tenants/default/search/basic".format(
        api_conf.params["instance"])
    schema_tables_payload = query_schema_tables_payload.serialize(query_schema_tables)
    atlan_api_query_schema_tables_request_object = AtlanApiRequest("POST", query_schema_tables_url, headers, schema_tables_payload)
    query_schema_tables_response = atlan_api_query_schema_tables_request_object.send_atlan_request()
    query_schema_tables_response_text = json.loads(query_schema_tables_response.text)

    # If no tables are found belonging to the schema, delete schema
    response_count = query_schema_tables_response_text["approximateCount"]

    if response_count == 0:
        print("No tables found attached to schema. Deleting schema.")
        # Get schema guid
        schema_qual_name = options.schema
        query_schema = AtlanQuery(schema_qual_name, asset_type='AtlanSchema')
        query_schema_payload = AtlanQuerySerializer()

        query_schema_url = "https://{}/api/metadata/atlas/tenants/default/search/basic".format(api_conf.params["instance"])
        schema_payload = query_schema_payload.serialize(query_schema)
        atlan_api_query_schema_request_object = AtlanApiRequest("POST", query_schema_url, headers, schema_payload)
        query_schema_response = atlan_api_query_schema_request_object.send_atlan_request()
        query_schema_response_text = json.loads(query_schema_response.text)

        # Test that there is one and only one response
        validate_single_return_response(query_schema_response_text["approximateCount"])

        schema_guid = query_schema_response_text["entities"][0]["guid"]
        logging.info("Deleting schema: {}".format(options.schema))
        dt_schema_url = "https://{}/api/metadata/atlas/tenants/default/entity/guid/{}?deleteType=HARD".format(api_conf.params["instance"], schema_guid)
        dt_schema_payload = {}
        dt_schema_headers = {
            'accept': 'application/json, text/plain, */*',
            'APIKEY': api_conf.params["api_key"]
        }
        atlan_api_schema_delete_request_object = AtlanApiRequest("DELETE", dt_schema_url, dt_schema_headers, dt_schema_payload)
        schema_delete_query_response = atlan_api_schema_delete_request_object.send_atlan_request()
        logging.info("API response: {}".format(schema_delete_query_response.text))

    if response_count > 0:
        print("There are still {} tables attached to the schema, so skipping the step to delete the schema: {}".format(response_count, options.schema))
        #TODO: print out list of attached tables.


def construct_qualified_name_prefix(integration_type):
    if integration_type == "dynamodb":
        prefix = "dynamodb/dynamodb.atlan.com/dynamo_db/{}"
    elif integration_type == "glue":
        prefix = "{}/default/{}"
    return prefix

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
