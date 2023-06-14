#!/usr/bin/env python3

"""
A script to locate an Atlan table using its schema and table name, then retrieve and delete
all columns, and finally delete the table.

Usage Options:
-s --schema : name of the database schema
-t --table : name of the database table
"""

import json
import os
import sys

from atlanapi.atlanutils import AtlanApiRequest
from optparse import OptionParser
from atlanapi.ApiConfig import create_api_config
from atlanapi.searchAssets import get_asset_guid_by_qualified_name
from model.table import Table


def delete_atlan_table_and_all_columns(args):
    parser = OptionParser(usage='usage: %prog [options] arguments')
    parser.add_option("-d", "--database", help="Name of the database -> Atlan Database")
    parser.add_option("-s", "--schema", help="Name of the DynamoDB table -> Atlan Schema")
    parser.add_option("-t", "--table", help="Name of the DynamoDB entity -> Atlan Table")
    parser.add_option("-i", "--integration_type", help="Name of the integration type -> Atlan integration type")
    (options, args) = parser.parse_args()

    logging.info("Loading API configs...")
    api_conf = create_api_config()

    logging.info("Searching for table metadata for {}.{}".format(options.schema, options.table))

    table = Table(database_name=options.database, schema_name=options.schema, entity_name=options.table,
                  integration_type=options.integration_type)
    table_info = get_asset_guid_by_qualified_name(table.get_qualified_name(), 'AtlanTable')

    print('Delete the table {} (guid={}) and all its columns? Proceed (y/n)?'.format(
        table_info["attributes"]["qualifiedName"], table_info["guid"]))
    x = input()
    if x == "y":
        print("proceeding...")
        # TODO: create getcolumns class and instantiate here
        c_payload = {}
        c_headers = {
            'APIKEY': api_conf.api_key
        }
        c_url = "https://{}/api/metadata/atlas/tenants/default/search/relationship?guid={}&limit=1000&offset=0&relation=columns&excludeDeletedEntities=true&attributes=integrationType&attributes=name&attributes=dataType&attributes=description&sortBy=name&sortOrder=ASCENDING".format(
            api_conf.instance, table_info["guid"])

        atlan_api_column_query_request_object = AtlanApiRequest("GET", c_url, c_headers, c_payload)
        column_query_response = atlan_api_column_query_request_object.send_atlan_request()

        logging.info("Deleting columns from table: {}.{}".format(options.schema, options.table))
        c = json.loads(column_query_response.text)
        for i in c["entities"]:
            logging.info("Deleting column guid: {}".format(i["guid"]))
            d_url = "https://{}/api/metadata/atlas/tenants/default/entity/guid/{}?deleteType=HARD".format(
                api_conf.instance, i["guid"])
            d_payload = {}
            d_headers = {
                'accept': 'application/json, text/plain, */*',
                'APIKEY': api_conf.api_key
            }

            atlan_api_column_delete_request_object = AtlanApiRequest("DELETE", d_url, d_headers, d_payload)
            column_delete_query_response = atlan_api_column_delete_request_object.send_atlan_request()

            logging.info("API response: {}".format(column_delete_query_response.text))

        logging.info("Deleting table: {}.{}".format(options.schema, options.table))
        dt_url = "https://{}/api/metadata/atlas/tenants/default/entity/guid/{}?deleteType=HARD".format(
            api_conf.instance, table_info["guid"])
        dt_payload = {}
        dt_headers = {
            'accept': 'application/json, text/plain, */*',
            'APIKEY': api_conf.api_key
        }

        atlan_api_table_delete_request_object = AtlanApiRequest("DELETE", dt_url, dt_headers, dt_payload)
        table_delete_query_response = atlan_api_table_delete_request_object.send_atlan_request()
        logging.info("API response: {}".format(table_delete_query_response.text))


    else:
        print("not proceeding...")


if __name__ == '__main__':
    import logging

    if not os.path.isdir("logs"):
        os.makedirs("logs")

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s      :: %(message)s',
                        filename=os.path.join('logs', 'delete-table-and-all-columns.log'))
    delete_atlan_table_and_all_columns(sys.argv[1:])
