#!/usr/bin/env python3

"""
A script to read table metadata from the DynamoDB template to catalog a DynamoDB table and post the information
via the Atlan API, the internal Data Governance tool.

Usage Options:
-t --table : name of the DynamoDB table with the metadata to read
-i --integration_type : Atlan source integration type ('DynamoDb', 'glue'). A venir: 'Redshift', 'Tableau'
"""

import json
import os
import sys

from atlanapi.createcolumnlineage import AtlanColumnLineage, AtlanColumnLineageEntityGenerator, AtlanColumnLineageSerializer
from atlanapi.createquery import AtlanQuery, AtlanQuerySerializer
from atlanapi.atlanutils import AtlanApiRequest, AtlanConfig, AtlanSourceFile
from ApiConfig import create_api_config
from optparse import OptionParser
import utils
import  logging

#TODO: add support for 'Redshift', 'Tableau' by determining their qualified name prefix.
from utils import get_column_qualified_name, construct_qualified_name_prefix

SUPPORTED_INTEGRATIONS = ["DynamoDb", "glue"]

logger = logging.getLogger('main_logger')

def create_atlan_column_lineage(path, integration_type, delimiter=","):

    logger.info("Load table definition...")
    source_data = AtlanSourceFile(path, sep=delimiter)
    table_name = utils.get_table_name(path)
    source_data.load_csv()

    api_conf = create_api_config()

    search_headers = {
        'Content-Type': 'application/json;charset=utf-8',
        'APIKEY': api_conf.api_key
    }

    headers = {
        'Content-Type': 'application/json',
        'APIKEY': api_conf.api_key
    }

    lineage_rows = source_data.assets_def[source_data.assets_def["Lineage Integration Type"].isin(SUPPORTED_INTEGRATIONS)]

    # Filter dataframe to include only lineage target columns that already exist in Atlan.
    logger.info("Searching to make sure lineage columns exist")
    k=[]
    full_qualified_name=[]
    integ_prefix=[]

    for index, row in lineage_rows.iterrows():
        if row["Lineage Type (Source / Target)"] != "":
            search_prefix = construct_qualified_name_prefix(row["Lineage Integration Type"])
            query = AtlanQuery(qualified_name=get_column_qualified_name(row["Lineage Schema/Database"], row["Lineage Table/Entity"], row["Lineage Column/Attribute"], search_prefix), asset_type="AtlanColumn")
            q_payload = AtlanQuerySerializer()
            query_payload = q_payload.serialize(query)
            query_url = "https://{}/api/metadata/atlas/tenants/default/search/basic".format(api_conf.instance)

            atlan_api_query_request_object = AtlanApiRequest("POST", query_url, search_headers, query_payload)
            query_response = atlan_api_query_request_object.send_atlan_request()
            t = json.loads(query_response.text)
            key_present = check_key(t, 'entities')
            f_qualified_name = get_qualified_name(t, key_present)
            row_integ_prefix = construct_qualified_name_prefix(integration_type)
            k.append(key_present)
            full_qualified_name.append(f_qualified_name)
            integ_prefix.append(row_integ_prefix)
    lineage_rows.insert(2, "column_present_in_atlan", k)
    lineage_rows.insert(3, "lineage_full_qualified_name", full_qualified_name)
    lineage_rows.insert(4, "integration_prefix", integ_prefix)

    lineage_rows_verified = lineage_rows[lineage_rows.column_present_in_atlan.eq('true')]
    lineage_rows_not_verified = lineage_rows[lineage_rows.column_present_in_atlan.eq('false')]

    logger.warning("Target columns must already exist in Atlan to create lineage. The following target columns were not found in Atlan: {}/{}/{}".format(lineage_rows_not_verified["Lineage Schema/Database"],
                                                                                                                                                    lineage_rows_not_verified["Lineage Table/Entity"],
                                                                                                                                                    lineage_rows_not_verified["Lineage Column/Attribute"]))
    logger.info("Generating API request to create lineage for verified columns in table: {}".format(table_name))
    entity_items = []
    for index, row in lineage_rows_verified.iterrows():
        if row["Lineage Type (Source / Target)"] != "":
            if row["Lineage Type (Source / Target)"] == "Source":
                col_lineage = AtlanColumnLineage(source_integration_type=row["Lineage Integration Type"],
                                                 source_qualified_name=row["lineage_full_qualified_name"],
                                                 target_integration_type=integration_type,
                                                 target_qualified_name=get_column_qualified_name(table_name, row["Table/Entity"], row["Column/Attribute"], row["integration_prefix"]))
            elif row["Lineage Type (Source / Target)"] == "Target":
                col_lineage = AtlanColumnLineage(source_integration_type=integration_type,
                                                 source_qualified_name=get_column_qualified_name(table_name, row["Table/Entity"], row["Column/Attribute"], row["integration_prefix"]),
                                                 target_integration_type=row["Lineage Integration Type"],
                                                 target_qualified_name=row["lineage_full_qualified_name"])

            generator = AtlanColumnLineageEntityGenerator()
            e = generator.create_column_lineage_entity(col_lineage)
            entity_items.append(e)
    if entity_items:
        col_payload = AtlanColumnLineageSerializer()
        payload = col_payload.serialize(entity_items)

        logger.info("Posting API request")
        lineage_post_url = 'https://{}/api/metadata/atlas/tenants/default/entity/bulk'.format(api_conf.instance)
        print(payload)
        atlan_api_lineage_request_object = AtlanApiRequest("POST", lineage_post_url, headers, payload)
        atlan_api_lineage_request_object.send_atlan_request()


def check_key(dict, key):
    if key in dict.keys():
        key_present = 'true'
    else:
        key_present = 'false'
    return key_present


def get_qualified_name(dict, key_present):
    if key_present == 'true':
        return dict['entities'][0]['attributes']['qualifiedName']
    else:
        return None


if __name__ == '__main__':
    parser = OptionParser(usage='usage: %prog [options] arguments')
    parser.set_defaults(delimiter=",")
    parser.add_option("-p", "--path", help="Name of the DynamoDB table -> Atlan Schema")
    parser.add_option("-i", "--integration_type", choices=['DynamoDb', 'glue'], help="Atlan source integration type: ('DynamoDb', 'glue') à venir: 'Redshift', 'Tableau')")
    parser.add_option("-d", "--delimiter", help="Source file csv delimiter (default = ',')")
    (options, args) = parser.parse_args()

    create_atlan_column_lineage(options.path, options.integration_type, options.delimiter)