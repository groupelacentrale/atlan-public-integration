#!/usr/bin/env python3

"""
A script to read table metadata from the DynamoDB template to catalog a DynamoDB table and post the information
via the Atlan API, the internal Data Governance tool.

Usage Options:
-t --table : name of the DynamoDB table with the metadata to read
-n --no_entity : specify this flag only if there are no entity / attribute relationships in the source file.
"""

import logging
import time

from atlanapi.createReadme import create_readme
from atlanapi.delete_asset import delete_asset
from atlanapi.get_entity_columns import get_entity_columns
from atlanapi.linkTerm import link_term
from atlanapi.createcolumn import AtlanColumn, AtlanColumnEntityGenerator, AtlanColumnSerializer
from atlanapi.atlanutils import AtlanApiRequest, AtlanSourceFile
from optparse import OptionParser
from ApiConfig import create_api_config
import utils
from atlanapi.searchAssets import get_asset_guid_by_qualified_name

logger = logging.getLogger('main_logger')

def create_atlan_columns(path, delimiter=",", includes_entity=False):

    logger.info("Load table definition...")
    source_data = AtlanSourceFile(path, sep=delimiter)
    table_name = utils.get_table_name(path)
    source_data.load_csv()

    api_conf = create_api_config()

    # Generate columns that are combinations of multiple variables

    if includes_entity == True:
        source_data.assets_def["Name"] = source_data.assets_def["Table/Entity"] + "." + source_data.assets_def["Column/Attribute"]
        source_data.assets_def = source_data.assets_def.drop(columns=["Table/Entity"])
    else:
        source_data.assets_def["Name"] = source_data.assets_def["Column/Attribute"]

    logger.info("Generating API request to create columns for table: {}".format(table_name))
    entity_items = []
    entities = {entity: [] for entity in source_data.assets_def["Table/Entity"]}

    for index, row in source_data.assets_def.iterrows():
        col = AtlanColumn(integration_type="DynamoDb",
                          name=row['Name'],
                          data_type=row['Type'],
                          description=row['Summary (Description)'],
                          qualified_name="dynamodb/dynamodb.atlan.com/dynamo_db/{}/{}/{}".format(table_name, row["Table/Entity"], row['Name']))
        generator = AtlanColumnEntityGenerator()
        e = generator.create_column_entity(col)
        entity_items.append(e)
        entities[row["Table/Entity"]].append(row['Name'])

    # Deleting columns no longer mentioned in csv file
    for entity in entities:
        asset_info = get_asset_guid_by_qualified_name("dynamodb/dynamodb.atlan.com/dynamo_db/{}/{}".format(table_name, entity))
        existing_columns = get_entity_columns(asset_info['guid'])
        for existing_column in existing_columns:
            if existing_column not in entities[entity]:
                print('Deleting column {} ...'.format(existing_column))
                delete_asset(existing_columns[existing_column])
                print('{} Deleted successfully'.format(existing_column))

    col_payload = AtlanColumnSerializer()
    payload = col_payload.serialize(entity_items)

    headers = {
        'Content-Type': 'application/json',
        'APIKEY': api_conf.api_key
    }

    logger.info("Posting API request")
    column_post_url = 'https://{}/api/metadata/atlas/tenants/default/entity/bulk'.format(api_conf.instance)
    atlan_api_column_request_object = AtlanApiRequest("POST", column_post_url, headers, payload)
    atlan_api_column_request_object.send_atlan_request()

    time.sleep(1)

    for index, row in source_data.assets_def.iterrows():
        if row['Readme']:
            create_readme(table_name, row["Table/Entity"], row['Name'], row['Readme'])
        if row['Term'] and row['Glossary']:
            link_term(table_name, row["Table/Entity"], row['Name'], row['Term'], row["Glossary"])



if __name__ == '__main__':
    parser = OptionParser(usage='usage: %prog [options] arguments')
    # TODO : replace argument for includes_entity to a test if the column exists in the source file. Too dangerous if the flag is set incorrectly!
    parser.set_defaults(includes_entity=False, delimiter=",")
    parser.add_option("-p", "--path", help="path of the DynamoDB table -> Atlan Schema")
    parser.add_option("-d", "--delimiter", help="Source file csv delimiter (default = ',')")
    # parser.add_option("-n", "--no_entity", action="store_false", dest="includes_entity",
    #                  default=True, help="True if the DynamoDb column definitions contain entities and attributes")
    (options, args) = parser.parse_args()
    create_atlan_columns(options.path, options.delimiter)
