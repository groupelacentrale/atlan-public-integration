#!/usr/bin/env python3

"""
A script to read table metadata from the DynamoDB template to catalog a DynamoDB table and post the information
via the Atlan API, the internal Data Governance tool.

Usage Options:
-t --table : name of the DynamoDB table with the metadata to read
-n --no_entity : specify this flag only if there are no entity / attribute relationships in the source file.
"""

import logging
from optparse import OptionParser

import utils
from atlanapi.atlanutils import AtlanSourceFile
from atlanapi.createAsset import create_assets
from atlanapi.delete_asset import delete_asset
from atlanapi.get_entity_columns import get_entity_columns
from atlanapi.searchAssets import get_asset_guid_by_qualified_name
from model.Asset import Column, Entity, INTEGRATION_TYPE_DYNAMO_DB

logger = logging.getLogger('main_logger')


def create_atlan_columns(path, delimiter=",", includes_entity=False):
    logger.debug("Load table definition...")
    source_data = AtlanSourceFile(path, sep=delimiter)
    table_name = utils.get_table_name(path)
    source_data.load_csv()

    # Generate columns that are combinations of multiple variables

    if includes_entity:
        source_data.assets_def["Name"] = source_data.assets_def["Table/Entity"] + "." + source_data.assets_def[
            "Column/Attribute"]
        source_data.assets_def = source_data.assets_def.drop(columns=["Table/Entity"])
    else:
        source_data.assets_def["Name"] = source_data.assets_def["Column/Attribute"]

    logger.debug("Preparing API request to create columns for table: {}".format(table_name))
    entities = {entity: [] for entity in source_data.assets_def["Table/Entity"]}
    columns = []

    for index, row in source_data.assets_def.iterrows():
        col = Column(integration_type=INTEGRATION_TYPE_DYNAMO_DB,
                     entity_name=row["Table/Entity"],
                     schema_name=table_name,
                     column_name=row['Name'],
                     data_type=row['Type'],
                     description=row['Summary (Description)'],
                     readme=row['Readme'],
                     term=row['Term'].strip(),
                     glossary=row['Glossary'].strip())
        columns.append(col)
        entities[row["Table/Entity"]].append(row['Name'])

    logger.debug("Preparing API request to delete columns no longer mentioned in csv file")
    for entity in entities:
        e = Entity(entity_name=entity, schema_name=table_name)
        # logging.info("Getting table id...")
        asset_info = get_asset_guid_by_qualified_name(e.get_qualified_name(), e.get_atlan_type_name())
        # logging.info("Getting existing column ids...")
        existing_columns = get_entity_columns(asset_info['guid'])
        for existing_column in existing_columns:
            if existing_column not in entities[entity]:
                logger.info("Deleting column no longer mentioned in csv file: '{}'...".format(existing_column))
                delete_asset(existing_columns[existing_column])
                logger.info('{} Deleted successfully'.format(existing_column))

    create_assets(columns)


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
