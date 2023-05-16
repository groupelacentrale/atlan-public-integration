#!/usr/bin/env python3

"""
A script to read table metadata from the DynamoDB template to catalog a DynamoDB table and post the information
via the Atlan API, the internal Data Governance tool.

Usage Options:
-t --table : name of the DynamoDB table with the metadata to read
-n --no_entity : specify this flag only if there are no entity / attribute relationships in the source file.
"""

import logging
import utils
from optparse import OptionParser

from atlanapi.attach_classification import attach_classification
from atlanapi.delete_asset import delete_asset
from atlanapi.get_entity_columns import get_entity_columns
from atlanapi.linkTerm import link_term
from atlanapi.searchAssets import get_asset_guid_by_qualified_name
from atlanapi.atlanutils import AtlanSourceFile
from atlanapi.createAsset import create_assets, update_assets
from constants import INTEGRATION_TYPE_DYNAMO_DB, INTEGRATION_TYPE_ATHENA, INTEGRATION_TYPE_REDSHIFT
from model import Column, Table

logger = logging.getLogger('main_logger')


def create_atlan_columns(database_name,
                         schema_name,
                         table_or_entity_name,
                         integration_type,
                         delimiter=",",
                         includes_entity=False,
                         table=None):
    logger.debug("Load table definition...")
    path_csv_table = utils.get_path(integration_type, schema_name, table_or_entity_name)
    source_data = AtlanSourceFile(path_csv_table, sep=delimiter)
    source_data.load_csv()

    # Generate columns that are combinations of multiple variables

    if includes_entity and integration_type == INTEGRATION_TYPE_DYNAMO_DB:
        source_data.assets_def["Name"] = source_data.assets_def["Table/Entity"] + "." + source_data.assets_def[
            "Column/Attribute"]
        source_data.assets_def = source_data.assets_def.drop(columns=["Table/Entity"])
    else:
        source_data.assets_def["Name"] = source_data.assets_def["Column/Attribute"]

    logger.debug("Preparing API request to create columns for table: {}"
                 .format(schema_name if integration_type == INTEGRATION_TYPE_DYNAMO_DB else table_or_entity_name))
    columns = []

    for index, row in source_data.assets_def.iterrows():
        col = Column(integration_type=integration_type,
                     database_name=database_name,
                     entity_name=row["Table/Entity"],
                     schema_name=schema_name,
                     column_name=row['Name'],
                     description=row['Summary (Description)'],
                     readme=row['Readme'],
                     term=row['Term'].strip(),
                     glossary=row['Glossary'].strip(),
                     classification=row['Classification'])
        if integration_type == INTEGRATION_TYPE_DYNAMO_DB:
            col.data_type = row['Type']

        columns.append(col)

    distinct_columns = set()
    count_columns_asset = 0
    columns = [col for col in columns if col not in distinct_columns and (distinct_columns.add(col) or True)]

    logger.debug("Preparing API request to delete columns no longer mentioned in csv file")
    if integration_type.lower() == INTEGRATION_TYPE_DYNAMO_DB:
        entities = {column.entity_name: [] for column in columns}
        for column in columns:
            entities[column.entity_name].append(column.column_name)
        for entity in entities:
            e = Table(entity_name=entity, database_name=database_name, schema_name=schema_name)
            asset_info_guid = get_asset_guid_by_qualified_name(e.get_qualified_name(), e.get_atlan_type_name())
            existing_columns = get_entity_columns(asset_info_guid)
            for existing_column in existing_columns:
                if existing_column not in entities[entity]:
                    logger.info("Deleting column no longer mentioned in csv file: '{}'...".format(existing_column))
                    # TODO check output from list columns because it's only guid column
                    delete_asset(existing_columns[existing_column])
                    logger.info("'{}' Deleted successfully".format(existing_column))
        for column in columns:
            if column.entity_name == table.entity_name:
                count_columns_asset += 1

        create_assets(columns, "createColumns")
        table.set_column_count(count_columns_asset)
        #update_assets([table], "createTables")
    else :
        columns_exist_in_atlan = [column for column in columns if get_asset_guid_by_qualified_name(column.get_qualified_name(), column.get_atlan_type_name())]
        for c in columns_exist_in_atlan:
            print(c)
        logger.info("Update asset : {}, integration type : {}".format(table_or_entity_name, integration_type))
        update_assets(columns_exist_in_atlan, "changeDescription")
        #attach_classification(columns_exist_in_atlan)
        #link_term(columns_exist_in_atlan)


if __name__ == '__main__':
    parser = OptionParser(usage='usage: %prog [options] arguments')
    # TODO : replace argument for includes_entity to a test if the column exists in the source file. Too dangerous if the flag is set incorrectly!
    parser.set_defaults(includes_entity=False, delimiter=",")
    parser.add_option("-d", "--database", help="Name of the database")
    parser.add_option("-s", "--schema", help="Name of the DynamoDB table -> Atlan Schema")
    parser.add_option("-t", "--table", help="Name of the DynamoDB entity -> Atlan Table")
    parser.add_option("-i", "--integration_type", choices=[INTEGRATION_TYPE_DYNAMO_DB, INTEGRATION_TYPE_ATHENA],
                      help="Atlan source integration type: ('DynamoDb', 'glue') à venir: 'Redshift', 'Tableau')"
                           "(default = '{}}')".format(INTEGRATION_TYPE_DYNAMO_DB))
    parser.add_option("-d", "--delimiter", help="Source file csv delimiter (default = ',')")
    (options, args) = parser.parse_args()
    create_atlan_columns(options.database, options.schema, options.table, options.integration_type, options.delimiter)
