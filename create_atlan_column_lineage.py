#!/usr/bin/env python3
import os

"""
A script to read table metadata from the DynamoDB template to catalog a DynamoDB table and post the information
via the Atlan API, the internal Data Governance tool.

Usage Options:
-t --table : name of the DynamoDB table with the metadata to read
-i --integration_type : Atlan source integration type ('DynamoDb', 'glue'). A venir: 'Redshift', 'Tableau'
"""

import logging
from optparse import OptionParser

import utils
from atlanapi.atlanutils import AtlanSourceFile
from atlanapi.createAsset import create_assets
from atlanapi.searchAssets import get_asset_guid_by_qualified_name
from model.Asset import Column, ColumnLineage, EntityLineage, Entity, INTEGRATION_TYPE_DYNAMO_DB, INTEGRATION_TYPE_GLUE, \
    INTEGRATION_TYPE_REDSHIFT

# TODO: add support for 'Tableau' by determining their qualified name prefix.
SUPPORTED_INTEGRATIONS = [INTEGRATION_TYPE_DYNAMO_DB, INTEGRATION_TYPE_GLUE, INTEGRATION_TYPE_REDSHIFT]

logger = logging.getLogger('main_logger')


def create_atlan_column_lineage(path, integration_type, delimiter=","):
    logger.debug("Load table definition...")
    source_data = AtlanSourceFile(path, sep=delimiter)
    table_name = utils.get_table_name(path)
    source_data.load_csv()

    # Filter dataframe to include only lineage target columns that already exist in Atlan.
    logger.info("Searching to make sure lineage columns exist")

    columns = []
    for index, row in source_data.assets_def.iterrows():
        if not row["Lineage Integration Type"] or not row["Lineage Integration Type"].lower() in SUPPORTED_INTEGRATIONS:
            if row["Lineage Integration Type"]:
                logger.warning("Lineage integration type '{}' is not supported, supported integration types for "
                               "lineage are {}".format(row["Lineage Integration Type"], SUPPORTED_INTEGRATIONS))
            continue
        if row["Lineage Type (Source / Target)"] != "":
            lineage = ColumnLineage(column=Column(schema_name=table_name,
                                                  entity_name=row["Table/Entity"],
                                                  column_name=row["Column/Attribute"],
                                                  integration_type=integration_type),
                                    lineage_type=row["Lineage Type (Source / Target)"],
                                    lineage_schema_name=row["Lineage Schema/Database"],
                                    lineage_entity_name=row["Lineage Table/Entity"],
                                    lineage_column_name=row["Lineage Column/Attribute"],
                                    lineage_integration_type=row["Lineage Integration Type"])
            asset = get_asset_guid_by_qualified_name(lineage.get_qualified_name(),
                                                     lineage.get_atlan_type_name())
            if 'attributes' in asset:
                lineage.lineage_full_qualified_name = asset['attributes']['qualifiedName']
            columns.append(lineage)
    lineage_columns_not_verified = list(filter(lambda col: not col.lineage_full_qualified_name, columns))

    if len(lineage_columns_not_verified):
        for lineage_not_verified in lineage_columns_not_verified:
            logger.warning("Cannot create lineage: column '{}/{}/{}' not found in Atlan. Target/Source columns must already exist in Atlan to create lineage.".format(lineage_not_verified.lineage_schema_name,
                                             lineage_not_verified.lineage_entity_name,
                                             lineage_not_verified.lineage_column_name))

    lineage_columns_verified = list(filter(lambda col: col.lineage_full_qualified_name, columns))

    logger.info("Creating lineage relationships for verified columns in table: {}".format(table_name))
    create_assets(lineage_columns_verified)

    logger.debug("Generate schema/table qualified names")
    entities_lineage = {}
    for lineage in lineage_columns_verified:
        entity_lineage = EntityLineage(entity=Entity(schema_name=table_name, entity_name=lineage.column.entity_name,
                                                     integration_type=integration_type),
                                       lineage_type=lineage.lineage_type,
                                       lineage_schema_name=lineage.lineage_schema_name,
                                       lineage_entity_name=lineage.lineage_entity_name,
                                       lineage_integration_type=lineage.lineage_integration_type,
                                       lineage_full_qualified_name=os.path.split(lineage.lineage_full_qualified_name)[0])
        entities_lineage[entity_lineage.entity.get_qualified_name()] = entity_lineage

    logger.info("Creating entity lineage relationships for table: {}".format(table_name))
    create_assets(entities_lineage.values())


if __name__ == '__main__':
    parser = OptionParser(usage='usage: %prog [options] arguments')
    parser.set_defaults(delimiter=",")
    parser.add_option("-p", "--path", help="Name of the DynamoDB table -> Atlan Schema")
    parser.add_option("-i", "--integration_type", choices=[INTEGRATION_TYPE_DYNAMO_DB, INTEGRATION_TYPE_GLUE],
                      help="Atlan source integration type: ('DynamoDb', 'glue') à venir: 'Redshift', 'Tableau')")
    parser.add_option("-d", "--delimiter", help="Source file csv delimiter (default = ',')")
    (options, args) = parser.parse_args()

    create_atlan_column_lineage(options.path, options.integration_type, options.delimiter)
