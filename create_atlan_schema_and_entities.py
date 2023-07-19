#!/usr/bin/env python3

"""
A script to read table metadata from a user-specified AWS Catalog database and table, enrich it with additional
metadata, and write to an output .csv file that can be imported into Atlan, the internal Data Governance tool.

Usage Options:
-p --path : path to the manifest file
-d --delimiter : Source file csv delimiter (default = ',')
"""

import logging
from optparse import OptionParser

from atlanapi.atlanutils import AtlanSourceFile
from atlanapi.createAsset import create_assets, create_asset_database
from model import Schema, Table
from constants import INTEGRATION_TYPE_DYNAMO_DB, INTEGRATION_TYPE_REDSHIFT

logger = logging.getLogger('main_logger')

"""
    TODO: Voir si dans la suppression des colonnes, si l'asset Table dans le manifest est supprimé, les colonnes existantes sont-elles supprimées
"""


def create_atlan_schema_and_entities(path_to_manifest, sep=","):
    logger.info("Loading manifest...")
    source_data = AtlanSourceFile(path_to_manifest, sep)
    source_data.load_csv()

    tables = []
    schemas = []
    assets_info = []
    for index, row in source_data.assets_def.iterrows():
        if row['Table/Entity']:
            table = Table(entity_name=row['Table/Entity'],
                          database_name=row['Database'],
                          schema_name=row['Schema'],
                          description=row['Summary (Description)'],
                          readme=row['Readme'],
                          term=row['Term'],
                          glossary=row['Glossary'],
                          classification=row['Classification'] if 'Classification' in row.keys() else None,
                          criticality=row['Criticality'] if 'Criticality' in row.keys() else None,
                          integration_type=row['Integration Type'])
            tables.append(table)
            # Create schema from entity row in case schema row is missing
            schema = Schema(database_name=row['Database'],
                            schema_name=row['Schema'],
                            integration_type=row['Integration Type'])
            schemas.append(schema)
            assets_info.append({
                'database_name': row['Database'],
                'schema_name': row['Schema'],
                'entity_name': row['Table/Entity'],
                'integration_type': row['Integration Type']
            })
        else:
            schema = Schema(database_name=row['Database'],
                            schema_name=row['Schema'],
                            description=row['Summary (Description)'],
                            readme=row['Readme'],
                            term=row['Term'],
                            glossary=row['Glossary'],
                            integration_type=row['Integration Type'])
            schemas.append(schema)

    manual_schemas = list(filter(lambda sch: sch.is_manual_integration, schemas))
    manual_tables = list(filter(lambda tab: tab.is_manual_integration, tables))

    crawl_schemas = list(filter(lambda sch: not sch.is_manual_integration, schemas))
    crawl_tables = list(filter(lambda tab: not tab.is_manual_integration, tables))

    # Create asset's database if does not exist
    for table in manual_tables:
        create_asset_database(table)

    # Create assets for DynamoDB integration only
    create_assets(manual_schemas, "createSchemas")
    create_assets(manual_tables, "createTables")

    # Update assets for Redshift/Glue/Athena
    create_assets(crawl_schemas, "createSchemas", INTEGRATION_TYPE_REDSHIFT)
    create_assets(crawl_tables, "createTables", INTEGRATION_TYPE_REDSHIFT)

    return assets_info, tables


if __name__ == '__main__':
    # TODO : Add option for integration type to generalize the script
    # NOTE: -t --table should be equal to the title of the source file csv (cf le Modèle Physique DynamoDb NOE)
    parser = OptionParser(usage='usage: %prog [options] arguments')
    parser.add_option("-p", "--path", help="path of the manifest file")
    parser.add_option("-d", "--delimiter", help="Source file csv delimiter (default = ',')")
    (options, args) = parser.parse_args()

    create_atlan_schema_and_entities(options.path, options.delimiter)
