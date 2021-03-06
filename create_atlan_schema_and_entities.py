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
from atlanapi.createAsset import create_assets
from model.Asset import Schema, Entity

logger = logging.getLogger('main_logger')


def create_atlan_schema_and_entities(path_to_manifest, sep=","):

    logger.info("Loading manifest...")
    source_data = AtlanSourceFile(path_to_manifest, sep)
    source_data.load_csv()

    assets = []
    assets_info = []
    for index, row in source_data.assets_def.iterrows():
        if row['Table/Entity']:
            entity = Entity(entity_name=row['Table/Entity'],
                            database_name=row['Database'],
                            schema_name=row['Schema'],
                            description=row['Summary (Description)'],
                            readme=row['Readme'],
                            term=row['Term'],
                            glossary=row['Glossary'],
                            integration_type=row['Integration Type'])
            assets.append(entity)
            # Create schema from entity row in case schema row is missing
            schema = Schema(database_name=row['Database'],
                            schema_name=row['Schema'],
                            integration_type=row['Integration Type'])
            assets.append(schema)
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
            assets.append(schema)

    create_assets(assets)

    return assets_info


if __name__ == '__main__':
    # TODO : Add option for integration type to generalize the script
    # NOTE: -t --table should be equal to the title of the source file csv (cf le Mod??le Physique DynamoDb NOE)
    parser = OptionParser(usage='usage: %prog [options] arguments')
    parser.add_option("-p", "--path", help="path of the manifest file")
    parser.add_option("-d", "--delimiter", help="Source file csv delimiter (default = ',')")
    (options, args) = parser.parse_args()

    create_atlan_schema_and_entities(options.path, options.delimiter)
