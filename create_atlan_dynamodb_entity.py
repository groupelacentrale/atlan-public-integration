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


def create_atlan_schema_and_entity(path_to_manifest, sep=","):

    logger.info("Loading manifest...")
    source_data = AtlanSourceFile(path_to_manifest, sep)
    source_data.load_csv()

    assets = []
    all_schemas = {}
    for index, row in source_data.assets_def.iterrows():
        if row['Table/Entity']:
            entity = Entity(entity_name=row['Table/Entity'],
                            schema_name=row['Schema'],
                            description=row['Summary (Description)'],
                            readme=row['Readme'],
                            term=row['Term'],
                            glossary=row['Glossary'],
                            integration_type=row['Integration Type'])
            assets.append(entity)
            # Create schema from entity row in case schema row is missing
            schema = Schema(schema_name=row['Schema'], integration_type=row['Integration Type'])
            assets.append(schema)
        else:
            schema = Schema(schema_name=row['Schema'],
                            description=row['Summary (Description)'],
                            readme=row['Readme'],
                            term=row['Term'],
                            glossary=row['Glossary'],
                            integration_type=row['Integration Type'])
            assets.append(schema)

        all_schemas[row['Schema']] = row['Integration Type']

    create_assets(assets)

    return all_schemas


if __name__ == '__main__':
    # TODO : Add option for integration type to generalize the script
    # NOTE: -t --table should be equal to the title of the source file csv (cf le Modèle Physique DynamoDb NOE)
    parser = OptionParser(usage='usage: %prog [options] arguments')
    parser.add_option("-p", "--path", help="path of the manifest file")
    parser.add_option("-d", "--delimiter", help="Source file csv delimiter (default = ',')")
    (options, args) = parser.parse_args()

    create_atlan_schema_and_entity(options.path, options.delimiter)
