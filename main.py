import sys
import os

import utils
from atlanapi.atlanutils import AtlanSourceFile
from validate_atlan_source_file import validate_atlan_source_file as validate
from create_atlan_dynamodb_entity import create_atlan_dynamodb_entity as create_table
from create_atlan_columns import create_atlan_columns
from create_atlan_column_lineage import create_atlan_column_lineage
import logging

if __name__ == '__main__':
    # setting up logger
    utils.setup_logger('main_logger')
    logger = logging.getLogger('main_logger')
    logger.info("starting the job ...")

    # load manifest
    source_data = AtlanSourceFile(utils.get_manifest_path(), sep=",")
    source_data.load_csv()

    for index, row in source_data.assets_def.iterrows():
        table = row['Table']
        path = utils.get_path(table)
        entity = row['Entity']
        description = row['Description']
        logger.info("Validating the csv file of the table {}".format(table))
        validate(path)
        logger.info("Creating table={}, entity={}, description={}".format(table, entity, description))
        create_table(table, entity, description)

    tables_set = set()
    for index, row in source_data.assets_def.iterrows():
        table = row['Table']
        path = utils.get_path(table)
        if table not in tables_set:
            logger.info("Creating columns for table={}".format(table))
            create_atlan_columns(path)
            tables_set.add(table)

    tables_set_l = set()
    for index, row in source_data.assets_def.iterrows():
        table = row['Table']
        path = utils.get_path(table)
        if table not in tables_set_l:
            logger.info("Creating lineage for table={}".format(table))
            create_atlan_column_lineage(path, "DynamoDb")
            tables_set_l.add(table)

    logger.info("the job finished with success")
