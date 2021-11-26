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
    logger.info("******* starting the job ...")

    # load manifest
    source_data = AtlanSourceFile(utils.get_manifest_path(), sep=",")
    source_data.load_csv()

    tables_set = set()
    logger.info("******* Creating entities")
    for index, row in source_data.assets_def.iterrows():
        table = row['Table']
        path = utils.get_path(table)
        entity = row['Entity']
        description = row['Description']
        logger.info("Creating table={}, entity={}, description={}".format(table, entity, description))
        create_table(table, entity, description)
        tables_set.add(table)

    logger.info("******* Validating files")
    for table in tables_set:
        path = utils.get_path(table)
        logger.info("Validating the csv file of the table {}".format(table))
        validate(path, 'DynamoDb')

    logger.info("******* Creating columns")
    for table in tables_set:
        path = utils.get_path(table)
        logger.info("Creating columns for table={}".format(table))
        create_atlan_columns(path)

    logger.info("******* Creating lineage")
    for table in tables_set:
        path = utils.get_path(table)
        logger.info("Creating lineage for table={}".format(table))
        create_atlan_column_lineage(path, "DynamoDb")

    logger.info("******* The job finished with success")
