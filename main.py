import logging

import utils
from create_atlan_column_lineage import create_atlan_column_lineage
from create_atlan_columns import create_atlan_columns
from create_atlan_dynamodb_entity import create_atlan_schema_and_entity
from model.Asset import INTEGRATION_TYPE_DYNAMO_DB
from validate_atlan_source_file import validate_atlan_source_file as validate

if __name__ == '__main__':
    # setting up logger
    utils.setup_logger('main_logger')
    logger = logging.getLogger('main_logger')
    logger.info("******* Starting the job ...")

    logger.info("******* Creating Schemas and entities...")
    all_schemas = create_atlan_schema_and_entity(utils.get_manifest_path())

    logger.info("******* Validating files")
    for schema in all_schemas.keys():
        logger.info("--- Validating the csv file of the table: '{}' ---".format(schema))
        validate(utils.get_path(all_schemas[schema], schema), all_schemas[schema])

    logger.info("******* Creating columns")
    for schema in all_schemas.keys():
        logger.info("--- Creating columns for table: '{}' ---".format(schema))
        create_atlan_columns(utils.get_path(all_schemas[schema], schema))

    logger.info("******* Creating lineage")
    for schema in all_schemas.keys():
        logger.info("--- Creating lineage for table: '{}' ---".format(schema))
        create_atlan_column_lineage(utils.get_path(all_schemas[schema], schema), INTEGRATION_TYPE_DYNAMO_DB)

    logger.info("******* The job finished with success")
