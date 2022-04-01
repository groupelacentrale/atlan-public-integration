import logging

import utils
from create_atlan_column_lineage import create_atlan_column_lineage
from create_atlan_columns import create_atlan_columns
from create_atlan_schema_and_entities import create_atlan_schema_and_entities
from model.Asset import ATLAN_PROD_AWS_ACCOUNT_ID, ATLAN_REDSHIFT_SERVER_URL
from validate_atlan_source_file import validate_atlan_source_file as validate


if __name__ == '__main__':
    # setting up logger
    utils.setup_logger('main_logger')
    logger = logging.getLogger('main_logger')
    logger.info("******* Starting the job ...")

    if not ATLAN_PROD_AWS_ACCOUNT_ID:
        logger.warning('ATLAN_PROD_AWS_ACCOUNT_ID is not defined in env variables, refer to '
                       'https://github.com/groupelacentrale/data-atlan-sample/blob/prod/.github/workflows/atlan'
                       '-integration-action.yml to complete your github action correctly')
    if not ATLAN_REDSHIFT_SERVER_URL:
        logger.warning('ATLAN_REDSHIFT_SERVER_URL is not defined in env variables, refer to '
                       'https://github.com/groupelacentrale/data-atlan-sample/blob/prod/.github/workflows/atlan'
                       '-integration-action.yml to complete your github action correctly')

    logger.info("******* Creating Schemas and entities...")
    assets_info = create_atlan_schema_and_entities(utils.get_manifest_path())

    logger.info("******* Validating files")
    for asset_info in assets_info:
        logger.info("--- Validating the csv file of the table: '{}' ---"
                    .format(utils.get_csv_file_name(asset_info['schema_name'],
                                                    asset_info['entity_name'],
                                                    asset_info['integration_type'])))
        validate(asset_info['schema_name'], asset_info['entity_name'], asset_info['integration_type'])

    logger.info("******* Creating columns")
    for asset_info in assets_info:
        logger.info("--- Creating columns for table: '{}' ---"
                    .format(utils.get_csv_file_name(asset_info['schema_name'],
                                                    asset_info['entity_name'],
                                                    asset_info['integration_type'])))
        create_atlan_columns(asset_info['database_name'],
                             asset_info['schema_name'],
                             asset_info['entity_name'],
                             asset_info['integration_type'])

    logger.info("******* Creating lineage")
    for asset_info in assets_info:
        logger.info("--- Creating lineage for table: '{}' ---"
                    .format(utils.get_csv_file_name(asset_info['schema_name'],
                                                    asset_info['entity_name'],
                                                    asset_info['integration_type'])))
        create_atlan_column_lineage(asset_info['database_name'],
                                    asset_info['schema_name'],
                                    asset_info['entity_name'],
                                    asset_info['integration_type'])

    logger.info("******* The job finished with success")
