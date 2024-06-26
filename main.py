import logging

import utils
import atlanapi.ApiConfig

from create_atlan_column_lineage import create_atlan_column_lineage
from create_atlan_columns import create_atlan_columns
from create_atlan_schema_and_entities import create_atlan_schema_and_entities
from model import ATLAN_ATHENA_CONNECTION_ID, ATLAN_REDSHIFT_CONNECTION_ID
from validate_atlan_source_file import validate_atlan_source_file as validate

if __name__ == '__main__':
    # setting up logger
    utils.setup_logger('main_logger')
    logger = logging.getLogger('main_logger')
    logger.info("******* Starting the job ...")

    #Check Conf
    api_conf = atlanapi.ApiConfig.create_api_config()

    if not ATLAN_ATHENA_CONNECTION_ID:
        logger.warning('ATLAN_ATHENA_CONNECTION_ID is not defined in env variables, refer to '
                       'https://github.com/groupelacentrale/data-atlan-sample/blob/prod/.github/workflows/atlan'
                       '-integration-action.yml to complete your github action correctly')
    if not ATLAN_REDSHIFT_CONNECTION_ID:
        logger.warning('ATLAN_REDSHIFT_CONNECTION_ID is not defined in env variables, refer to '
                       'https://github.com/groupelacentrale/data-atlan-sample/blob/prod/.github/workflows/atlan'
                       '-integration-action.yml to complete your github action correctly')

    logger.info("******* Starting create schemas and tables...")
    assets_info, tables = create_atlan_schema_and_entities(utils.get_manifest_path())
    logger.debug("******* End create schemas and tables...")

    logger.info("******* Starting validate files")
    for asset_info in assets_info:
        logger.info("--- Validating the csv file of the table: '{}' ---"
                    .format(utils.get_csv_file_name(asset_info['schema_name'],
                                                    asset_info['entity_name'],
                                                    asset_info['integration_type'])))
        validate(asset_info['schema_name'], asset_info['entity_name'], asset_info['integration_type'])
    logger.debug("******* End validate files")

    logger.info("******* Starting create columns")
    for index, asset_info in enumerate(assets_info):
        logger.info("--- Creating columns for table: '{}' ---"
                    .format(utils.get_csv_file_name(asset_info['schema_name'],
                                                    asset_info['entity_name'],
                                                    asset_info['integration_type'])))
        create_atlan_columns(asset_info['database_name'],
                             asset_info['schema_name'],
                             asset_info['entity_name'],
                             asset_info['integration_type'],
                             table=tables[index])
    logger.debug("******* End of create columns")

    logger.info("******* Starting create lineage")
    for asset_info in assets_info:
        logger.info("--- Creating lineage for table: '{}' ---"
                    .format(utils.get_csv_file_name(asset_info['schema_name'],
                                                    asset_info['entity_name'],
                                                    asset_info['integration_type'])))
        create_atlan_column_lineage(asset_info['database_name'],
                                    asset_info['schema_name'],
                                    asset_info['entity_name'],
                                    asset_info['integration_type'])
    logger.debug("******* End of create lineage")

    logger.info("******* The job finished with success")
