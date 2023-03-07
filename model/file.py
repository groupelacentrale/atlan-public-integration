import logging
import os

from exception.EnvVariableNotFound import EnvVariableNotFound
from constants import INTEGRATION_TYPE_DYNAMO_DB, INTEGRATION_TYPE_ATHENA, \
    INTEGRATION_TYPE_REDSHIFT, DYNAMO_DB_DATABASE_NAME, ATHENA_DATABASE_NAME, REDSHIFT_DATABASE_NAME

logger = logging.getLogger('main_logger')

ATLAN_ATHENA_CONNECTION_ID = os.environ.get('ATLAN_ATHENA_CONNECTION_ID')
ATLAN_REDSHIFT_CONNECTION_ID = os.environ.get('ATLAN_REDSHIFT_CONNECTION_ID')
ATLAN_TEAM = os.environ.get('ATLAN_TEAM')


def get_atlan_athena_connection_id(asset):
    if not ATLAN_ATHENA_CONNECTION_ID:
        raise EnvVariableNotFound(asset, 'ATLAN_ATHENA_CONNECTION_ID')
    return ATLAN_ATHENA_CONNECTION_ID


def get_atlan_redshift_connection_id(asset):
    if not ATLAN_REDSHIFT_CONNECTION_ID:
        raise EnvVariableNotFound(asset, 'ATLAN_ATHENA_CONNECTION_ID')
    return ATLAN_REDSHIFT_CONNECTION_ID


def get_database(integration_type):
    if integration_type == INTEGRATION_TYPE_DYNAMO_DB:
        return DYNAMO_DB_DATABASE_NAME
    if integration_type == INTEGRATION_TYPE_ATHENA:
        return ATHENA_DATABASE_NAME
    if integration_type == INTEGRATION_TYPE_REDSHIFT:
        return REDSHIFT_DATABASE_NAME
    return ATLAN_REDSHIFT_CONNECTION_ID

def get_atlan_team():
    if not ATLAN_TEAM:
        raise EnvVariableNotFound('')
    return ATLAN_TEAM
