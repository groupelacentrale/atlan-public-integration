import logging
import os

from exception.EnvVariableNotFound import EnvVariableNotFound

logger = logging.getLogger('main_logger')

ATLAN_ATHENA_CONNECTION_ID = os.environ.get('ATLAN_ATHENA_CONNECTION_ID')
ATLAN_REDSHIFT_CONNECTION_ID = os.environ.get('ATLAN_REDSHIFT_CONNECTION_ID')


def get_atlan_athena_connection_id(asset):
    if not ATLAN_ATHENA_CONNECTION_ID:
        raise EnvVariableNotFound(asset, 'ATLAN_ATHENA_CONNECTION_ID')
    return ATLAN_ATHENA_CONNECTION_ID


def get_atlan_redshift_connection_id(asset):
    if not ATLAN_REDSHIFT_CONNECTION_ID:
        raise EnvVariableNotFound(asset, 'ATLAN_ATHENA_CONNECTION_ID')
    return ATLAN_REDSHIFT_CONNECTION_ID
