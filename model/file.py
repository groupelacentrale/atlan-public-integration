import logging
import os

from exception.EnvVariableNotFound import EnvVariableNotFound

logger = logging.getLogger('main_logger')

ATLAN_PROD_AWS_ACCOUNT_ID = os.environ.get('ATLAN_PROD_AWS_ACCOUNT_ID')
ATLAN_REDSHIFT_SERVER_URL = os.environ.get('ATLAN_REDSHIFT_SERVER_URL')


def get_atlan_athena_unique_id(asset):
    # if not 'ATLAN_PROD_AWS_ACCOUNT_ID':
    #     raise EnvVariableNotFound(asset, 'ATLAN_PROD_AWS_ACCOUNT_ID')
    # return ATLAN_PROD_AWS_ACCOUNT_ID
    return '1663675805'


def get_atlan_redshift_server_url(asset):
    # if not ATLAN_REDSHIFT_SERVER_URL:
    #     raise EnvVariableNotFound(asset, 'ATLAN_REDSHIFT_SERVER_URL')
    # return ATLAN_REDSHIFT_SERVER_URL
    return '1663689527'
