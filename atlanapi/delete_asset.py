import logging

from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest

logger = logging.getLogger('main_logger')

api_conf = create_api_config()
authorization = 'Bearer {}'.format(api_conf.api_token)

headers = {
    'Authorization': authorization,
    'Content-Type': 'application/json'
}


def delete_asset(asset_guid):
    delete_url = 'https://{}/api/meta/entity/bulk?guid={}&deleteType=HARD"'.format(api_conf.instance, asset_guid)
    atlan_api_query_request_object = AtlanApiRequest("DELETE", delete_url, headers, {})
    try:
        return atlan_api_query_request_object.send_atlan_request().status_code == 200
    except Exception as e:
        logger.warning("Error while deleting an asset with id '{}'".format(asset_guid), e)
        return {}
