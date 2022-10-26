import logging
import json

from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest

api_conf = create_api_config()
search_headers = {
    'Content-Type': 'application/json;charset=utf-8',
    'Authorization': api_conf.api_token
}

logger = logging.getLogger('main_logger')


def delete_asset(asset_guid):
    search_url = "https://{}/api/metadata/atlas/tenants/default/entity/guid/{}?deleteType=HARD".format(
        api_conf.instance, asset_guid)
    atlan_api_query_request_object = AtlanApiRequest("DELETE", search_url, search_headers, {})
    try:
        json.loads(atlan_api_query_request_object.send_atlan_request().text)
    except Exception as e:
        logger.warning("Error while deleting an asset with id '{}'".format(asset_guid), e)
        return {}
