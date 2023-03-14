import json
import logging
from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest

logger = logging.getLogger('main_logger')

api_conf = create_api_config()
authorization = 'Bearer {}'.format(api_conf.api_token)
search_headers = {
    'Authorization': authorization,
    'Content-Type': 'application/json'
}


def get_asset_guid_by_qualified_name(qualified_name, asset_atlan_type):
    search_url = "https://{}/api/meta/entity/uniqueAttribute/type/{}?attr%3AqualifiedName={}".format(api_conf.instance, asset_atlan_type, qualified_name)
    atlan_api_query_request_object = AtlanApiRequest("GET", search_url, search_headers, {})
    try:
        search_response = json.loads(atlan_api_query_request_object.send_atlan_request().text)
        return search_response['entity']['guid']
    except Exception as e:
        logger.debug("Cannot get search result for qualified_name: '{}'".format(qualified_name))
        return {}


def get_schema_tables(qualified_name):
    search_url = "https://{}/api/meta/entity/uniqueAttribute/type/{}?attr%3AqualifiedName={}".format(api_conf.instance, "Schema", qualified_name)
    atlan_api_query_request_object = AtlanApiRequest("GET", search_url, search_headers, {})
    try:
        search_response = json.loads(atlan_api_query_request_object.send_atlan_request().text)
        return search_response['entity']['tables']
    except Exception as e:
        logger.debug("Cannot get search result for qualified_name: '{}'".format(qualified_name))
        return {}


def get_asset_by_guid(guid):
    search_url = "https://{}/api/meta/entity/guid/{}".format(api_conf.instance, guid)
    atlan_api_query_request_object = AtlanApiRequest("GET", search_url, search_headers, {})
    try:
        search_response = atlan_api_query_request_object.send_atlan_request()
        return search_response.json()
    except Exception as e:
        logger.debug("Cannot get search result for asset guid: '{}'".format(guid))
        return 0
