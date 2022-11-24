import json
import logging
from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest

logger = logging.getLogger('main_logger')

api_conf = create_api_config()
search_headers = {
    'Content-Type': 'application/json;charset=utf-8',
    'Authorization': 'Bearer ' + api_conf.api_token
}


def get_asset_guid_by_qualified_name(qualified_name, asset_atlan_type):
    search_url = "https://{}/api/meta/entity/uniqueAttribute/type/{}?attr%3AqualifiedName={}".format(api_conf.instance, asset_atlan_type, qualified_name)
    atlan_api_query_request_object = AtlanApiRequest("GET", search_url, search_headers, {})
    try:
        search_response = json.loads(atlan_api_query_request_object.send_atlan_request().text)
        print("-------------->"+search_response["guid"])
        return search_response["guid"]
    except Exception as e:
        logger.debug("Cannot get search result for qualified_name: '{}'".format(qualified_name))
        return {}
