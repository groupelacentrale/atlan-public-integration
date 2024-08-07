import json
import logging
from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest
from constants import SERVICE_ACC_API_NAME

logger = logging.getLogger('main_logger')

api_conf = create_api_config()
authorization = 'Bearer {}'.format(api_conf.api_token)
search_headers = {
    'Authorization': authorization,
    'Content-Type': 'application/json'
}


def is_asset_updated_by_service_acc_api(asset):
    asset_infos = get_asset_infos(asset)
    updated_by = asset_infos.get('entity', {}).get('updatedBy', None)
    if updated_by in SERVICE_ACC_API_NAME:
        logger.info('Asset {} has been updated by service account API'.format(asset))
        return True
    return False


def get_asset_guid_by_qualified_name(qualified_name, asset_atlan_type):
    search_url = ("https://{}/api/meta/entity/uniqueAttribute/type/{}?attr%3AqualifiedName={"
                  "}&ignoreRelationships=true&minExtInfo=true").format(api_conf.instance,
                                                                       asset_atlan_type,
                                                                       qualified_name)
    atlan_api_query_request_object = AtlanApiRequest("GET", search_url, search_headers, {})
    try:
        search_response = json.loads(atlan_api_query_request_object.send_atlan_request().text)
        return search_response['entity']['guid']
    except Exception as e:
        logger.debug("Cannot get search result for qualified_name: '{}', error {}".format(qualified_name, e))
        return {}


def get_schema_tables(qualified_name):
    search_url = ("https://{}/api/meta/entity/uniqueAttribute/type/{}?attr%3AqualifiedName={"
                  "}&ignoreRelationships=true&minExtInfo=true").format(api_conf.instance,
                                                                       "Schema",
                                                                       qualified_name)
    atlan_api_query_request_object = AtlanApiRequest("GET", search_url, search_headers, {})
    try:
        search_response = json.loads(atlan_api_query_request_object.send_atlan_request().text)
        return search_response['entity']['relationshipAttributes']['tables']
    except Exception as e:
        logger.debug("Cannot get search result for qualified_name: '{}', error {}".format(qualified_name, e))
        return {}


def get_asset_by_guid(guid):
    search_url = "https://{}/api/meta/entity/guid/{}?ignoreRelationships=true&minExtInfo=true".format(api_conf.instance,
                                                                                                      guid)
    atlan_api_query_request_object = AtlanApiRequest("GET", search_url, search_headers, {})
    try:
        search_response = json.loads(atlan_api_query_request_object.send_atlan_request().text)
        return search_response
    except Exception as e:
        logger.debug("Cannot get search result for asset guid: '{}', error {}".format(guid, e))
        return {}


def get_asset_infos(asset):
    try:
        asset_guid = get_asset_guid_by_qualified_name(asset.get_qualified_name(), asset.get_atlan_type_name())
        logger.debug("Asset : {}, guid: {}, type: {}".format(asset.get_qualified_name(),
                                                             asset.get_atlan_type_name(),
                                                             asset_guid))
        asset_infos = get_asset_by_guid(asset_guid)

        return asset_infos
    except Exception as e:
        logger.debug('Cannot get asset infos for asset guid: {}, error {}'.format(asset_guid, e))
        return {}
