import logging
import json

from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest

logger = logging.getLogger('main_logger')

api_conf = create_api_config()
search_url = "https://{}/api/metadata/atlas/tenants/default/search/basic".format(api_conf.instance)
search_headers = {
    'Content-Type': 'application/json;charset=utf-8',
    'Authorization': api_conf.api_token
}


def get_asset_guid_by_qualified_name(qualified_name, asset_atlan_type, operator="eq"):
    query = json.dumps({
        "searchType": "BASIC",
        "typeName": "AtlanAsset",
        "excludeDeletedEntities": True,
        "includeClassificationAttributes": False,
        "includeSubClassifications": False,
        "includeSubTypes": True,
        "attributes": [],
        "limit": 20,
        "offset": 0,
        "query": "",
        "entityFilters": {
            "condition": "AND",
            "criterion": [
                {
                    "attributeName": "typeName",
                    "attributeValue": asset_atlan_type,
                    "operator": "eq"
                },
                {
                    "attributeName": "qualifiedName",
                    "attributeValue": qualified_name,
                    "operator": operator
                }
            ]
        }
    })
    atlan_api_query_request_object = AtlanApiRequest("POST", search_url, search_headers, query)

    try:
        search_response = json.loads(atlan_api_query_request_object.send_atlan_request().text)
        return search_response["entities"][0]
    except Exception as e:
        logger.debug("Cannot get search result for qualified_name: '{}'".format(qualified_name))
        return {}
