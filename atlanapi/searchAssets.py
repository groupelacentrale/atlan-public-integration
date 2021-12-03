import json

from ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest

api_conf = create_api_config()
search_url = "https://{}/api/metadata/atlas/tenants/default/search/basic".format(api_conf.instance)
search_headers = {
    'Content-Type': 'application/json;charset=utf-8',
    'APIKEY': api_conf.api_key
}

def get_asset_guid_by_qualified_name(qualified_name):
    query = json.dumps({
        "searchType": "BASIC",
        "typeName": "AtlanAsset",
        "excludeDeletedEntities": True,
        "includeClassificationAttributes": False,
        "includeSubClassifications": False,
        "includeSubTypes": True,
        "limit": 300,
        "offset": 0,
        "attributes": [],
        "minScore": 100,
        "query": "",
        "entityFilters": {
            "condition": "AND",
            "criterion": [
                {
                    "condition": "OR",
                    "criterion": [
                        {
                            "attributeName": "qualifiedName",
                            "attributeValue": qualified_name,
                            "operator": "eq"
                        }
                    ]
                }
            ]
        }
    })
    atlan_api_query_request_object = AtlanApiRequest("POST", search_url, search_headers, query)
    try:
        search_response = json.loads(atlan_api_query_request_object.send_atlan_request().text)
        return search_response["entities"][0]
    except:
        print("Cannot get search result for qualified_name '{}'".format(qualified_name))
        return {}

