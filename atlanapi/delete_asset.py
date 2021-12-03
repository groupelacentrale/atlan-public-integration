import json

from ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest

api_conf = create_api_config()
search_headers = {
    'Content-Type': 'application/json;charset=utf-8',
    'APIKEY': api_conf.api_key
}


def delete_asset(asset_guid):
    search_url = "https://{}/api/metadata/atlas/tenants/default/entity/guid/{}?deleteType=HARD".format(api_conf.instance, asset_guid)
    atlan_api_query_request_object = AtlanApiRequest("DELETE", search_url, search_headers, {})
    try:
        columns_response = json.loads(atlan_api_query_request_object.send_atlan_request().text)
        print(columns_response)
    except:
        print("Error while deleting an asset withid '{}'".format(asset_guid))
        return {}

