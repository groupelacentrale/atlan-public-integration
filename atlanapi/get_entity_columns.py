import json

from ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest

api_conf = create_api_config()
search_headers = {
    'Content-Type': 'application/json;charset=utf-8',
    'APIKEY': api_conf.api_key
}


def get_entity_columns(entity_guid):
    search_url = "https://{}/api/metadata/atlas/tenants/default/search/relationship?guid={}&relation=columns".format(api_conf.instance,entity_guid)
    atlan_api_query_request_object = AtlanApiRequest("GET", search_url, search_headers, {})
    try:
        columns_response = json.loads(atlan_api_query_request_object.send_atlan_request().text)
        columns = columns_response["entities"]
        if columns:
            return {column['displayText']: column['guid'] for column in columns}
        print("entity '{}' do not have any columns".format(entity_guid))
        return {}
    except:
        print("Error while fetching columns for entity id '{}'".format(entity_guid))
        return {}

