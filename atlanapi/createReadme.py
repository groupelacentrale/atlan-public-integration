import json

from ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest
from atlanapi.searchAssets import get_asset_guid_by_qualified_name
from utils import get_column_qualified_name

api_conf = create_api_config()
readme_post_url = 'https://{}/api/metadata/atlas/tenants/default/entity'.format(api_conf.instance)
headers = {
    'Content-Type': 'application/json;charset=utf-8',
    'APIKEY': api_conf.api_key
}


def create_readme(table_name, entity, asset_name, readme_content):
    asset_qualified_name = get_column_qualified_name(table_name, entity, asset_name)
    asset_info = get_asset_guid_by_qualified_name(asset_qualified_name)
    asset_guid = asset_info['guid']
    column_info = {
        "typeName": "AtlanFile",
        "attributes": {
            "mimeType": "text/markdown",
            "name": "{}-readme.md".format(asset_guid),
            "content": readme_content,
            "qualifiedName": "{}/files/{}-readme.md".format(asset_qualified_name, asset_guid),
        },
        "relationshipAttributes": {
            "asset": {
                "uniqueAttributes": {
                    "qualifiedName": asset_qualified_name
                },
                "typeName": "AtlanColumn"
            }
        }
    }
    readme_payload = json.dumps({"entity": column_info})

    atlan_api_readme_request_object = AtlanApiRequest("POST", readme_post_url, headers, readme_payload)
    try:
        atlan_api_readme_request_object.send_atlan_request()
        print('Readme added successfully to {} in table {}'.format(asset_name, table_name))
    except Exception as e:
        print('Error while adding readme to the {}\nReason: {}'.format(asset_name, e))

