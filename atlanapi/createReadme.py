import json

from ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest
from atlanapi.searchAssets import get_asset_guid_by_qualified_name
from model.Asset import ColumnLineage, EntityLineage


def create_readme(asset):
    if isinstance(asset, ColumnLineage) or isinstance(asset, EntityLineage) or not asset.readme:
        return
    asset_qualified_name = asset.get_qualified_name()
    asset_info = get_asset_guid_by_qualified_name(asset_qualified_name, asset.get_atlan_type_name())
    asset_guid = asset_info['guid']
    column_info = {
        "typeName": "AtlanFile",
        "attributes": {
            "mimeType": "text/markdown",
            "name": "{}-readme.md".format(asset_guid),
            "content": asset.readme,
            "qualifiedName": "{}/files/{}-readme.md".format(asset_qualified_name, asset_guid),
        },
        "relationshipAttributes": {
            "asset": {
                "uniqueAttributes": {
                    "qualifiedName": asset_qualified_name
                },
                "typeName": asset.get_atlan_type_name()
            }
        }
    }
    readme_payload = json.dumps({"entity": column_info})

    api_conf = create_api_config()
    readme_post_url = 'https://{}/api/metadata/atlas/tenants/default/entity'.format(api_conf.instance)
    headers = {
        'Content-Type': 'application/json;charset=utf-8',
        'APIKEY': api_conf.api_key
    }
    atlan_api_readme_request_object = AtlanApiRequest("POST", readme_post_url, headers, readme_payload)
    try:
        atlan_api_readme_request_object.send_atlan_request()
        print('Readme added successfully to {}'.format(asset.get_asset_name()))
    except Exception as e:
        print('Error while adding readme to the {}\nReason: {}'.format(asset.get_asset_name(), e))


