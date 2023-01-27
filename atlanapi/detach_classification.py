import json
import logging

from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest
from atlanapi.searchAssets import get_asset_guid_by_qualified_name

logger = logging.getLogger('main_logger')

api_conf = create_api_config()

authorization = 'Bearer {}'.format(api_conf.api_token)
headers = {
    'Authorization': authorization,
    'Content-Type': 'application/json'
}

def detach_classification(asset):
    data = {
        "guidHeaderMap": {
            get_asset_guid_by_qualified_name(asset.get_qualified_name(), asset.get_atlan_type_name()): {
                "guid": get_asset_guid_by_qualified_name(asset.get_qualified_name(), asset.get_atlan_type_name()),
                "typeName": asset.get_atlan_type_name(),
                "attributes": {
                    "name": asset.get_asset_name(),
                    "qualifiedName": asset.get_qualified_name(),
                },
                "classifications": []
            }
        }
    }
    payload = json.dumps(data)
    url = 'https://{}/api/meta/entity/bulk/setClassifications'.format(api_conf.instance)
    request_object = AtlanApiRequest("POST", url, headers, payload)
    request_object.send_atlan_request()