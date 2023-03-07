import json
import logging

from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest
from atlanapi.searchAssets import get_asset_guid_by_qualified_name
from model import ColumnLineage, TableLineage

logger = logging.getLogger('main_logger')

api_conf = create_api_config()
authorization = 'Bearer {}'.format(api_conf.api_token)
headers = {
    'Authorization': authorization,
    'Content-Type': 'application/json'
}


def update_aws_team_tag(asset):
    logger.info('Update AWS team tag for asset: {}'.format(asset.get_asset_name()))
    asset_qualified_name = asset.get_qualified_name()
    asset_guid = get_asset_guid_by_qualified_name(asset_qualified_name, asset.get_atlan_type_name())
    if not asset_guid and (isinstance(asset, ColumnLineage) or isinstance(asset, TableLineage)):
        return
    try:
        payload = {
            "AWS TAG": {
                "Team": [
                    "data"
                ]
            }
        }
        update_tag_url = 'https://{}/api/meta/types/typedefs#customMetadata'.format(api_conf.instance)
        request_object = AtlanApiRequest("POST", update_tag_url, headers, json.dumps(payload))
        request_object.send_atlan_request()
    except Exception as e:
        logger.warning('Error while updating AWS tag team to the {}\nReason: {}'.format(asset.get_asset_name(), e))
