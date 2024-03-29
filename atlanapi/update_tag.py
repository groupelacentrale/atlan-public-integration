import json
import logging

from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest
from atlanapi.searchAssets import get_asset_guid_by_qualified_name
from constants import CRITICALITY_LEVEL
from model import ColumnLineage, TableLineage, get_atlan_team, Table

logger = logging.getLogger('main_logger')

api_conf = create_api_config()
authorization = 'Bearer {}'.format(api_conf.api_token)
headers = {
    'Authorization': authorization,
    'Content-Type': 'application/json'
}


def update_aws_team_tag(asset):
    if asset is None or isinstance(asset, ColumnLineage) or isinstance(asset, TableLineage):
        return
    try:
        team_tag = get_atlan_team()
        logger.info('Update AWS team tag for asset: {}, team {}'.format(asset.get_asset_name(), team_tag))
        asset_qualified_name = asset.get_qualified_name()
        asset_guid = get_asset_guid_by_qualified_name(asset_qualified_name, asset.get_atlan_type_name())
        payload = {
            "AWS TAG": {
                "Team": [
                    team_tag
                ]
            }
        }
        update_tag_url = 'https://{}/api/meta/entity/guid/{}/businessmetadata/displayName'.format(api_conf.instance,
                                                                                                  asset_guid)
        request_object = AtlanApiRequest("POST", update_tag_url, headers, json.dumps(payload))
        request_object.send_atlan_request()
    except Exception as e:
        logger.warning('Error while updating AWS tag team to the {}\nReason: {}'.format(asset.get_asset_name(), e))


def update_level_criticality(asset):
    if asset is None or not isinstance(asset, Table):
        return
    if asset.get_level_criticality() is None:
        logger.warning('Criticality is not provided for asset {}'.format(asset.get_asset_name()))
        return
    criticality = asset.get_level_criticality().capitalize()
    if asset.get_level_criticality().capitalize() not in CRITICALITY_LEVEL:
        logger.warning('Criticality value "{}" is not support asset {}'.format(criticality, asset.get_asset_name()))
        return
    try:
        logger.info('Update level Criticality: {} for asset {}'.format(criticality, asset.get_asset_name()))
        asset_qualified_name = asset.get_qualified_name()
        asset_guid = get_asset_guid_by_qualified_name(asset_qualified_name, asset.get_atlan_type_name())
        payload = {
            "Criticality": {
                "Level": criticality
            }
        }
        update_tag_url = 'https://{}/api/meta/entity/guid/{}/businessmetadata/displayName'.format(api_conf.instance, asset_guid)
        request_object = AtlanApiRequest("POST", update_tag_url, headers, json.dumps(payload))
        request_object.send_atlan_request()
    except Exception as e:
        logger.warning(
            'Error while updating level criticality to the asset {}\nReasons: {}'.format(asset.get_asset_name(), e))
