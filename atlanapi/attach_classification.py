import json
import logging
from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest
from atlanapi.detach_classification import detach_classification
from atlanapi.searchAssets import get_asset_infos, is_asset_updated_by_user
from constants import CLASSIFICATION_TAGS_DICT
from model import Column, Table

logger = logging.getLogger('main_logger')

api_conf = create_api_config()

authorization = 'Bearer {}'.format(api_conf.api_token)
headers = {
    'Authorization': authorization,
    'Content-Type': 'application/json'
}


def has_classification_in_atlan(asset):
    asset_infos = get_asset_infos(asset)
    if asset_infos.get('entity', {}).get('classifications', None):
        return True
    return False


def attach_classification(assets):
    # List of assets with the classification specified in csv files
    assets_with_classification = [asset for asset in assets if
                                  (isinstance(asset, Column) or isinstance(asset, Table))
                                  and asset.classification
                                  and (has_classification_in_atlan(asset) is False
                                       or is_asset_updated_by_user(asset) is False)
                                  and asset.classification.capitalize() in [x.capitalize() for
                                                                            x in
                                                                            CLASSIFICATION_TAGS_DICT]]

    # List of assets without classification specified in csv files
    assets_without_classification = [asset.get_asset_name() for asset in assets if
                                     (isinstance(asset, Column) or isinstance(asset, Table))
                                     and (not asset.classification or asset.classification.capitalize() not in [
                                         x.capitalize() for x in CLASSIFICATION_TAGS_DICT])]

    if len(assets_without_classification) > 0:
        logger.warning('Assets {} doesn\'t have a valid classification'.format(assets_without_classification))
    if len(assets_with_classification) < 1:
        logger.debug("No classification to attach")
        return

    try:
        detach_classification(assets_with_classification)
        for asset in assets_with_classification:
            payload = json.dumps(asset.get_classification_payload())
            attach_classification_url = ('https://{}/api/meta/entity/uniqueAttribute/type/{}/classifications?attr'
                                         ':qualifiedName={}').format(
                api_conf.instance, asset.get_atlan_type_name(), asset.get_qualified_name())
            atlan_api_request_object = AtlanApiRequest("POST", attach_classification_url, headers, payload)
            response = atlan_api_request_object.send_atlan_request()
            logger.debug("Attach classification for assets : {} {} - tag {}, response {}".format(asset.get_atlan_type_name(), asset.get_asset_name(), asset.classification, response.status_code))
    except Exception as e:
        logger.warning("Error while attaching classification. Error message: %s", e)
