import json
import logging
from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest
from atlanapi.detach_classification import detach_classification
from atlanapi.searchAssets import get_asset_infos, is_asset_updated_by_service_acc_api
from constants import CLASSIFICATION
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
                                  and (not has_classification_in_atlan(asset)
                                       or is_asset_updated_by_service_acc_api(asset))
                                  and asset.classification.capitalize() in [x.capitalize() for
                                                                            x in
                                                                            CLASSIFICATION]]

    # List of assets without classification specified in csv files
    assets_without_classification = [asset.get_asset_name() for asset in assets if
                                     (isinstance(asset, Column) or isinstance(asset, Table))
                                     and (not asset.classification or asset.classification.capitalize() not in [
                                         x.capitalize() for x in CLASSIFICATION])]

    if len(assets_without_classification) > 0:
        logger.warning('Assets {} doesn\'t have a valid classification'.format(assets_without_classification))
    if len(assets_with_classification):
        logger.info("Asset with classification : {}".format(assets_with_classification))
    else:
        logger.info("No classification to attach")
        return

    try:
        detach_classification(assets_with_classification)
        for asset in assets_with_classification:
            payload = json.dumps(list(asset.get_classification_payload()))
            attach_classification_url = 'https://{}/api/meta/entity/bulk/classification/displayName'.format(
                api_conf.instance)
            atlan_api_request_object = AtlanApiRequest("POST", attach_classification_url, headers, payload)
            response = atlan_api_request_object.send_atlan_request()
            logger.info("Attach classification for assets : {} - {}".format(asset.get_asset_name(), response.status_code))
    except Exception as e:
        logger.warning("Error while attaching classification. Error message: %s", e)
