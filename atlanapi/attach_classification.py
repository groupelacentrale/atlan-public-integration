import json
import logging
from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest
from atlanapi.detach_classification import detach_classification
from constants import CLASSIFICATION
from model import Column, Table

logger = logging.getLogger('main_logger')

api_conf = create_api_config()

authorization = 'Bearer {}'.format(api_conf.api_token)
headers = {
    'Authorization': authorization,
    'Content-Type': 'application/json'
}


def attach_classification(assets):
    # List of assets with classification
    assets_with_classification = [asset for asset in assets if (isinstance(asset, Column) or isinstance(asset, Table))
                                  and asset.classification and asset.classification.capitalize() in [x.capitalize() for
                                                                                                     x in
                                                                                                     CLASSIFICATION]]
    # List of assets without classification
    assets_without_classification = [asset.get_asset_name() for asset in assets if
                                     (isinstance(asset, Column) or isinstance(asset, Table))
                                     and (not asset.classification or asset.classification.capitalize() not in [
                                         x.capitalize() for x in CLASSIFICATION])]
    if len(assets_without_classification) > 0:
        logger.warning('Assets {} doesn\'t have a valid classification'.format(assets_without_classification))
    if not len(assets_with_classification):
        logger.info("No classification to attach")
        return
    try:
        logger.info("attaching classification in bulk mode")
        # Detach the classification from assets which have a valid classification to be updated
        detach_classification(assets_with_classification)
        payload_for_bulk = map(lambda el: el.get_classification_payload_for_bulk_mode(), assets_with_classification)
        payload = json.dumps(list(payload_for_bulk))
        attach_classification_url = 'https://{}/api/meta/entity/bulk/classification/displayName'.format(
            api_conf.instance)
        atlan_api_request_object = AtlanApiRequest("POST", attach_classification_url, headers, payload)

        atlan_api_request_object.send_atlan_request()

    except Exception as e:
        logger.warning("Error while attaching classification. Error message: %s", e)
