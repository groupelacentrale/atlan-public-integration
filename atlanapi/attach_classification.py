import json
import logging

from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest
from atlanapi.detach_classification import detach_classification
from constants import CLASSIFICATION
from exception.EnvVariableNotFound import EnvVariableNotFound
from model import Column

logger = logging.getLogger('main_logger')

api_conf = create_api_config()

authorization = 'Bearer {}'.format(api_conf.api_token)
headers = {
    'Authorization': authorization,
    'Content-Type': 'application/json'
}


def attach_classification(assets):
    assets_with_classification = [asset for asset in assets if isinstance(Column, asset) and asset.classification and asset.classification in CLASSIFICATION]
    if not len(assets_with_classification):
        logger.info("No classification")
        return
    try:
        logger.info("attaching classification in bulk mode")
        # Detach the classification from assets which have a valid classification to be updated
        detach_classification(assets_with_classification)
        payload_for_bulk = map(lambda el: el.get_classification_payload_for_bulk_mode(), assets_with_classification)
        payload = json.dumps(list(payload_for_bulk))
        attach_classification_url = 'https://{}/api/meta/entity/bulk/classification/displayName'.format(api_conf.instance)
        atlan_api_request_object = AtlanApiRequest("POST", attach_classification_url, headers, payload)

        atlan_api_request_object.send_atlan_request()

    except EnvVariableNotFound as e:
        logger.warning(e.message)
        raise e
