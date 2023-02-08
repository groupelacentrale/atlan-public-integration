import json
import logging

from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest
from exception.EnvVariableNotFound import EnvVariableNotFound

logger = logging.getLogger('main_logger')

api_conf = create_api_config()

authorization = 'Bearer {}'.format(api_conf.api_token)
headers = {
    'Authorization': authorization,
    'Content-Type': 'application/json'
}


def detach_classification(assets):
    try:
        payload_for_bulk = map(lambda el: el.get_detach_classification_payload_for_bulk_mode(), assets)
        payload = json.dumps(list(payload_for_bulk))
        attach_classification_url = 'https://{}/api/meta/entity/bulk/setClassifications'.format(api_conf.instance)
        atlan_api_request_object = AtlanApiRequest("POST", attach_classification_url, headers, payload)

        atlan_api_request_object.send_atlan_request()

    except EnvVariableNotFound as e:
        logger.warning(e.message)
        raise e
