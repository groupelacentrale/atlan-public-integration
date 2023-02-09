import json
import logging
import functools

from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest

logger = logging.getLogger('main_logger')

api_conf = create_api_config()

authorization = 'Bearer {}'.format(api_conf.api_token)
headers = {
    'Authorization': authorization,
    'Content-Type': 'application/json'
}


def detach_classification(assets):
    list_of_payloads = list(map(lambda el: el.get_detach_classification_payload_for_bulk_mode(), assets))
    payload = json.dumps({"guidHeaderMap": functools.reduce(lambda d1, d2: {**d1, **d2}, list_of_payloads)})
    attach_classification_url = 'https://{}/api/meta/entity/bulk/setClassifications'.format(api_conf.instance)
    atlan_api_request_object = AtlanApiRequest("POST", attach_classification_url, headers, payload)

    response = atlan_api_request_object.send_atlan_request()
    response.content
