import json
import logging

from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest

logger = logging.getLogger('main_logger')

api_conf = create_api_config()

authorization = 'Bearer {}'.format(api_conf.api_token)
headers = {
    'Authorization': authorization,
    'Content-Type': 'application/json'
}

def unlink_term(assets):
    payload_bulk_mode = map(lambda el: el.get_unlink_term_for_bulk_mode(), assets)
    payload = json.dumps({"entities": list(payload_bulk_mode)})
    unlink_term_url = 'https://{}/api/meta/entity/bulk'.format(api_conf.instance)
    atlan_api_request_object = AtlanApiRequest("POST", unlink_term_url, headers, payload)

    atlan_api_request_object.send_atlan_request()