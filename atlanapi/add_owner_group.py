import json
import logging

from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest
from model import Column, Table, Schema

logger = logging.getLogger('main_logger')

api_conf = create_api_config()

authorization = 'Bearer {}'.format(api_conf.api_token)
headers = {
    'Authorization': authorization,
    'Content-Type': 'application/json'
}


def add_owner_group(assets):
    filtered_assets = [asset for asset in assets if isinstance(asset, Schema) or isinstance(asset, Table)]
    if not len(filtered_assets):
        return
    try:
        logger.info("Adding owner group to assets in bulk mode")
        payloads_for_bulk = map(lambda el: el.get_add_owner_group_request_payload(), filtered_assets)
        payload = json.dumps({"entities": list(payloads_for_bulk)})
        add_owner_group_url = 'https://{}/api/meta/entity/bulk#changeOwners'.format(api_conf.instance)
        atlan_api_request_object = AtlanApiRequest("POST", add_owner_group_url, headers, payload)

        atlan_api_request_object.send_atlan_request()
    except Exception as e:
        logger.warning("Error while adding owner group. Error message: %s", e)
