import logging
import json
import time

from ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest
from atlanapi.createReadme import create_readme
from atlanapi.linkTerm import link_term

logger = logging.getLogger('main_logger')

api_conf = create_api_config()

headers = {
    'Content-Type': 'application/json',
    'APIKEY': api_conf.api_key
}


def create_asset(asset):
    create_assets([asset])


def create_assets(assets):
    if not assets:
        return
    logger.info("Generating API request to create assets in bulk mode so it is searchable")
    payloads_for_bulk = map(lambda el: el.get_creation_payload_for_bulk_mode(), assets)

    payload = json.dumps({"entities": list(payloads_for_bulk)})

    logger.info("Posting API request to create assets")
    schema_post_url = 'https://{}/api/metadata/atlas/tenants/default/entity/bulk'.format(api_conf.instance)
    atlan_api_schema_request_object = AtlanApiRequest("POST", schema_post_url, headers, payload)
    atlan_api_schema_request_object.send_atlan_request()

    time.sleep(1)

    for asset in assets:
        create_readme(asset)
        link_term(asset)

