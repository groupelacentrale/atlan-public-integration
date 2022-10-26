import logging
import json
import time

from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest
from atlanapi.createReadme import create_readme
from atlanapi.linkTerm import link_term
from exception.EnvVariableNotFound import EnvVariableNotFound

logger = logging.getLogger('main_logger')

api_conf = create_api_config()

headers = {
    'Content-Type': 'application/json',
    'Authorization': api_conf.api_token
}


def create_asset(asset):
    create_assets([asset])


def create_assets(assets):
    try:
        if not assets:
            return
        logger.debug("Generating API request to create assets in bulk mode so it is searchable")
        payloads_for_bulk = map(lambda el: el.get_creation_payload_for_bulk_mode(), assets)

        payload = json.dumps({"entities": list(payloads_for_bulk)})

        schema_post_url = 'https://{}/api/metadata/atlas/tenants/default/entity/bulk'.format(api_conf.instance)
        atlan_api_schema_request_object = AtlanApiRequest("POST", schema_post_url, headers, payload)
        atlan_api_schema_request_object.send_atlan_request()

        time.sleep(1)

        logger.debug("Creating Readme and linking glossary terms...")
        for asset in assets:
            create_readme(asset)
            link_term(asset)
    except EnvVariableNotFound as e:
        logger.warning(e.message)
        raise e

