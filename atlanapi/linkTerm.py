import json
import logging

from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest
from atlanapi.searchGlossaryTerms import get_glossary_term_guid_by_name
from atlanapi.unlink_term import unlink_term
from model import TableLineage, ColumnLineage

logger = logging.getLogger('main_logger')

api_conf = create_api_config()

authorization = 'Bearer {}'.format(api_conf.api_token)
headers = {
    'Authorization': authorization,
    'Content-Type': 'application/json'
}


def link_term(assets):
    assets_with_terms = [asset for asset in assets if not isinstance(asset, ColumnLineage) or not isinstance(asset, TableLineage) or asset.term or asset.glossary]
    if not assets_with_terms:
        return
    try:
        #Update all changes for linked terms
        unlink_term(assets_with_terms)
        payload_for_bulk_mode = map(lambda el: el.get_link_term_for_bulk_mode(), assets_with_terms)
        payload = json.dumps({"entities": list(payload_for_bulk_mode)})
        link_to_term_url = 'https://{}/api/meta/entity/bulk#{}'.format(api_conf.instance, 'attachGlossaryTerm')
        atlan_api_request_object = AtlanApiRequest("POST", link_to_term_url, headers, payload)

        atlan_api_request_object.send_atlan_request()

    except Exception as e:
        logger.warning("Error while linking glossary terms")