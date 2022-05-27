import json
import logging

from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest
from atlanapi.searchAssets import get_asset_guid_by_qualified_name
from atlanapi.searchGlossaryTerms import get_glossary_term_guid_by_name
from model.Asset import EntityLineage, ColumnLineage

logger = logging.getLogger('main_logger')


def link_term(asset):
    if isinstance(asset, ColumnLineage) or isinstance(asset, EntityLineage) or not asset.term or not asset.glossary:
        return
    try:
        term_guid = get_glossary_term_guid_by_name(asset.term, asset.glossary)
        asset_qualified_name = asset.get_qualified_name()
        asset_info = get_asset_guid_by_qualified_name(asset_qualified_name, asset.get_atlan_type_name())
        asset_guid = asset_info['guid']
        payload = json.dumps([{"guid": asset_guid}])
        api_conf = create_api_config()
        headers = {
            'Content-Type': 'application/json;charset=utf-8',
            'APIKEY': api_conf.api_key
        }
        link_to_term_url = 'https://{}/api/metadata/atlas/tenants/default/glossary/terms/{}/assignedEntities'.format(
            api_conf.instance, term_guid)

        atlan_api_request_object = AtlanApiRequest("POST", link_to_term_url, headers, payload)

        atlan_api_request_object.send_atlan_request()
        logger.debug("Glossary term '{}' linked successfully to {} '{}'"
                     .format(asset.term, asset.get_atlan_type_name(), asset.get_asset_name()))
    except Exception as e:
        logger.warning("Error while linking glossary term '{}' to {} '{}'. Glossary term must already exist in Atlan."
                       .format(asset.term, asset.get_atlan_type_name(), asset.get_asset_name()))
