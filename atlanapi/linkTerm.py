import json
import logging

from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest
from atlanapi.searchGlossaryTerms import get_glossary_term_guid_by_name
from model import TableLineage, ColumnLineage

logger = logging.getLogger('main_logger')

api_conf = create_api_config()

authorization = 'Bearer {}'.format(api_conf.api_token)
headers = {
    'Authorization': authorization,
    'Content-Type': 'application/json'
}


def link_term(asset):
    if isinstance(asset, ColumnLineage) or isinstance(asset, TableLineage) or not asset.term or not asset.glossary:
        return
    try:
        term_guid = get_glossary_term_guid_by_name(asset.term, asset.glossary)
        payload = json.dumps({
            "entities":
                [
                    {
                        "typeName": asset.get_atlan_type_name(),
                        "attributes": {
                            "name": asset.get_asset_name(),
                            "qualifiedName": asset.get_qualified_name()
                        },
                        "relationshipAttributes": {
                            "meanings": [
                                {
                                    "typeName": "AtlasGlossaryTerm",
                                    "guid": term_guid
                                }
                            ]
                        }
                    }
                ]
            })
        link_to_term_url = 'https://{}/api/meta/entity/bulk#{}'.format(api_conf.instance, 'attachGlossaryTerm')
        atlan_api_request_object = AtlanApiRequest("POST", link_to_term_url, headers, payload)

        atlan_api_request_object.send_atlan_request()
        logger.info("Glossary term '{}' linked successfully to {} '{}'".format(
            asset.term, asset.get_atlan_type_name(), asset.get_asset_name())
        )

    except Exception as e:
        logger.warning("Error while linking glossary term '{}' to {} '{}'. Glossary term must already exist in Atlan."
                       .format(asset.term, asset.get_atlan_type_name(), asset.get_asset_name()))
