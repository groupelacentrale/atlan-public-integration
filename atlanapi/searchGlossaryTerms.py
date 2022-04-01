import logging
import json

from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest

logger = logging.getLogger('main_logger')

api_conf = create_api_config()
search_url = "https://{}/api/metadata/atlas/tenants/default/search/basic".format(api_conf.instance)
search_headers = {
    'Content-Type': 'application/json;charset=utf-8',
    'APIKEY': api_conf.api_key
}


def matching_term_predicate(entity, glossary_term_name, glossary):
    return (entity["displayText"] == glossary_term_name and
            entity["attributes"]["anchor"]["uniqueAttributes"]["qualifiedName"] == "Glossary" and
            entity["attributes"]["anchor"]["uniqueAttributes"]["name"] == glossary)


def get_glossary_term_guid_by_name(glossary_term_name, glossary):
    query = json.dumps({
        "excludeDeletedEntities": False,
        "includeSubClassifications": False,
        "includeSubTypes": True,
        "includeClassificationAttributes": False,
        "typeName": "AtlasGlossary,AtlasGlossaryTerm,AtlasGlossaryCategory",
        "attributes": [
            "anchor",
            "categories",
            "status"
        ],
        "limit": 20,
        "offset": 0,
        "query": glossary_term_name,
        "minScore": 100
    })
    atlan_api_query_request_object = AtlanApiRequest("POST", search_url, search_headers, query)
    try:
        search_response = json.loads(atlan_api_query_request_object.send_atlan_request().text)
        result = next(
            (entity for entity in search_response["entities"] if
             matching_term_predicate(entity, glossary_term_name, glossary)), None)
        logger.debug(result)
        return result["guid"]
    except:
        logger.debug("Cannot get search result for glossary term '{}'".format(glossary_term_name))
        return {}
