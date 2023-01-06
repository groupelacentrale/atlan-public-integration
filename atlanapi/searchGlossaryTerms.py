import logging
import json

from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest

logger = logging.getLogger('main_logger')

api_conf = create_api_config()

search_url = "https://{}/api/meta/search/indexsearch#findAssetByExactName".format(api_conf.instance)

authorization = 'Bearer {}'.format(api_conf.api_token)
headers = {
    'Authorization': authorization,
    'Content-Type': 'application/json'
}


def matching_term_predicate(entity, glossary_term_name, glossary):
    return (entity["displayText"] == glossary_term_name and
            entity["attributes"]["anchor"]["typeName"] == "AtlasGlossary" and
            entity["attributes"]["anchor"]["attributes"]["name"] == glossary)


def get_glossary_term_guid_by_name(glossary_term_name, glossary):
    query = json.dumps(
        {
            "dsl": {
                "from": 0,
                "size": 20,
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match": {
                                    "__state": "ACTIVE"
                                }
                            },
                            {
                                "match": {
                                    "__typeName.keyword": "AtlasGlossaryTerm"
                                }
                            },
                            {
                                "match": {
                                    "name.keyword": glossary_term_name
                                }
                            }
                        ]
                    }
                }
            },
            "attributes": [
                "anchor"
            ],
            "relationAttributes": [
                "certificateStatus",
                "name",
                "description",
                "qualifiedName"
            ]
        })

    atlan_api_query_request_object = AtlanApiRequest("POST", search_url, headers, query)
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
