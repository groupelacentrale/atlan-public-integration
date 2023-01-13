import logging
import json

from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest

logger = logging.getLogger('main_logger')

api_conf = create_api_config()
authorization = 'Bearer {}'.format(api_conf.api_token)

search_headers = {
    'Authorization': authorization,
    'Content-Type': 'application/json'
}


def get_entity_columns(entity_guid):
    search_url = "https://{}//api/meta/entity/guid/{}".format(api_conf.instance, entity_guid)
    atlan_api_query_request_object = AtlanApiRequest("GET", search_url, search_headers, {})
    try:
        columns_response = json.loads(atlan_api_query_request_object.send_atlan_request().text)
        columns = columns_response['entity']['relationshipAttributes']['columns']
        if columns:
            return {column['displayText']: column['guid'] for column in columns}
        # Not really an error, because it is normal that an entity doesn't have columns at first.
        logger.debug("entity '{}' does not have any columns".format(entity_guid))
        return {}
    except:
        logger.error("Error while fetching columns for entity id '{}'".format(entity_guid))
        return {}
