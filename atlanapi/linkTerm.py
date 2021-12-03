import json

from ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest
from atlanapi.searchAssets import get_asset_guid_by_qualified_name
from atlanapi.searchGlossaryTerms import get_glossary_term_guid_by_name

api_conf = create_api_config()
headers = {
    'Content-Type': 'application/json;charset=utf-8',
    'APIKEY': api_conf.api_key
}


def link_term(table_name, entity, asset_name, term, glossary):
    term_guid = get_glossary_term_guid_by_name(term, glossary)
    asset_qualified_name = "dynamodb/dynamodb.atlan.com/dynamo_db/{}/{}/{}".format(
        table_name, entity, asset_name)
    asset_info = get_asset_guid_by_qualified_name(asset_qualified_name)
    asset_guid = asset_info['guid']
    payload = json.dumps([{"guid": asset_guid}])
    link_to_term_url = 'https://{}/api/metadata/atlas/tenants/default/glossary/terms/{}/assignedEntities'.format(api_conf.instance, term_guid)
    print(payload)
    atlan_api_request_object = AtlanApiRequest("POST", link_to_term_url, headers, payload)
    try:
        atlan_api_request_object.send_atlan_request()
        print('Term {} linked successfully to {} in table {}'.format(term, asset_name, table_name))
    except Exception as e:
        print('Error while linking term {} to {}\nReason: {}'.format(term, asset_name, e))

