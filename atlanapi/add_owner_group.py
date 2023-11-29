import json
import logging

from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest
from atlanapi.update_tag import update_aws_team_tag
from model import Column, Table, Schema

logger = logging.getLogger('main_logger')

api_conf = create_api_config()

authorization = 'Bearer {}'.format(api_conf.api_token)
headers = {
    'Authorization': authorization,
    'Content-Type': 'application/json'
}


def get_list_owners_group():
    try:
        url_list = 'https://{}/api/service/groups'.format(api_conf.instance)
        request_object = AtlanApiRequest("GET", url_list, headers, None)
        response = request_object.send_atlan_request()
        response_object = response.json()
        return response_object['records']
    except Exception as e:
        logger.warning('Error while getting owners groups list')


def check_if_group_exist(atlan_team):
    if atlan_team:
        list_og = get_list_owners_group()
        result = [owner_group for owner_group in list_og if atlan_team.lower() == owner_group['name']]
        return len(result) > 0
    return

def add_owner_group(assets):
    filtered_assets = [asset for asset in assets if asset and isinstance(asset, Schema) or isinstance(asset, Table)]
    if not len(filtered_assets):
        return
    try:
        # Update AWS Team tag
        [update_aws_team_tag(asset) for asset in filtered_assets]

        logger.info("Adding owner group to assets in bulk mode")
        payloads_for_bulk = map(lambda el: el.get_add_owner_group_request_payload(), filtered_assets)
        payload = json.dumps({"entities": list(payloads_for_bulk)})
        add_owner_group_url = 'https://{}/api/meta/entity/bulk#changeOwners'.format(api_conf.instance)
        atlan_api_request_object = AtlanApiRequest("POST", add_owner_group_url, headers, payload)
        logger.info("-------------------- ADD OWNER ---------------------------------------")
        logger.info(payload)
        response = atlan_api_request_object.send_atlan_request()
        logger.info(response.status_code)
        logger.info("-----------------------------------------------------------------------")
    except Exception as e:
        logger.warning("Error while adding owner group. Error message: %s", e)
