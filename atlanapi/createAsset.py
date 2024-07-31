import logging
import json
import os
import time

from atlanapi.add_owner_group import add_owner_group, check_if_group_exist
from atlanapi.attach_classification import attach_classification
from atlanapi.searchAssets import get_asset_guid_by_qualified_name, get_asset_infos
from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest
from atlanapi.createReadme import create_readme
from atlanapi.linkTerm import link_term
from atlanapi.update_tag import update_level_criticality
from constants import INTEGRATION_TYPE_DYNAMO_DB, INTEGRATION_TYPE_ATHENA, INTEGRATION_TYPE_REDSHIFT, DYNAMODB_CONN_QN, \
    ATHENA_CONN_QN, REDSHIFT_CONN_QN, SERVICE_ACC_API_NAME
from exception.EnvVariableNotFound import EnvVariableNotFound
from model.file import get_atlan_team
from model import Schema, Table, Column

logger = logging.getLogger('main_logger')

api_conf = create_api_config()  # TODO Move to singleton or Env
authorization = 'Bearer {}'.format(api_conf.api_token)
headers = {
    'Authorization': authorization,
    'Content-Type': 'application/json'
}

"""
Looking for asset attribute database qualified name
- asset is Schema, qualified name :         default/mongodb/database/schema
- get_attribute_qualified_name(asset, 1) -> default/mongodb/database

- asset is Entity, qualified name :         default/mongodb/database/schema/entity
- get_attribute_qualified_name(asset, 1) -> default/mongodb/database/schema
- get_attribute_qualified_name(asset, 2) -> default/mongodb/database
"""


def get_attribute_qualified_name(asset, level):
    return '/'.join(asset.get_qualified_name().split('/')[:-level])


def get_asset_attribute_qualified_name(asset, level):
    logger.info("Asset qualified Name : {}".format(asset.get_qualified_name()))
    if isinstance(asset, Schema):
        qualified_name = get_attribute_qualified_name(asset, level)
    else:
        qualified_name = get_attribute_qualified_name(asset, level + 1)
    return qualified_name


def create_asset_connection(asset):
    qualified_name = get_asset_attribute_qualified_name(asset, 2)
    if get_asset_guid_by_qualified_name(qualified_name, "Connection"):
        logger.info("Connection : {} already exists, does not need to be created".format(qualified_name))
    else:
        logger.info("Connection : {} does not exist, creating database...".format(qualified_name))
        if asset.integration_type == INTEGRATION_TYPE_DYNAMO_DB:
            connection = DYNAMODB_CONN_QN
        elif asset.integration_type == INTEGRATION_TYPE_ATHENA:
            connection = ATHENA_CONN_QN
        elif asset.integration_type == INTEGRATION_TYPE_REDSHIFT:
            connection = REDSHIFT_CONN_QN
        data = {
            "entities": [
                {
                    "typeName": "Connection",
                    "attributes": {
                        "name": asset.integration_type + "-integration",
                        "category": "warehouse",
                        "connectorName": asset.integration_type,
                        "qualifiedName": qualified_name,
                        "adminUsers": [
                            "mfelja"
                        ]
                    }
                }
            ]
        }

        url = "https://{}/api/meta/entity/bulk#createConnections".format(api_conf.instance)
        request_object = AtlanApiRequest("POST", url, headers, json.dumps(data))
        response = request_object.send_atlan_request()
        logger.info(response.content)
        logger.debug("...created")


def get_asset_updated_by(asset):
    asset_infos = get_asset_infos(asset)
    updated_by = asset_infos.get('entity', {}).get('updatedBy', None)
    return updated_by


def get_asset_description(asset):
    asset_infos = get_asset_infos(asset)
    asset_description = asset_infos.get('entity', {}).get('attributes', {}).get('userDescription', None)
    return asset_description


'''
Create assets only for DynamoDB Integration. For Athena/Glue or Redshift, we are using workflows from Atlan to create assets
We are only updating metadata for Athena/Glue, Redshift integration
'''


def create_assets(assets, tag, integration_type=INTEGRATION_TYPE_DYNAMO_DB):
    try:
        if not assets:
            logger.info("Cannot create assets, the list of assets is empty")
            return
        if integration_type == INTEGRATION_TYPE_DYNAMO_DB or tag == "createProcesses" or tag == "createColumnProcesses":
            logger.debug(
                "Generating API request to create assets in bulk mode so it is searchable for integration : %s",
                integration_type)
            payloads_for_bulk = map(lambda el: el.get_creation_payload_for_bulk_mode(), assets)
            payload = json.dumps({"entities": list(payloads_for_bulk)})
            schema_post_url = 'https://{}/api/meta/entity/bulk#{}'.format(api_conf.instance, tag)
            atlan_api_schema_request_object = AtlanApiRequest("POST", schema_post_url, headers, payload)
            response = atlan_api_schema_request_object.send_atlan_request()
            logger.debug("Creating assets payload {} - {}", payload, response)
            time.sleep(1)
        logger.debug("Creating Readme, linking glossary terms and linking classification...")
        filtered_assets = [asset for asset in assets if
                           (isinstance(asset, Table) or isinstance(asset, Column)) and get_asset_guid_by_qualified_name(
                               asset.get_qualified_name(), asset.get_atlan_type_name())]

        if tag == 'createColumns':
            attach_classification(filtered_assets)
            update_assets_description(filtered_assets, 'changeDescription')
        if get_atlan_team() and check_if_group_exist(get_atlan_team()) and \
                (integration_type == INTEGRATION_TYPE_DYNAMO_DB or (
                        integration_type != INTEGRATION_TYPE_DYNAMO_DB and tag != 'createSchemas')):
            add_owner_group(filtered_assets)
        if tag == 'createTables':
            attach_classification(filtered_assets)
            [update_level_criticality(asset) for asset in filtered_assets]
            update_assets_description(filtered_assets, 'changeDescription')
        for asset in filtered_assets:
            create_readme(asset)
    except EnvVariableNotFound as e:
        logger.warning("Error while creation asset for %s tag. Error message: %s", tag, e)
        raise e


def update_assets_description(assets, tag):
    try:
        if not assets:
            return
        logger.info("Generating API request to create assets in bulk mode so it is searchable")
        if tag == "createTables":
            payloads_for_bulk = map(lambda el: el.get_creation_payload_for_bulk_mode(), assets)
        else:
            payloads_for_bulk = map(lambda el: el.get_update_description_payload_for_bulk_mode(), assets)

        payload = json.dumps({"entities": list(payloads_for_bulk)})
        schema_post_url = 'https://{}/api/meta/entity/bulk#{}'.format(api_conf.instance, tag)
        atlan_api_schema_request_object = AtlanApiRequest("POST", schema_post_url, headers, payload)
        atlan_api_schema_request_object.send_atlan_request()
        time.sleep(1)

    except EnvVariableNotFound as e:
        logger.warning("Error while updating asset for %s tag. Error message: %s", tag, e)
        raise e
