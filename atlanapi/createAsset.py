import logging
import json
import os
import time

from atlanapi.add_owner_group import add_owner_group, check_if_group_exist
from atlanapi.attach_classification import attach_classification
from atlanapi.searchAssets import get_asset_guid_by_qualified_name
from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest
from atlanapi.createReadme import create_readme
from atlanapi.linkTerm import link_term
from atlanapi.update_tag import update_aws_team_tag, update_level_criticality
from constants import INTEGRATION_TYPE_DYNAMO_DB, INTEGRATION_TYPE_ATHENA, INTEGRATION_TYPE_REDSHIFT, DYNAMODB_CONN_QN, \
    ATHENA_CONN_QN, REDSHIFT_CONN_QN
from exception.EnvVariableNotFound import EnvVariableNotFound
from model.file import get_atlan_team
from model.schema import Schema

logger = logging.getLogger('main_logger')

api_conf = create_api_config()
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


def create_asset_database(asset):
    qualified_name = get_asset_attribute_qualified_name(asset, 1)
    if get_asset_guid_by_qualified_name(qualified_name, "Database"):
        logger.info("Database : {} already exists, does not need to be created".format(asset.database_name))
    else:
        logger.info("Database : {} does not exist, creating database...".format(asset.database_name))

        data = {
            "entities": [
                {
                    "typeName": "Database",
                    "attributes": {
                        "name": asset.database_name,
                        "connectorName": asset.integration_type,
                        "qualifiedName": qualified_name,
                        "description": "Databse Integration : {}".format(asset.integration_type),
                        "connectionQualifiedName": os.path.split(qualified_name)[0]
                    }
                }
            ]
        }
        url = 'https://{}/api/meta/entity/bulk#{}'.format(api_conf.instance, 'createDatabases')
        request_object = AtlanApiRequest("POST", url, headers, json.dumps(data))
        # TODO this feature should be disabled as database is created once and manually
        # request_object.send_atlan_request()
        logger.debug("...created")


def create_assets(assets, tag):
    try:
        if not assets:
            return
        logger.debug("Generating API request to create assets in bulk mode so it is searchable")
        payloads_for_bulk = map(lambda el: el.get_creation_payload_for_bulk_mode(), assets)

        payload = json.dumps({"entities": list(payloads_for_bulk)})
        schema_post_url = 'https://{}/api/meta/entity/bulk#{}'.format(api_conf.instance, tag)
        atlan_api_schema_request_object = AtlanApiRequest("POST", schema_post_url, headers, payload)
        atlan_api_schema_request_object.send_atlan_request()
        time.sleep(1)

        logger.debug("Creating Readme, linking glossary terms and linking classification...")
        if tag == 'createColumns':
            attach_classification(assets)
        if tag == 'createColumns' or tag == 'createTables':
            link_term(assets)
        if get_atlan_team() and check_if_group_exist(get_atlan_team()):
            add_owner_group(assets)
        if tag == 'createTables':
            attach_classification(assets)
            [update_level_criticality(asset) for asset in assets]
        for asset in assets:
            create_readme(asset)
    except EnvVariableNotFound as e:
        logger.warning("Error while creation asset for %s tag. Error message: %s", tag, e)
        raise e


def update_assets(assets, tag):
    try:
        if not assets:
            return
        logger.debug("Generating API request to create assets in bulk mode so it is searchable")
        payloads_for_bulk = map(lambda el: el.get_creation_payload_for_bulk_mode(), assets)

        payload = json.dumps({"entities": list(payloads_for_bulk)})
        schema_post_url = 'https://{}/api/meta/entity/bulk#{}'.format(api_conf.instance, tag)
        atlan_api_schema_request_object = AtlanApiRequest("POST", schema_post_url, headers, payload)
        atlan_api_schema_request_object.send_atlan_request()
        time.sleep(1)

        logger.debug("Creating Readme, linking glossary terms and linking classification...")
    except EnvVariableNotFound as e:
        logger.warning("Error while updating asset for %s tag. Error message: %s", tag, e)
        raise e
