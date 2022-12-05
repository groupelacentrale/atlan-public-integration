import logging
import json
import os
import time

from atlanapi.searchAssets import get_asset_guid_by_qualified_name
from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest
from atlanapi.createReadme import create_readme
from atlanapi.linkTerm import link_term
from constants import INTEGRATION_TYPE_DYNAMO_DB, INTEGRATION_TYPE_ATHENA, INTEGRATION_TYPE_REDSHIFT, DYNAMODB_CONN_QN, ATHENA_CONN_QN, REDSHIFT_CONN_QN
from exception.EnvVariableNotFound import EnvVariableNotFound
from model import Asset

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
    if isinstance(asset, Asset.Schema):
        qualified_name = get_attribute_qualified_name(asset, level)
    else:
        qualified_name = get_attribute_qualified_name(asset, level + 1)
    return qualified_name


def create_asset_connection(asset):
    logger.info("Asset qualified Name : {}".format(asset.get_qualified_name()))
    qualified_name = get_asset_attribute_qualified_name(asset, 2)
    logger.info("CONNECTION ---------> Qualified Name {}".format(qualified_name))
    if get_asset_guid_by_qualified_name(qualified_name, "Connection"):
        logger.info("Connection : {} already exists, does not need to be created".format(qualified_name))
    else:
        logger.info("Connection : {} does not exist, creating database...".format(qualified_name))
        if asset.integration_type == INTEGRATION_TYPE_DYNAMO_DB :
            connection = DYNAMODB_CONN_QN
        elif asset.integration_type == INTEGRATION_TYPE_ATHENA :
            connection = ATHENA_CONN_QN
        elif asset.integration_type == INTEGRATION_TYPE_REDSHIFT :
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


def create_asset_database(asset):
    qualified_name = get_asset_attribute_qualified_name(asset, 1)
    logger.info("DATABASE ---------> Qualified Name {}".format(qualified_name))
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
                        "qualifiedName": qualified_name,
                        "description": "Databse Integration : {}".format(asset.integration_type),
                        "connectorName": asset.integration_type,
                        "connectionQualifiedName": os.path.split(qualified_name)[0]
                    }
                }
            ]
        }
        logger.info("QualifiedName database : {}".format(qualified_name))
        logger.info("ConnectorName : {}".format(asset.integration_type))
        logger.info("Connection Qualified Name : {}".format(os.path.split(qualified_name)[0]))
        url = 'https://{}/api/meta/entity/bulk#{}'.format(api_conf.instance, 'createDatabases')
        request_object = AtlanApiRequest("POST", url, headers, json.dumps(data))
        response = request_object.send_atlan_request()
        logger.info(response.content)
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
        response = atlan_api_schema_request_object.send_atlan_request()
        logger.info(response.content)

        time.sleep(1)

        logger.debug("Creating Readme and linking glossary terms...")
        for asset in assets:
            create_readme(asset)
            link_term(asset)
    except EnvVariableNotFound as e:
        logger.warning(e.message)
        raise e
