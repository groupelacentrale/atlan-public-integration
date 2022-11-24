import logging
import json
import os
import time

from atlanapi.searchAssets import get_asset_guid_by_qualified_name
from constants import DYNAMODB_CONN_QUALIFIED_NAME, INTEGRATION_TYPE_GLUE, INTEGRATION_TYPE_DYNAMO_DB, \
    INTEGRATION_TYPE_REDSHIFT
from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest
from atlanapi.createReadme import create_readme
from atlanapi.linkTerm import link_term
from exception.EnvVariableNotFound import EnvVariableNotFound
from model import Asset
from model.Asset import get_atlan_prod_aws_account_id, get_atlan_redshift_server_url

logger = logging.getLogger('main_logger')

api_conf = create_api_config()

headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer ' + api_conf.api_token
}


def create_asset_database(asset):
    logger.info("Asset qualified Name : {}".format(asset.get_qualified_name()))
    if isinstance(asset, Asset.Schema):
        qualified_name = os.path.split(asset.get_qualified_name())[0]
    else:
        qualified_name = os.path.split(os.path.split(asset.get_qualified_name())[0])[0]
    if get_asset_guid_by_qualified_name(qualified_name, "Database") is None:
        logger.info("Database : {} already exists, does not need to be created".format(asset.database_name))
        return
    logger.info("Database : {} does not exist, creating database...".format(asset.database_name))

    data = {
        "entities": [
            {
                "typeName": "Database",
                "attributes": {
                    "name": asset.database_name,
                    "qualifiedName": qualified_name,
                    "description": "Databse Integration : {}".format(asset.integration_type),
                    "connectorName": os.path.split(qualified_name)[0],
                    "connectionQualifiedName": os.path.split(qualified_name)[0]
                }
            }
        ]
    }
    logger.info("QualifiedName database : {}".format(qualified_name))
    logger.info("ConnectorName : {}".format(os.path.split(qualified_name)[0]))
    logger.info("Connection Qualified Name : {}".format(os.path.split(qualified_name)[0]))
    url = 'https://{}/api/meta/entity/bulk#{}'.format(api_conf.instance, 'createDatabases')
    request_object = AtlanApiRequest("POST", url, headers, json.dumps(data))
    response = request_object.send_atlan_request()
    logger.info(response.content)
    logger.debug("...created")


def create_asset(asset):
    create_assets([asset])


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
