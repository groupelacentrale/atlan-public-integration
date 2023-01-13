import json
import os

from atlanapi.createAsset import create_assets, create_asset_database
from atlanapi.delete_asset import delete_asset
from atlanapi.searchAssets import get_asset_guid_by_qualified_name
from model.Asset import Entity
from constants import INTEGRATION_TYPE_DYNAMO_DB, INTEGRATION_TYPE_REDSHIFT, DYNAMODB_CONN_QN, INTEGRATION_TYPE_ATHENA, \
    ATHENA_CONN_QN, REDSHIFT_CONN_QN

import logging

logger = logging.getLogger()

DATABASE = "dynamo_db2"
SCHEMA = "test_schema"
ENTITY = "test_table"
INTEGRATION_TYPE = INTEGRATION_TYPE_REDSHIFT
DESCRIPTION = "Test integration schema test_schema"
TERM = "test_term"
README = "test_schema readme"
GLOSSARY = "test_schema glossary"

# Default connection qualified name is dynamo
CONN_QN = DYNAMODB_CONN_QN
CONN_NAME = INTEGRATION_TYPE_DYNAMO_DB.lower()

if INTEGRATION_TYPE == INTEGRATION_TYPE_ATHENA:
    CONN_NAME = ATHENA_CONN_QN
    CONN_QN = ATHENA_CONN_QN + "/athena-tmp-test"
elif INTEGRATION_TYPE == INTEGRATION_TYPE_REDSHIFT:
    CONN_NAME = REDSHIFT_CONN_QN
    CONN_QN = REDSHIFT_CONN_QN + "/redshift-tmp"

DATA = {"entities": [
    {
        "typeName": "Table",
        "attributes": {
            "name": ENTITY,
            "qualifiedName": "{}/{}/{}/{}".format(CONN_QN, DATABASE, SCHEMA, ENTITY),
            "connectorName": CONN_NAME,
            "schemaName": SCHEMA,
            "schemaQualifiedName": "{}/{}/{}".format(CONN_QN, DATABASE, SCHEMA),
            "databaseName": DATABASE,
            "databaseQualifiedName": "{}/{}".format(CONN_QN, DATABASE),
            "connectionQualifiedName": CONN_QN,
            "description": DESCRIPTION
        },
        "relationshipAttributes": {
            "atlanSchema": {
                "typeName": "Schema",
                "uniqueAttributes": {
                    "qualifiedName": "{}/{}/{}".format(CONN_QN, DATABASE, SCHEMA)
                }
            }
        }
    }
]}


def test_table_payload():
    entity = Entity(integration_type=INTEGRATION_TYPE,
                    entity_name=ENTITY,
                    description=DESCRIPTION,
                    database_name=DATABASE,
                    schema_name=SCHEMA,
                    readme=README,
                    glossary=GLOSSARY,
                    term=TERM)
    entity_payload = entity.get_creation_payload()
    assert json.dumps(DATA) == entity_payload


def test_create_table():
    entity = Entity(integration_type=INTEGRATION_TYPE,
                    entity_name=ENTITY,
                    description=DESCRIPTION,
                    database_name=DATABASE,
                    schema_name=SCHEMA,
                    readme=README,
                    glossary=GLOSSARY,
                    term=TERM)
    create_assets([entity], "createTables")
    entity_guid = get_asset_guid_by_qualified_name(entity.get_qualified_name(), "Table")
    assert delete_asset(entity_guid)


def test_create_entity_database():
    entity = Entity(integration_type=INTEGRATION_TYPE,
                    entity_name=ENTITY,
                    description=DESCRIPTION,
                    database_name=DATABASE,
                    schema_name=SCHEMA,
                    readme=README,
                    glossary=GLOSSARY,
                    term=TERM)
    create_asset_database(entity)
    schema_qualified_name = os.path.split(entity.get_qualified_name())[0]
    database_qualified_name = os.path.split(schema_qualified_name)[0]
    database_guid = get_asset_guid_by_qualified_name(database_qualified_name, "Database")
    assert delete_asset(database_guid)
