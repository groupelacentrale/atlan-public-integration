import json
import os

from atlanapi.createAsset import create_assets, create_asset_database
from atlanapi.delete_asset import delete_asset
from atlanapi.searchAssets import get_asset_guid_by_qualified_name
from model.Asset import Entity
from constants import INTEGRATION_TYPE_DYNAMO_DB

import logging

logger = logging.getLogger()

ENTITY = "test_table"
INTEGRATION_TYPE = "DynamoDb"
DESCRIPTION = "Test Unit"
DATABASE = "dynamo_db2"
SCHEMA = "test_schema"
README = "test_readme"
TERM = "test_term"
GLOSSARY = "test_glossary"

DATA = {"entities": [
    {
        "typeName": "Table",
        "attributes": {
            "name": ENTITY,
            "qualifiedName": "dynamodb/dynamodb2.atlan.com/{}/{}/{}".format(DATABASE, SCHEMA, ENTITY),
            "connectorName": "dynamodb",
            "schemaName": SCHEMA,
            "schemaQualifiedName": "dynamodb/dynamodb2.atlan.com/{}/{}".format(DATABASE, SCHEMA),
            "databaseName": DATABASE,
            "databaseQualifiedName": "dynamodb/dynamodb2.atlan.com/{}".format(DATABASE),
            "connectionQualifiedName": "dynamodb/dynamodb2.atlan.com",
            "description": DESCRIPTION
        },
        "relationshipAttributes": {
            "atlanSchema": {
                "typeName": "Schema",
                "uniqueAttributes": {
                    "qualifiedName": "dynamodb/dynamodb2.atlan.com/{}/{}".format(DATABASE, SCHEMA)
                }
            }
        }
    }
]}


def test_table_payload():
    entity = Entity(integration_type=INTEGRATION_TYPE_DYNAMO_DB,
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
    entity = Entity(integration_type=INTEGRATION_TYPE_DYNAMO_DB,
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
    entity = Entity(integration_type=INTEGRATION_TYPE_DYNAMO_DB,
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
