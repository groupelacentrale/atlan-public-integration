import json
import os
import logging
from atlanapi.createAsset import create_assets, create_asset_database
from atlanapi.delete_asset import delete_asset
from atlanapi.searchAssets import get_asset_guid_by_qualified_name
from model.Asset import Schema
from constants import INTEGRATION_TYPE_DYNAMO_DB

logger = logging.getLogger("test_create_schema")
DATABASE = "dynamo_db2"
SCHEMA = "test_schema"
TABLE = "test_table"
INTEGRATION_TYPE = "DynamoDB"
DESCRIPTION = "Test integration schema test_schema"
TERM = "test_term"
README = "test_schema readme"
GLOSSARY = "test_schema glossary"

DATA = {
        "entities": [
            {
                "typeName": "Schema",
                "attributes": {
                    "name": SCHEMA,
                    "qualifiedName": "dynamodb/dynamodb2.atlan.com/{}/{}".format(DATABASE, SCHEMA),
                    "databaseName": DATABASE,
                    "description": DESCRIPTION,
                    "databaseQualifiedName": "dynamodb/dynamodb2.atlan.com/{}".format(DATABASE),
                    "connectorName": "dynamodb",
                    "connectionQualifiedName": "dynamodb/dynamodb2.atlan.com"
                },
                "relationshipAttributes": {
                    "database": {
                        "typeName": "Database",
                        "uniqueAttributes": {
                            "qualifiedName": "dynamodb/dynamodb2.atlan.com/{}".format(DATABASE)
                        }
                    }
                }
            }
        ]
    }

def test_schema_payload():
    schema = Schema(database_name=DATABASE,
                    integration_type=INTEGRATION_TYPE_DYNAMO_DB,
                    schema_name=SCHEMA,
                    description=DESCRIPTION,
                    readme=README,
                    term=TERM,
                    glossary=GLOSSARY)
    schema_payload = schema.get_creation_payload()
    assert json.dumps(DATA) == schema_payload

def test_create_schema():
    schema = Schema(database_name=DATABASE,
                    integration_type=INTEGRATION_TYPE_DYNAMO_DB,
                    schema_name=SCHEMA,
                    description=DESCRIPTION,
                    readme=README,
                    term=TERM,
                    glossary=GLOSSARY)
    create_assets([schema], "createSchemas")
    schema_guid = get_asset_guid_by_qualified_name(schema.get_qualified_name(), "Schema")
    assert delete_asset(schema_guid)


def test_create_schema_database():
    schema = Schema(database_name=DATABASE,
                    integration_type=INTEGRATION_TYPE_DYNAMO_DB,
                    schema_name=SCHEMA,
                    description=DESCRIPTION,
                    readme=README,
                    term=TERM,
                    glossary=GLOSSARY)

    create_asset_database(schema)
    database_guid = get_asset_guid_by_qualified_name(os.path.split(schema.get_qualified_name())[0], "Database")
    assert delete_asset(database_guid)
