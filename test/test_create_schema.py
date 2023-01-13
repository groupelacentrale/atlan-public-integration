import json
import os
import logging
from atlanapi.createAsset import create_assets, create_asset_database
from atlanapi.delete_asset import delete_asset
from atlanapi.searchAssets import get_asset_guid_by_qualified_name
from model.Asset import Schema
from constants import INTEGRATION_TYPE_DYNAMO_DB, INTEGRATION_TYPE_REDSHIFT, INTEGRATION_TYPE_ATHENA, DYNAMODB_CONN_QN, \
    REDSHIFT_CONN_QN, ATHENA_CONN_QN

logger = logging.getLogger("test_create_schema")
DATABASE = "dynamo_db2"
SCHEMA = "test_schema"
TABLE = "test_table"
INTEGRATION_TYPE = INTEGRATION_TYPE_ATHENA
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

DATA = {
    "entities": [
        {
            "typeName": "Schema",
            "attributes": {
                "name": SCHEMA,
                "qualifiedName": "{}/{}/{}".format(CONN_QN, DATABASE, SCHEMA),
                "databaseName": DATABASE,
                "description": DESCRIPTION,
                "databaseQualifiedName": "{}/{}".format(CONN_QN, DATABASE),
                "connectorName": CONN_NAME,
                "connectionQualifiedName": CONN_QN
            },
            "relationshipAttributes": {
                "database": {
                    "typeName": "Database",
                    "uniqueAttributes": {
                        "qualifiedName": "{}/{}".format(CONN_QN, DATABASE)
                    }
                }
            }
        }
    ]
}


def test_schema_payload():
    schema = Schema(database_name=DATABASE,
                    integration_type=INTEGRATION_TYPE,
                    schema_name=SCHEMA,
                    description=DESCRIPTION,
                    readme=README,
                    term=TERM,
                    glossary=GLOSSARY)
    schema_payload = schema.get_creation_payload()
    assert json.dumps(DATA) == schema_payload


def test_create_schema():
    schema = Schema(database_name=DATABASE,
                    integration_type=INTEGRATION_TYPE,
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
                    integration_type=INTEGRATION_TYPE,
                    schema_name=SCHEMA,
                    description=DESCRIPTION,
                    readme=README,
                    term=TERM,
                    glossary=GLOSSARY)

    create_asset_database(schema)
    database_guid = get_asset_guid_by_qualified_name(os.path.split(schema.get_qualified_name())[0], "Database")
    assert delete_asset(database_guid)
