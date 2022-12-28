import json

from atlanapi.createAsset import create_assets
from atlanapi.delete_asset import delete_asset
from atlanapi.searchAssets import get_asset_guid_by_qualified_name
from model.Asset import Column
from constants import INTEGRATION_TYPE_DYNAMO_DB, INTEGRATION_TYPE_REDSHIFT, DYNAMODB_CONN_QN, INTEGRATION_TYPE_ATHENA, \
    ATHENA_CONN_QN, REDSHIFT_CONN_QN

COLUMN = "col_index"
DATABASE = "dynamo_db2"
SCHEMA = "test_schema"
ENTITY = "test_table"
INTEGRATION_TYPE = INTEGRATION_TYPE_REDSHIFT
DESCRIPTION = "Test integration schema test_schema"
TERM = "test_term"
README = "test_schema readme"
GLOSSARY = "test_schema glossary"
DATA_TYPE = "INT"

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
    "typeName": "Column",
    "attributes": {
        "name": COLUMN,
        "qualifiedName": "{}/{}/{}/{}/{}".format(CONN_QN, DATABASE, SCHEMA, ENTITY, COLUMN),
        "connectorName": CONN_NAME,
        "tableName": ENTITY,
        "tableQualifiedName": "{}/{}/{}/{}".format(CONN_QN, DATABASE, SCHEMA, ENTITY),
        "schemaName": SCHEMA,
        "schemaQualifiedName": "{}/{}/{}".format(CONN_QN, DATABASE, SCHEMA),
        "databaseName": DATABASE,
        "databaseQualifiedName": "{}/{}".format(CONN_QN, DATABASE),
        "connectionQualifiedName": CONN_QN,
        "dataType": DATA_TYPE,
        "order": 1,
        "description": DESCRIPTION
    },
    "relationshipAttributes": {
        "table": {
            "typeName": "Table",
            "uniqueAttributes": {
                "qualifiedName": "{}/{}/{}/{}".format(CONN_QN, DATABASE, SCHEMA, ENTITY)
            }
        }
    }
}


def test_column_payload_bulk_mode():
    column = Column(integration_type=INTEGRATION_TYPE,
                    database_name=DATABASE,
                    schema_name=SCHEMA,
                    entity_name=ENTITY,
                    column_name=COLUMN,
                    description=DESCRIPTION,
                    data_type=DATA_TYPE)
    column_payload = column.get_creation_payload_for_bulk_mode()
    assert DATA == column_payload


def test_create_column():
    column = Column(integration_type=INTEGRATION_TYPE,
                    database_name=DATABASE,
                    schema_name=SCHEMA,
                    entity_name=ENTITY,
                    column_name=COLUMN,
                    description=DESCRIPTION,
                    data_type=DATA_TYPE)
    create_assets([column], "createColumns")
    column_guid = get_asset_guid_by_qualified_name(column.get_qualified_name(), "Column")
    assert delete_asset(column_guid)
