import json

from atlanapi.createAsset import create_assets
from atlanapi.delete_asset import delete_asset
from atlanapi.searchAssets import get_asset_guid_by_qualified_name
from model.Asset import Column
from constants import INTEGRATION_TYPE_DYNAMO_DB

COLUMN = "col_index"
ENTITY = "test_table"
INTEGRATION_TYPE = "DynamoDb"
DESCRIPTION = "Test Unit"
DATABASE = "dynamo_db2"
SCHEMA = "test_schema"
README = "test_readme"
TERM = "test_term"
GLOSSARY = "test_glossary"
DATA_TYPE = "INT"

DATA = {
    "typeName": "Column",
    "attributes": {
        "name": COLUMN,
        "qualifiedName": "dynamodb/dynamodb2.atlan.com/{}/{}/{}/{}".format(DATABASE, SCHEMA, ENTITY,
                                                                           COLUMN),
        "connectorName": "dynamodb",
        "tableName": ENTITY,
        "tableQualifiedName": "dynamodb/dynamodb2.atlan.com/{}/{}/{}".format(DATABASE, SCHEMA, ENTITY),
        "schemaName": SCHEMA,
        "schemaQualifiedName": "dynamodb/dynamodb2.atlan.com/{}/{}".format(DATABASE, SCHEMA),
        "databaseName": DATABASE,
        "databaseQualifiedName": "dynamodb/dynamodb2.atlan.com/{}".format(DATABASE),
        "connectionQualifiedName": "dynamodb/dynamodb2.atlan.com",
        "dataType": DATA_TYPE,
        "order": 1,
        "description": DESCRIPTION
    },
    "relationshipAttributes": {
        "table": {
            "typeName": "Table",
            "uniqueAttributes": {
                "qualifiedName": "dynamodb/dynamodb2.atlan.com/{}/{}/{}".format(DATABASE, SCHEMA,
                                                                                ENTITY)
            }
        }
    }
}


def test_column_payload_bulk_mode():
    column = Column(integration_type=INTEGRATION_TYPE_DYNAMO_DB,
                    database_name=DATABASE,
                    schema_name=SCHEMA,
                    entity_name=ENTITY,
                    column_name=COLUMN,
                    description=DESCRIPTION,
                    data_type=DATA_TYPE)
    column_payload = column.get_creation_payload_for_bulk_mode()
    assert DATA == column_payload


def test_create_column():
    column = Column(integration_type=INTEGRATION_TYPE_DYNAMO_DB,
                    database_name=DATABASE,
                    schema_name=SCHEMA,
                    entity_name=ENTITY,
                    column_name=COLUMN,
                    description=DESCRIPTION,
                    data_type=DATA_TYPE)
    create_assets([column], "createColumns")
    column_guid = get_asset_guid_by_qualified_name(column.get_qualified_name(), "Column")
    assert delete_asset(column_guid)
