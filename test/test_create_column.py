import json

from atlanapi.createAsset import create_assets
from atlanapi.delete_asset import delete_asset
from atlanapi.searchAssets import get_asset_guid_by_qualified_name, get_asset_by_guid
from model import Column
from test.data import DATABASE, SCHEMA, ENTITY, COLUMN, INTEGRATION_TYPE, DESCRIPTION, DATA_TYPE, COLUMN_DATA


def test_column_payload_bulk_mode():
    column = Column(integration_type=INTEGRATION_TYPE,
                    database_name=DATABASE,
                    schema_name=SCHEMA,
                    entity_name=ENTITY,
                    column_name=COLUMN,
                    description=DESCRIPTION,
                    data_type=DATA_TYPE)
    column_payload = column.get_creation_payload_for_bulk_mode()
    assert COLUMN_DATA == column_payload


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
    created_asset = get_asset_by_guid(column_guid)
    if created_asset['entity']['typeName'] == column.get_atlan_type_name() and created_asset['entity']['attributes']['qualifiedName'] == column.get_qualified_name():
        delete_asset(column_guid)
        assert True
    else:
        assert False
