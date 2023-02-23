import json

from atlanapi.searchAssets import get_asset_guid_by_qualified_name
from model import Column
from test.data import DATABASE, SCHEMA, ENTITY, COLUMN, INTEGRATION_TYPE, DESCRIPTION, DATA_TYPE

# Test for Column for example, it works for schema or table as well
# Need an existing asset to be testable because of the asset guid

asset = Column(integration_type=INTEGRATION_TYPE,
                database_name=DATABASE,
                schema_name=SCHEMA,
                entity_name=ENTITY,
                column_name=COLUMN,
                description=DESCRIPTION,
                data_type=DATA_TYPE)


def test_detach_classification_payload():
    DATA = {
        "entityGuid": get_asset_guid_by_qualified_name(asset.get_qualified_name(), asset.get_atlan_type_name()),
        "displayName": asset.classification,
        "propagate": False,
        "removePropagationsOnEntityDelete": True
    }
    assert json.dumps(DATA) == json.dumps(asset.get_classification_payload_for_bulk_mode())


def test_detach_classification():
    asset_guid = get_asset_guid_by_qualified_name(asset.get_qualified_name(), asset.get_atlan_type_name())
    DATA = {
        "guidHeaderMap": {
            {
                asset_guid: {
                    "guid": asset_guid,
                    "typeName": asset.get_atlan_type_name(),
                    "attributes": {
                        "name": "date",
                        "qualifiedName": asset.get_qualified_name(),
                    },
                    "classifications": []
                }
            }
        }
    }
    assert json.dumps(DATA) == json.dumps(asset.get_detach_classification_payload_for_bulk_mode())
