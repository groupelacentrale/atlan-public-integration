import json
from atlanapi.createAsset import create_assets
from atlanapi.detach_classification import detach_classification
from atlanapi.searchAssets import get_asset_guid_by_qualified_name, get_asset_by_guid
from model import Column
from test.data import DATABASE, SCHEMA, ENTITY, COLUMN, INTEGRATION_TYPE, DESCRIPTION, DATA_TYPE, CLASSIFICATION

# Test for Column for example, it works for schema or table as well
# Need an existing asset to be testable because of the asset guid

asset = Column(integration_type=INTEGRATION_TYPE,
               database_name=DATABASE,
               schema_name=SCHEMA,
               entity_name=ENTITY,
               column_name=COLUMN,
               description=DESCRIPTION,
               classification=CLASSIFICATION,
               data_type=DATA_TYPE)

DATA = {
    "entityGuid": get_asset_guid_by_qualified_name(asset.get_qualified_name(), asset.get_atlan_type_name()),
    "displayName": asset.classification,
    "propagate": False,
    "removePropagationsOnEntityDelete": True
}


def test_detach_classification_payload():
    assert json.dumps(DATA) == json.dumps(asset.get_classification_payload_for_bulk_mode())


def test_detach_classification_payload():
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


def test_attach_classification():
    # It creates the asset with its classification
    create_assets([asset], "createColumns")
    asset_guid = get_asset_guid_by_qualified_name(asset.get_qualified_name(), "Column")
    created_asset = get_asset_by_guid(asset_guid)
    if created_asset['entity']['classifications'] is not None:
        detach_classification([asset])
        asset_detached_classification = get_asset_by_guid(asset_guid)
        assert not hasattr(asset_detached_classification['entity'],'classifications')
    else:
        assert False