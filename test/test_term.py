import json

from atlanapi.createAsset import create_assets
from atlanapi.linkTerm import link_term
from atlanapi.searchAssets import get_asset_guid_by_qualified_name, get_asset_by_guid
from atlanapi.searchGlossaryTerms import get_glossary_term_guid_by_name
from atlanapi.unlink_term import unlink_term
from model import Table
from test.data import DATABASE, SCHEMA, ENTITY, INTEGRATION_TYPE, DESCRIPTION, README, TERM, GLOSSARY


asset = Table(integration_type=INTEGRATION_TYPE,
              entity_name=ENTITY,
              description=DESCRIPTION,
              database_name=DATABASE,
              schema_name=SCHEMA,
              readme=README,
              glossary=GLOSSARY,
              term=TERM)


def test_link_term_payload():
    # Asset must exist and term in Atlan
    term_guid = get_glossary_term_guid_by_name(asset.term, asset.glossary)
    DATA = {
        "typeName": asset.get_atlan_type_name(),
        "attributes": {
            "name": asset.get_asset_name(),
            "qualifiedName": asset.get_qualified_name()
        },
        "relationshipAttributes": {
            "meanings": [
                {
                    "typeName": "AtlasGlossaryTerm",
                    "guid": term_guid
                }
            ]
        }
    }

    assert json.dumps(DATA) == json.dumps(asset.get_link_term_payload_for_bulk_mode())


def test_unlink_term_payload():
    # Asset must exist and term in Atlan
    asset_guid = get_asset_guid_by_qualified_name(asset.get_qualified_name(), asset.get_atlan_type_name())
    DATA = {
        "guid": asset_guid,
        "typeName": asset.get_atlan_type_name(),
        "attributes": {
            "name": asset.get_asset_name(),
            "qualifiedName": asset.get_qualified_name()
        },
        "relationshipAttributes": {
            "meanings": []
        }
    }

    assert json.dumps(DATA) == json.dumps(asset.get_unlink_term_payload_for_bulk_mode())

def test_link_term():
    #Creating the asset
    create_assets([asset], "createTables")
    asset_guid = get_asset_guid_by_qualified_name(asset.get_qualified_name(), asset.get_atlan_type_name())
    created_asset = get_asset_by_guid(asset_guid)

    term_guid = get_glossary_term_guid_by_name(asset.term, asset.glossary)
    if created_asset['entity']['relationshipAttributes']['meanings'][0]['guid'] == term_guid:
        unlink_term([asset])
        created_asset = get_asset_by_guid(asset_guid)
        assert not hasattr(created_asset['entity']['relationshipAttributes'], 'meanings')
    else:
        assert False