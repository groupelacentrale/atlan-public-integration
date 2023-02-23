import json

from atlanapi.searchAssets import get_asset_guid_by_qualified_name
from atlanapi.searchGlossaryTerms import get_glossary_term_guid_by_name
from model import Table
from test.data import DATABASE, SCHEMA, ENTITY, INTEGRATION_TYPE, DESCRIPTION, README, TERM, GLOSSARY

# Asset must exist and term in Atlan

asset = Table(integration_type=INTEGRATION_TYPE,
              entity_name=ENTITY,
              description=DESCRIPTION,
              database_name=DATABASE,
              schema_name=SCHEMA,
              readme=README,
              glossary=GLOSSARY,
              term=TERM)


def test_link_term_payload():
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
