import json
from atlanapi.createAsset import create_assets
from atlanapi.delete_asset import delete_asset
from atlanapi.searchAssets import get_asset_guid_by_qualified_name, get_asset_by_guid
from model.schema import Schema
from test.data import DATABASE, SCHEMA, INTEGRATION_TYPE, DESCRIPTION, README, TERM, GLOSSARY, SCHEMA_DATA


def test_schema_payload():
    schema = Schema(database_name=DATABASE,
                    integration_type=INTEGRATION_TYPE,
                    schema_name=SCHEMA,
                    description=DESCRIPTION,
                    readme=README,
                    term=TERM,
                    glossary=GLOSSARY)
    schema_payload = schema.get_creation_payload()
    assert json.dumps(SCHEMA_DATA) == schema_payload


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
    created_asset = get_asset_by_guid(schema_guid)
    if created_asset['entity']['typeName'] == schema.get_atlan_type_name() and created_asset['entity']['attributes']['qualifiedName'] == schema.get_qualified_name():
        delete_asset(schema_guid)
        assert True
    else:
        assert False



