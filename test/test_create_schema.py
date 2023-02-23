import json
import os
import logging
from atlanapi.createAsset import create_assets, create_asset_database
from atlanapi.delete_asset import delete_asset
from atlanapi.searchAssets import get_asset_guid_by_qualified_name
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
