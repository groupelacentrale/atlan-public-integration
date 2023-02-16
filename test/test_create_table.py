import json
import os

from atlanapi.createAsset import create_assets, create_asset_database
from atlanapi.delete_asset import delete_asset
from atlanapi.searchAssets import get_asset_guid_by_qualified_name
from model import Table
from test.data import DATABASE, SCHEMA, ENTITY, INTEGRATION_TYPE, DESCRIPTION, README, TERM, GLOSSARY, TABLE_DATA


def test_table_payload():
    entity = Table(integration_type=INTEGRATION_TYPE,
                   entity_name=ENTITY,
                   description=DESCRIPTION,
                   database_name=DATABASE,
                   schema_name=SCHEMA,
                   readme=README,
                   glossary=GLOSSARY,
                   term=TERM)
    entity_payload = entity.get_creation_payload()
    assert json.dumps(TABLE_DATA) == entity_payload


def test_create_table():
    entity = Table(integration_type=INTEGRATION_TYPE,
                   entity_name=ENTITY,
                   description=DESCRIPTION,
                   database_name=DATABASE,
                   schema_name=SCHEMA,
                   readme=README,
                   glossary=GLOSSARY,
                   term=TERM)
    create_assets([entity], "createTables")
    entity_guid = get_asset_guid_by_qualified_name(entity.get_qualified_name(), "Table")
    assert delete_asset(entity_guid)


def test_create_entity_database():
    entity = Table(integration_type=INTEGRATION_TYPE,
                   entity_name=ENTITY,
                   description=DESCRIPTION,
                   database_name=DATABASE,
                   schema_name=SCHEMA,
                   readme=README,
                   glossary=GLOSSARY,
                   term=TERM)
    create_asset_database(entity)
    schema_qualified_name = os.path.split(entity.get_qualified_name())[0]
    database_qualified_name = os.path.split(schema_qualified_name)[0]
    database_guid = get_asset_guid_by_qualified_name(database_qualified_name, "Database")
    assert delete_asset(database_guid)
