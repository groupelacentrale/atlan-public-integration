import json
import os

from atlanapi.createAsset import create_assets, create_asset_database
from atlanapi.delete_asset import delete_asset
from atlanapi.searchAssets import get_asset_guid_by_qualified_name, get_asset_by_guid
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
    table = Table(integration_type=INTEGRATION_TYPE,
                  entity_name=ENTITY,
                  description=DESCRIPTION,
                  database_name=DATABASE,
                  schema_name=SCHEMA,
                  readme=README,
                  glossary=GLOSSARY,
                  term=TERM)
    create_assets([table], "createTables")
    table_guid = get_asset_guid_by_qualified_name(table.get_qualified_name(), "Table")
    created_asset = get_asset_by_guid(table_guid)
    if created_asset['entity']['typeName'] == table.get_atlan_type_name() and created_asset['entity']['attributes'][
        'qualifiedName'] == table.get_qualified_name():
        delete_asset(table_guid)
        assert True
    else:
        assert False


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
