import json

from atlanapi.createAsset import create_assets
from atlanapi.searchAssets import get_asset_guid_by_qualified_name
from model.Asset import Entity
from constants import INTEGRATION_TYPE_DYNAMO_DB

import logging

logger = logging.getLogger()

TABLE = "test_table"
INTEGRATION_TYPE = "DynamoDb"
DESCRIPTION = "Test Unit"
DATABASE = "dynamo_db2"
SCHEMA = "test_schema"
README = "test_readme"
TERM = "test_term"
GLOSSARY = "test_glossary"

def test_create_table():
    entity = Entity(integration_type=INTEGRATION_TYPE_DYNAMO_DB,
                    entity_name=TABLE,
                    description=DESCRIPTION,
                    database_name=DATABASE,
                    schema_name=SCHEMA,
                    readme=README,
                    glossary=GLOSSARY,
                    term=TERM)
    create_assets([entity], "createTables")
    logger.info(entity.get_creation_payload())
    assert get_asset_guid_by_qualified_name(entity.get_qualified_name(), "Table") != {}
