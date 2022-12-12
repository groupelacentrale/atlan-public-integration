import json
import logging
from atlanapi.createAsset import create_assets
from atlanapi.searchAssets import get_asset_guid_by_qualified_name
from model.Asset import Schema
from constants import INTEGRATION_TYPE_DYNAMO_DB

logger = logging.getLogger("test_create_schema")
DATABASE = "dynamo_db2"
SCHEMA = "test_schema"
TABLE = "test_table"
INTEGRATION_TYPE = "DynamoDB"
DESCRIPTION = "test schema atlan"
TERM = "test_term"
README = "test_schema readme"
GLOSSARY = "test_schema glossary"


def test_create_schema():
    schema = Schema(database_name=DATABASE,
                    integration_type=INTEGRATION_TYPE_DYNAMO_DB,
                    schema_name=SCHEMA,
                    description=DESCRIPTION,
                    readme=README,
                    term=TERM,
                    glossary=GLOSSARY)
    logger.info(schema.get_creation_payload())
    create_assets([schema], "createSchemas")
    assert get_asset_guid_by_qualified_name(schema.get_qualified_name(), "Schema") != {}
