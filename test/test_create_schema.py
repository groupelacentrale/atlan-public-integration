import json
from model.Asset import Schema
from constants import INTEGRATION_TYPE_DYNAMO_DB

TABLE = "test_table"
SCHEMA = "test_schema"
INTEGRATION_TYPE = "DynamoDB"
DESCRIPTION = "test schema atlan"
TERM = "test_term"
README = "test_schema readme"
GLOSSARY = "test_schema glossary"
DATA = {"entities": [
    {
        "typeName": "AtlanSchema",
        "attributes": {
            "typeName": "AtlanSchema",
            "integrationType": INTEGRATION_TYPE,
            "qualifiedName": "dynamodb/dynamodb.atlan.com/dynamo_db/{}".format(TABLE),
            "name": TABLE,
            "description": DESCRIPTION
        }
    }
]}


def test_create_schema():
    schema = Schema(database_name=TABLE,
                    integration_type=INTEGRATION_TYPE_DYNAMO_DB,
                    schema_name=TABLE,
                    description=DESCRIPTION,
                    readme=README,
                    term=TERM,
                    glossary=GLOSSARY)
    s_payload = schema.get_creation_payload()
    assert json.dumps(DATA) == s_payload
