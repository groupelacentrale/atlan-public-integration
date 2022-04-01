import json
from model.Asset import Schema
from constants import INTEGRATION_TYPE_DYNAMO_DB

TABLE = "test"
INTEGRATION_TYPE = INTEGRATION_TYPE_DYNAMO_DB
DATA = {"entities": [
            {
                "typeName": "AtlanSchema",
                "attributes": {
                    "typeName": "AtlanSchema",
                    "integrationType": INTEGRATION_TYPE,
                    "qualifiedName": "dynamodb/dynamodb.atlan.com/dynamo_db/{}".format(TABLE),
                    "name": TABLE

            }
        }
    ]}

def test_create_schema():
    schema = Schema(integration_type=INTEGRATION_TYPE,
                         schema_name=TABLE)
    s_payload = schema.get_creation_payload()
    assert json.dumps(DATA) == s_payload
