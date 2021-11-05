import json
from atlanapi.createschema import AtlanSchema, AtlanSchemaSerializer

TABLE = "test"
INTEGRATION_TYPE = "DynamoDb"
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
    schema = AtlanSchema(integration_type=INTEGRATION_TYPE,
                         name=TABLE,
                         qualified_name="dynamodb/dynamodb.atlan.com/dynamo_db/{}".format(TABLE))
    s_payload = AtlanSchemaSerializer()
    schema_payload = s_payload.serialize(schema)
    assert json.dumps(DATA) == schema_payload
