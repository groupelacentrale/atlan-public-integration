import json
from atlanapi.createtable import AtlanTable, AtlanTableSerializer

TABLE = "test_table"
ENTITY = "test_entity"
INTEGRATION_TYPE = "DynamoDb"
DESCRIPTION = "Example Description"
DATA = {"entities": [
            {
                "typeName": "AtlanTable",
                "attributes": {
                    "qualifiedName": "dynamodb/dynamodb.atlan.com/dynamo_db/{}/{}".format(TABLE, ENTITY),
                    "description": DESCRIPTION,
                    "name": ENTITY,
                    "integrationType": INTEGRATION_TYPE,
                    "typeName": "AtlanTable"
                }
            }
        ]}

def test_create_table():
    schema = AtlanTable(integration_type="DynamoDb",
                         name=ENTITY,
                         description = DESCRIPTION,
                         qualified_name="dynamodb/dynamodb.atlan.com/dynamo_db/{}/{}".format(TABLE, ENTITY))
    s_payload = AtlanTableSerializer()
    schema_payload = s_payload.serialize(schema)
    assert json.dumps(DATA) == schema_payload

