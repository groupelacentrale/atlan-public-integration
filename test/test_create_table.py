import json
from model.Asset import Entity
from utils import INTEGRATION_TYPE_DYNAMO_DB

TABLE = "test_table"
ENTITY = "test_entity"
INTEGRATION_TYPE = INTEGRATION_TYPE_DYNAMO_DB
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
    schema = Entity(integration_type=INTEGRATION_TYPE_DYNAMO_DB,
                    entity_name=ENTITY,
                    description=DESCRIPTION)
    s_payload = schema.get_creation_payload()
    assert json.dumps(DATA) == s_payload
