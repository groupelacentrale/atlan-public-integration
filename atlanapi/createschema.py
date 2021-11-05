import json

class AtlanSchema:
    def __init__(self, integration_type, name, qualified_name):
        self.integration_type = integration_type
        self.name = name
        self.qualified_name = qualified_name


class AtlanSchemaSerializer:
    """
    Creates the request to create an Atlan Schema.
    """
    def serialize(self, atlanschema):
        schema_info = {"entities": [
            {
                "typeName": "AtlanSchema",
                "attributes": {
                    "typeName": "AtlanSchema",
                    "integrationType": atlanschema.integration_type,
                    "qualifiedName": atlanschema.qualified_name,
                    "name": atlanschema.name
                }
            }
        ]}
        return json.dumps(schema_info)