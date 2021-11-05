import json

class AtlanTable:
    def __init__(self, integration_type, name, qualified_name, description):
        self.integration_type = integration_type
        self.name = name
        self.qualified_name = qualified_name
        self.description = description


class AtlanTableSerializer:
    """
    Creates the request to create an Atlan table and its description.
    """
    def serialize(self, atlantable):
        table_info = {"entities": [
            {
                "typeName": "AtlanTable",
                "attributes": {
                    "qualifiedName": atlantable.qualified_name,
                    "description": atlantable.description,
                    "name": atlantable.name,
                    "integrationType": atlantable.integration_type,
                    "typeName": "AtlanTable"
                }
            }
        ]}
        return json.dumps(table_info)