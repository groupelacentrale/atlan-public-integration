import json
import os

class AtlanColumn:
    def __init__(self, integration_type, name, data_type, description, qualified_name):
        self.integration_type = integration_type
        self.name = name
        self.data_type = data_type
        self.description = description
        self.qualified_name = qualified_name


class AtlanColumnEntityGenerator:
    """
    Returns a dictionary entry for a single column definition
    """
    def create_column_entity(self, atlancolumn):
        column_info = {
                "typeName": "AtlanColumn",
                "attributes": {
                    "typeName": "AtlanColumn",
                    "description": atlancolumn.description,
                    "integrationType": atlancolumn.integration_type,
                    "qualifiedName": atlancolumn.qualified_name,
                    "name": atlancolumn.name,
                    "order": 1,
                    "dataType": atlancolumn.data_type,
                    "table": {
                        "uniqueAttributes": {
                            "qualifiedName": os.path.split(atlancolumn.qualified_name)[0]
                        },
                        "typeName": "AtlanTable"
                    }
                }
        }
        return column_info


class AtlanColumnSerializer:
    def serialize(self, atlancolumns):
        payload = {"entities": atlancolumns}
        return json.dumps(payload)