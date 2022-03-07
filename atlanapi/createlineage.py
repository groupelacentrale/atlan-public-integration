import json

class AtlanLineage:
    def __init__(self, source_integration_type, source_qualified_name, target_integration_type, target_qualified_name, asset_type):
        self.source_integration_type = source_integration_type
        self.source_qualified_name = source_qualified_name
        self.target_integration_type = target_integration_type
        self.target_qualified_name = target_qualified_name
        self.asset_type = asset_type
        self._lineage_qualified_name = "{}/{}/{}/{}".format(source_integration_type, source_qualified_name, target_integration_type, target_qualified_name)
        self._lineage_name = "{}-{} Transformation".format(source_integration_type, target_integration_type)


class AtlanLineageEntityGenerator:
    """
    Returns a dictionary entry for a single column-column lineage definition
    """
    def create_lineage_entity(self, atlanlineage):
        lineage_info = {
                "typeName": "AtlanProcess",
                "attributes": {
                    "qualifiedName": atlanlineage._lineage_qualified_name,
                    "name": atlanlineage._lineage_name,
                    "processConfig": [
                        {
                            "_data_type": "string",
                            "_value": atlanlineage._lineage_name,
                            "_key": "process_type"
                        },
                        {
                            "_data_type": "string",
                            "_value": "",
                            "_key": "query"
                        }
                    ],
                    "description": atlanlineage._lineage_name,
                    "typeName": "AtlanProcess",
                    "inputs": [
                        {
                            "uniqueAttributes": {
                                "qualifiedName": atlanlineage.source_qualified_name
                            },
                            "typeName": atlanlineage.asset_type
                        }
                    ],
                    "outputs": [
                        {
                            "uniqueAttributes": {
                                "qualifiedName": atlanlineage.target_qualified_name
                            },
                            "typeName": atlanlineage.asset_type
                        }
                    ]
                }
        }
        return lineage_info


class AtlanLineageSerializer:
    def serialize(self, atlanlineageentities):
        payload = {"entities": atlanlineageentities}
        return json.dumps(payload)