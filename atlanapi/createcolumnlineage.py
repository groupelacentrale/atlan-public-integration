import json

class AtlanColumnLineage:
    def __init__(self, source_integration_type, source_qualified_name, target_integration_type, target_qualified_name):
        self.source_integration_type = source_integration_type
        self.source_qualified_name = source_qualified_name
        self.target_integration_type = target_integration_type
        self.target_qualified_name = target_qualified_name
        self._lineage_qualified_name = "{}/{}/{}/{}".format(source_integration_type, source_qualified_name, target_integration_type, target_qualified_name)
        self._lineage_name = "{}-{} Transformation".format(source_integration_type, target_integration_type)


class AtlanColumnLineageEntityGenerator:
    """
    Returns a dictionary entry for a single column-column lineage definition
    """
    def create_column_lineage_entity(self, atlancolumnlineage):
        column_lineage_info = {
                "typeName": "AtlanProcess",
                "attributes": {
                    "qualifiedName": atlancolumnlineage._lineage_qualified_name,
                    "name": atlancolumnlineage._lineage_name,
                    "processConfig": [
                        {
                            "_data_type": "string",
                            "_value": atlancolumnlineage._lineage_name,
                            "_key": "process_type"
                        },
                        {
                            "_data_type": "string",
                            "_value": "",
                            "_key": "query"
                        }
                    ],
                    "description": atlancolumnlineage._lineage_name,
                    "typeName": "AtlanProcess",
                    "inputs": [
                        {
                            "uniqueAttributes": {
                                "qualifiedName": atlancolumnlineage.source_qualified_name
                            },
                            "typeName": "AtlanColumn"
                        }
                    ],
                    "outputs": [
                        {
                            "uniqueAttributes": {
                                "qualifiedName": atlancolumnlineage.target_qualified_name
                            },
                            "typeName": "AtlanColumn"
                        }
                    ]
                }
        }
        return column_lineage_info


class AtlanColumnLineageSerializer:
    def serialize(self, atlanlineagecolumns):
        payload = {"entities": atlanlineagecolumns}
        return json.dumps(payload)