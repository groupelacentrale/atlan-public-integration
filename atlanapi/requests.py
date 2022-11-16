def get_attribute_qualified_name(asset, level):
    # TODO : Faire le cas de l'intégration GLUE
    return '/'.join(asset.get_qualified_name().split('/')[:-level])


def create_column_request_payload(asset):
    # Faking static variable behaviour to preserve column's order

    if not hasattr(create_column_request_payload, "count_order"):
        create_column_request_payload.count_order = 0
    create_column_request_payload.count_order += 1

    return {
        "typeName": "Column",
        "attributes": {
            "name": asset.column_name,
            "description": asset.description,
            "qualifiedName": asset.get_qualified_name(),
            "connectorName": asset.integration_type,
            "connectionQualifiedName": get_attribute_qualified_name(asset, 4),
            "databaseName": asset.database_name,
            "databaseQualifiedName": get_attribute_qualified_name(asset, 3),
            "schemaName": asset.schema_name,
            "schemaQualifiedName": get_attribute_qualified_name(asset, 2),
            "tableName": asset.entity_name,
            "tableQualifiedName": get_attribute_qualified_name(asset, 1),
            "dataType": asset.data_type,
            "order": create_column_request_payload.count_order,
            "relationshipAttributes": {
                "table": {
                    "typeName": "Table",
                    "uniqueAttributes": {
                        "qualifiedName": get_attribute_qualified_name(asset, 1)
                    }
                }
            }
        }
    }


def create_entity_request_payload(asset):
    return {
        "typeName": "Table",
        "attributes": {
            "description": asset.description,
            "name": asset.entity_name,
            "qualifiedName": asset.get_qualified_name(),
            "connectorName": asset.integration_type,
            "connectionQualifiedName": get_attribute_qualified_name(asset, 3),
            "databaseName": asset.database_name,
            "databaseQualifiedName": get_attribute_qualified_name(asset, 2),
            "schemaName": asset.schema_name,
            "schemaQualifiedName": get_attribute_qualified_name(asset, 1),
        }
    }


def create_schema_request_payload(asset):
    return {
        "typeName": "Schema",
        "attributes": {
            "name": asset.schema_name,
            "description": asset.description,
            "qualifiedName": asset.get_qualified_name(),
            "connectorName": asset.integration_type,
            "connectionQualifiedName": get_attribute_qualified_name(asset, 2),
            "databaseName": asset.database_name,
            "databaseQualifiedName": get_attribute_qualified_name(asset, 1),
        }
    }


def create_column_lineage_request_payload(asset):
    if asset.lineage_type == "Target":
        _lineage_qualified_name = "{}/{}/{}/{}".format(asset.column.integration_type,
                                                       asset.column.get_qualified_name(),
                                                       asset.lineage_integration_type,
                                                       asset.lineage_full_qualified_name)
        _lineage_name = "{}-{} Transformation".format(asset.column.integration_type, asset.lineage_integration_type)
    else:
        _lineage_qualified_name = "{}/{}/{}/{}".format(asset.lineage_integration_type,
                                                       asset.lineage_full_qualified_name,
                                                       asset.column.integration_type,
                                                       asset.column.get_qualified_name())
        _lineage_name = "{}-{} Transformation".format(asset.lineage_integration_type, asset.column.integration_type)

    return {
        "typeName": "AtlanProcess",
        "attributes": {
            "qualifiedName": _lineage_qualified_name,
            "name": _lineage_name,
            "processConfig": [
                {
                    "_data_type": "string",
                    "_value": _lineage_name,
                    "_key": "process_type"
                },
                {
                    "_data_type": "string",
                    "_value": "",
                    "_key": "query"
                }
            ],
            "description": _lineage_name,
            "typeName": "AtlanProcess",
            "inputs": [
                {
                    "uniqueAttributes": {
                        "qualifiedName": asset.column.get_qualified_name() if asset.lineage_type == "Target" else asset.lineage_full_qualified_name
                    },
                    "typeName": "AtlanColumn"
                }
            ],
            "outputs": [
                {
                    "uniqueAttributes": {
                        "qualifiedName": asset.lineage_full_qualified_name if asset.lineage_type == "Target" else asset.column.get_qualified_name()
                    },
                    "typeName": "AtlanColumn"
                }
            ]
        }
    }


def create_entity_lineage_request_payload(asset):
    if asset.lineage_type == "Target":
        _lineage_qualified_name = "{}/{}/{}/{}".format(asset.entity.integration_type,
                                                       asset.entity.get_qualified_name(),
                                                       asset.lineage_integration_type,
                                                       asset.lineage_full_qualified_name)
        _lineage_name = "{}-{} Transformation".format(asset.entity.integration_type, asset.lineage_integration_type)
    else:
        _lineage_qualified_name = "{}/{}/{}/{}".format(asset.lineage_integration_type,
                                                       asset.lineage_full_qualified_name,
                                                       asset.entity.integration_type,
                                                       asset.entity.get_qualified_name())
        _lineage_name = "{}-{} Transformation".format(asset.lineage_integration_type, asset.entity.integration_type)

    return {
        "typeName": "AtlanProcess",
        "attributes": {
            "qualifiedName": _lineage_qualified_name,
            "name": _lineage_name,
            "processConfig": [
                {
                    "_data_type": "string",
                    "_value": _lineage_name,
                    "_key": "process_type"
                },
                {
                    "_data_type": "string",
                    "_value": "",
                    "_key": "query"
                }
            ],
            "description": _lineage_name,
            "typeName": "AtlanProcess",
            "inputs": [
                {
                    "uniqueAttributes": {
                        "qualifiedName": asset.entity.get_qualified_name() if asset.lineage_type == "Target" else asset.lineage_full_qualified_name
                    },
                    "typeName": "AtlanTable"
                }
            ],
            "outputs": [
                {
                    "uniqueAttributes": {
                        "qualifiedName": asset.lineage_full_qualified_name if asset.lineage_type == "Target" else asset.entity.get_qualified_name()
                    },
                    "typeName": "AtlanTable"
                }
            ]
        }
    }
