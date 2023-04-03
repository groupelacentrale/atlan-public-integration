import os

from atlanapi.searchAssets import get_asset_guid_by_qualified_name
from atlanapi.searchGlossaryTerms import get_glossary_term_guid_by_name
from constants import INTEGRATION_TYPE_DYNAMO_DB, INTEGRATION_TYPE_ATHENA, INTEGRATION_TYPE_REDSHIFT

GET_CONNECTOR_NAME_INTEGRATION_TYPE = {
    INTEGRATION_TYPE_DYNAMO_DB: 'dynamodb',
    INTEGRATION_TYPE_ATHENA: 'athena',
    INTEGRATION_TYPE_REDSHIFT: 'redshift'
}

"""
Looking for asset attribute database qualified name
- asset is Schema, qualified name :         default/mongodb(2)/database(1)/schema
- get_attribute_qualified_name(asset, 1) -> default/mongodb/database

- asset is Entity, qualified name :         default/mongodb(3)/database(2)/schema(1)/entity
- get_attribute_qualified_name(asset, 1) -> default/mongodb/database/schema
- get_attribute_qualified_name(asset, 2) -> default/mongodb/database
"""


def get_attribute_qualified_name(asset, level):
    return '/'.join(asset.get_qualified_name().split('/')[:-level])


def create_column_request_payload(asset):
    # Faking static variable behaviour to preserve column's order
    if not hasattr(create_column_request_payload, "count_order") and not hasattr(create_column_request_payload,
                                                                                 "current_asset_name"):
        create_column_request_payload.count_order = 0
        create_column_request_payload.current_asset_name = asset.entity_name

    if create_column_request_payload.current_asset_name == asset.entity_name:
        create_column_request_payload.count_order += 1
    else:
        # Next asset, we don't need to reset count_order to 0
        create_column_request_payload.count_order = 1
        create_column_request_payload.current_asset_name = asset.entity_name
    return {
        "typeName": "Column",
        "attributes": {
            "name": asset.column_name,
            "qualifiedName": asset.get_qualified_name(),
            "connectorName": GET_CONNECTOR_NAME_INTEGRATION_TYPE[asset.integration_type],
            "tableName": asset.entity_name,
            "tableQualifiedName": get_attribute_qualified_name(asset, 1),
            "schemaName": asset.schema_name,
            "schemaQualifiedName": get_attribute_qualified_name(asset, 2),
            "databaseName": asset.database_name,
            "databaseQualifiedName": get_attribute_qualified_name(asset, 3),
            "connectionQualifiedName": get_attribute_qualified_name(asset, 4),
            "dataType": asset.data_type,
            "order": create_column_request_payload.count_order,
            "description": asset.description
        },
        "relationshipAttributes": {
            "table": {
                "typeName": "Table",
                "uniqueAttributes": {
                    "qualifiedName": get_attribute_qualified_name(asset, 1)
                }
            }
        }
    }


def create_table_request_payload(asset):
    return {
        "typeName": "Table",
        "attributes": {
            "name": asset.entity_name,
            "qualifiedName": asset.get_qualified_name(),
            "connectorName": GET_CONNECTOR_NAME_INTEGRATION_TYPE[asset.integration_type],
            "schemaName": asset.schema_name,
            "schemaQualifiedName": get_attribute_qualified_name(asset, 1),
            "databaseName": asset.database_name,
            "databaseQualifiedName": get_attribute_qualified_name(asset, 2),
            "connectionQualifiedName": get_attribute_qualified_name(asset, 3),
            "description": asset.description,
            "columnCount": asset.column_count
        },
        "relationshipAttributes": {
            "atlanSchema": {
                "typeName": "Schema",
                "uniqueAttributes": {
                    "qualifiedName": get_attribute_qualified_name(asset, 1)
                }
            }
        }
    }


def create_schema_request_payload(asset):
    return {
        "typeName": "Schema",
        "attributes": {
            "name": asset.schema_name,
            "qualifiedName": asset.get_qualified_name(),
            "databaseName": asset.database_name,
            "description": asset.description,
            "databaseQualifiedName": get_attribute_qualified_name(asset, 1),
            "connectorName": GET_CONNECTOR_NAME_INTEGRATION_TYPE[asset.integration_type],
            "connectionQualifiedName": get_attribute_qualified_name(asset, 2)
        },
        "relationshipAttributes": {
            "database": {
                "typeName": "Database",
                "uniqueAttributes": {
                    "qualifiedName": get_attribute_qualified_name(asset, 1)
                }
            }
        }
    }


def create_column_lineage_request_payload(asset):
    if asset.lineage_type == "Target":
        _lineage_qualified_name = "{}/{}".format(asset.column.get_qualified_name(),
                                                 asset.lineage_full_qualified_name)
        _lineage_name = "{}-{} Transformation".format(asset.column.integration_type, asset.lineage_integration_type)
        process_lineage_qualified_name = "{}/{}".format(get_attribute_qualified_name(asset.column, 1),
                                                        os.path.split(asset.lineage_full_qualified_name)[0])
    else:
        _lineage_qualified_name = "{}/{}".format(asset.lineage_full_qualified_name,
                                                 asset.column.get_qualified_name())
        _lineage_name = "{}-{} Transformation".format(asset.lineage_integration_type, asset.column.integration_type)
        process_lineage_qualified_name = "{}/{}".format(os.path.split(asset.lineage_full_qualified_name)[0],
                                                        get_attribute_qualified_name(asset.column, 1))

    return {
        "typeName": "ColumnProcess",
        "attributes": {
            "name": _lineage_name,
            "qualifiedName": _lineage_qualified_name,
            "connectorName": GET_CONNECTOR_NAME_INTEGRATION_TYPE[asset.lineage_integration_type],
            "connectionQualifiedName": get_attribute_qualified_name(asset.column, 4)
        },
        "relationshipAttributes": {
            "inputs": [
                {
                    "typeName": "Column",
                    "uniqueAttributes": {
                        "qualifiedName": asset.column.get_qualified_name() if asset.lineage_type == "Target" else asset.lineage_full_qualified_name
                    }
                }
            ],
            "outputs": [
                {
                    "typeName": "Column",
                    "uniqueAttributes": {
                        "qualifiedName": asset.lineage_full_qualified_name if asset.lineage_type == "Target" else asset.column.get_qualified_name()
                    }
                }
            ],
            "process": {
                "typeName": "Process",
                "uniqueAttributes": {
                    "qualifiedName": process_lineage_qualified_name
                }
            }
        }
    }


def create_entity_lineage_request_payload(asset):
    if asset.lineage_type == "Target":
        _lineage_qualified_name = "{}/{}".format(asset.table.get_qualified_name(),
                                                 asset.lineage_full_qualified_name)
        _lineage_name = "{}-{} Transformation".format(asset.table.integration_type, asset.lineage_integration_type)
    else:
        _lineage_qualified_name = "{}/{}".format(asset.lineage_full_qualified_name,
                                                 asset.table.get_qualified_name())
        _lineage_name = "{}-{} Transformation".format(asset.lineage_integration_type, asset.table.integration_type)

    return {
        "typeName": "Process",
        "attributes": {
            "name": _lineage_name,
            "qualifiedName": _lineage_qualified_name,
            "connectorName": GET_CONNECTOR_NAME_INTEGRATION_TYPE[asset.lineage_integration_type],
            "connectionQualifiedName": get_attribute_qualified_name(asset.table, 3),

        },
        "relationshipAttributes": {
            "inputs": [
                {
                    "typeName": "Table",
                    "uniqueAttributes": {
                        "qualifiedName": asset.table.get_qualified_name() if asset.lineage_type == "Target" else asset.lineage_full_qualified_name
                    }
                }
            ],
            "outputs": [
                {
                    "typeName": "Table",
                    "uniqueAttributes": {
                        "qualifiedName": asset.lineage_full_qualified_name if asset.lineage_type == "Target" else asset.table.get_qualified_name()
                    }
                }
            ]
        }
    }


def classification_request_payload(asset):
    return {
        "entityGuid": get_asset_guid_by_qualified_name(asset.get_qualified_name(), asset.get_atlan_type_name()),
        "displayName": asset.classification.capitalize(),
        "propagate": False,
        "removePropagationsOnEntityDelete": True
    }


def detach_classification_request_payload(asset):
    asset_guid = get_asset_guid_by_qualified_name(asset.get_qualified_name(), asset.get_atlan_type_name())
    return {
        asset_guid: {
            "guid": asset_guid,
            "typeName": asset.get_atlan_type_name(),
            "attributes": {
                "name": asset.get_asset_name(),
                "qualifiedName": asset.get_qualified_name(),
            },
            "classifications": []
        }
    }


def link_term_request_payload(asset):
    term_guid = get_glossary_term_guid_by_name(asset.term, asset.glossary)
    return {
        "typeName": asset.get_atlan_type_name(),
        "attributes": {
            "name": asset.get_asset_name(),
            "qualifiedName": asset.get_qualified_name()
        },
        "relationshipAttributes": {
            "meanings": [
                {
                    "typeName": "AtlasGlossaryTerm",
                    "guid": term_guid
                }
            ]
        }
    }


def unlink_term_request_payload(asset):
    asset_guid = get_asset_guid_by_qualified_name(asset.get_qualified_name(), asset.get_atlan_type_name())
    return {
        "guid": asset_guid,
        "typeName": asset.get_atlan_type_name(),
        "attributes": {
            "name": asset.get_asset_name(),
            "qualifiedName": asset.get_qualified_name()
        },
        "relationshipAttributes": {
            "meanings": []
        }
    }
