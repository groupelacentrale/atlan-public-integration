import os

from utils import INTEGRATION_TYPE_DYNAMO_DB, INTEGRATION_TYPE_GLUE, INTEGRATION_TYPE_REDSHIFT

GET_REQUEST_INTEGRATION_TYPE = {
    INTEGRATION_TYPE_DYNAMO_DB: 'DynamoDb',
    INTEGRATION_TYPE_GLUE: 'Glue',
    INTEGRATION_TYPE_REDSHIFT: 'Redshift',
}

def create_column_request_payload(asset):
    return {
        "typeName": "AtlanColumn",
        "attributes": {
            "typeName": "AtlanColumn",
            "description": asset.description,
            "integrationType": GET_REQUEST_INTEGRATION_TYPE[asset.integration_type],
            "qualifiedName": asset.get_qualified_name(),
            "name": asset.column_name,
            "order": 1,
            "dataType": asset.data_type,
            "table": {
                "uniqueAttributes": {
                    "qualifiedName": os.path.split(asset.get_qualified_name())[0]
                },
                "typeName": "AtlanTable"
            }
        }
    }


def create_entity_request_payload(asset):
    return {
        "typeName": "AtlanTable",
        "attributes": {
            "qualifiedName": asset.get_qualified_name(),
            "description": asset.description,
            "name": asset.entity_name,
            "integrationType": GET_REQUEST_INTEGRATION_TYPE[asset.integration_type],
            "typeName": "AtlanTable"
        }
    }


def create_schema_request_payload(asset):
    return {
        "typeName": "AtlanSchema",
        "attributes": {
            "typeName": "AtlanSchema",
            "description": asset.description,
            "integrationType": GET_REQUEST_INTEGRATION_TYPE[asset.integration_type],
            "qualifiedName": asset.get_qualified_name(),
            "name": asset.schema_name
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
