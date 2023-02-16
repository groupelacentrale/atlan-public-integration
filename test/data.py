from constants import INTEGRATION_TYPE_DYNAMO_DB, INTEGRATION_TYPE_ATHENA, INTEGRATION_TYPE_REDSHIFT, DYNAMODB_CONN_QN, \
    ATHENA_CONN_QN, REDSHIFT_CONN_QN

DATABASE = "dynamo_db"

COLUMN = "col_index"
COLUMNOP = "col_index_output"
SCHEMA = "test_schema"

ENTITY = "test_table"
ENTITY_OUT = "test_table_out"

COLUMN = "col_index"
COLUMNOP = "col_index_output"

INTEGRATION_TYPE = INTEGRATION_TYPE_DYNAMO_DB
DESCRIPTION = "Test integration schema test_schema"
TERM = "test_term"
README = "test_schema readme"
GLOSSARY = "test_schema glossary"
DATA_TYPE = "INT"

# Default connection qualified name is dynamo
CONN_QN = DYNAMODB_CONN_QN
CONN_NAME = INTEGRATION_TYPE_DYNAMO_DB.lower()

if INTEGRATION_TYPE == INTEGRATION_TYPE_ATHENA:
    CONN_NAME = ATHENA_CONN_QN
    CONN_QN = ATHENA_CONN_QN + "/athena-tmp-test"
elif INTEGRATION_TYPE == INTEGRATION_TYPE_REDSHIFT:
    CONN_NAME = REDSHIFT_CONN_QN
    CONN_QN = REDSHIFT_CONN_QN + "/redshift-tmp"


IN_PROC = "{}/{}/{}/{}".format(CONN_QN, DATABASE, SCHEMA, ENTITY)
OUT_PROC = "{}/{}/{}/{}".format(CONN_QN, DATABASE, SCHEMA, ENTITY_OUT)

IN_COL_PROC = "{}/{}/{}/{}/{}".format(CONN_QN, DATABASE, SCHEMA, ENTITY, COLUMN)
OUT_COL_PROC = "{}/{}/{}/{}/{}".format(CONN_QN, DATABASE, SCHEMA, ENTITY_OUT, COLUMNOP)


SCHEMA_DATA = {
    "entities": [
        {
            "typeName": "Schema",
            "attributes": {
                "name": SCHEMA,
                "qualifiedName": "{}/{}/{}".format(CONN_QN, DATABASE, SCHEMA),
                "databaseName": DATABASE,
                "description": DESCRIPTION,
                "databaseQualifiedName": "{}/{}".format(CONN_QN, DATABASE),
                "connectorName": CONN_NAME,
                "connectionQualifiedName": CONN_QN
            },
            "relationshipAttributes": {
                "database": {
                    "typeName": "Database",
                    "uniqueAttributes": {
                        "qualifiedName": "{}/{}".format(CONN_QN, DATABASE)
                    }
                }
            }
        }
    ]
}


TABLE_DATA = {"entities": [
    {
        "typeName": "Table",
        "attributes": {
            "name": ENTITY,
            "qualifiedName": "{}/{}/{}/{}".format(CONN_QN, DATABASE, SCHEMA, ENTITY),
            "connectorName": CONN_NAME,
            "schemaName": SCHEMA,
            "schemaQualifiedName": "{}/{}/{}".format(CONN_QN, DATABASE, SCHEMA),
            "databaseName": DATABASE,
            "databaseQualifiedName": "{}/{}".format(CONN_QN, DATABASE),
            "connectionQualifiedName": CONN_QN,
            "description": DESCRIPTION,
            "columnCount": None
        },
        "relationshipAttributes": {
            "atlanSchema": {
                "typeName": "Schema",
                "uniqueAttributes": {
                    "qualifiedName": "{}/{}/{}".format(CONN_QN, DATABASE, SCHEMA)
                }
            }
        }
    }
]}


COLUMN_DATA = {
    "typeName": "Column",
    "attributes": {
        "name": COLUMN,
        "qualifiedName": "{}/{}/{}/{}/{}".format(CONN_QN, DATABASE, SCHEMA, ENTITY, COLUMN),
        "connectorName": CONN_NAME,
        "tableName": ENTITY,
        "tableQualifiedName": "{}/{}/{}/{}".format(CONN_QN, DATABASE, SCHEMA, ENTITY),
        "schemaName": SCHEMA,
        "schemaQualifiedName": "{}/{}/{}".format(CONN_QN, DATABASE, SCHEMA),
        "databaseName": DATABASE,
        "databaseQualifiedName": "{}/{}".format(CONN_QN, DATABASE),
        "connectionQualifiedName": CONN_QN,
        "dataType": DATA_TYPE,
        "order": 1,
        "description": DESCRIPTION
    },
    "relationshipAttributes": {
        "table": {
            "typeName": "Table",
            "uniqueAttributes": {
                "qualifiedName": "{}/{}/{}/{}".format(CONN_QN, DATABASE, SCHEMA, ENTITY)
            }
        }
    }
}


TABLE_LINEAGE_DATA = {
    "typeName": "Process",
    "attributes": {
        "name": "{}-{} Transformation".format(INTEGRATION_TYPE, INTEGRATION_TYPE),
        "qualifiedName": "{}/{}".format(IN_PROC, OUT_PROC),
        "connectorName": CONN_NAME,
        "connectionQualifiedName": CONN_QN
    },
    "relationshipAttributes": {
        "inputs": [
            {
                "typeName": "Table",
                "uniqueAttributes": {
                    "qualifiedName": IN_PROC
                }
            }
        ],
        "outputs": [
            {
                "typeName": "Table",
                "uniqueAttributes": {
                    "qualifiedName": OUT_PROC
                }
            }
        ]
    }
}


COLUMN_LINEAGE_DATA = {
    "typeName": "ColumnProcess",
    "attributes": {
        "name": "{}-{} Transformation".format(INTEGRATION_TYPE, INTEGRATION_TYPE),
        "qualifiedName": "{}/{}".format(IN_COL_PROC, OUT_COL_PROC),
        "connectorName": CONN_NAME,
        "connectionQualifiedName": CONN_QN
    },
    "relationshipAttributes": {
        "inputs": [
            {
                "typeName": "Column",
                "uniqueAttributes": {
                    "qualifiedName": IN_COL_PROC
                }
            }
        ],
        "outputs": [
            {
                "typeName": "Column",
                "uniqueAttributes": {
                    "qualifiedName": OUT_COL_PROC
                }
            }
        ],
        "process": {
            "typeName": "Process",
            "uniqueAttributes": {
                "qualifiedName": "{}/{}".format(IN_PROC, OUT_PROC)
            }
        }
    }
}
