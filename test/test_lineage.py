import json

from constants import DYNAMODB_CONN_QN, INTEGRATION_TYPE_DYNAMO_DB, INTEGRATION_TYPE_ATHENA, ATHENA_CONN_QN, \
    INTEGRATION_TYPE_REDSHIFT, REDSHIFT_CONN_QN
from model.Asset import EntityLineage, ColumnLineage, Entity, Column

COLUMN = "col_index"
COLUMNOP = "col_index_output"
ENTITY = "test_table"
ENTITY_OUT = "test_table_out"
INTEGRATION_TYPE = INTEGRATION_TYPE_DYNAMO_DB
DESCRIPTION = "Test Unit"
DATABASE = "dynamo_db2"
DATATYPE = "INT"
SCHEMA = "test_schema"
TARGET = "Target"
SOURCE = "SOURCE"

#Default connection qualified name is dynamo
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

DATA_PROCESS = {
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

DATA_COLUMN_PROCESS = {
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


def test_process_lineage_payload():
    entity_src = Entity(integration_type=INTEGRATION_TYPE,
                        entity_name=ENTITY,
                        description=DESCRIPTION,
                        database_name=DATABASE,
                        schema_name=SCHEMA
                        )

    entity_target = Entity(integration_type=INTEGRATION_TYPE,
                           entity_name=ENTITY_OUT,
                           description=DESCRIPTION,
                           database_name=DATABASE,
                           schema_name=SCHEMA
                           )

    process = EntityLineage(entity=entity_src,
                            lineage_type=TARGET,
                            lineage_integration_type=INTEGRATION_TYPE,
                            lineage_database_name=entity_target.database_name,
                            lineage_schema_name=entity_target.schema_name,
                            lineage_entity_name=entity_target.entity_name,
                            lineage_full_qualified_name=entity_target.get_qualified_name()
                            )

    assert json.dumps(process.get_creation_payload_for_bulk_mode()) == json.dumps(DATA_PROCESS)


def test_column_lineage_payload():

    column_inp = Column(integration_type=INTEGRATION_TYPE,
                        database_name=DATABASE,
                        schema_name=SCHEMA,
                        entity_name=ENTITY,
                        column_name=COLUMN,
                        data_type=DATATYPE,
                        )
    column_op = Column(integration_type=INTEGRATION_TYPE,
                       database_name=DATABASE,
                       schema_name=SCHEMA,
                       entity_name=ENTITY_OUT,
                       column_name=COLUMNOP,
                       data_type=DATATYPE,
                       )
    col_lineage = ColumnLineage(column_inp,
                                lineage_type=TARGET,
                                lineage_integration_type=INTEGRATION_TYPE,
                                lineage_schema_name=column_op.schema_name,
                                lineage_entity_name=column_op.entity_name,
                                lineage_column_name=column_op.column_name,
                                lineage_full_qualified_name=column_op.get_qualified_name())

    assert json.dumps(col_lineage.get_creation_payload_for_bulk_mode()) == json.dumps(DATA_COLUMN_PROCESS)