import json

from model import TableLineage, ColumnLineage, Table, Column
from test.data import DATABASE, SCHEMA, ENTITY, ENTITY_OUT, COLUMN, COLUMNOP, INTEGRATION_TYPE, DESCRIPTION, DATA_TYPE, \
    TABLE_LINEAGE_DATA, COLUMN_LINEAGE_DATA

TARGET = "Target"
SOURCE = "SOURCE"


def test_process_lineage_payload():
    entity_src = Table(integration_type=INTEGRATION_TYPE,
                       entity_name=ENTITY,
                       description=DESCRIPTION,
                       database_name=DATABASE,
                       schema_name=SCHEMA
                       )

    entity_target = Table(integration_type=INTEGRATION_TYPE,
                          entity_name=ENTITY_OUT,
                          description=DESCRIPTION,
                          database_name=DATABASE,
                          schema_name=SCHEMA
                          )

    process = TableLineage(table=entity_src,
                           lineage_type=TARGET,
                           lineage_integration_type=INTEGRATION_TYPE,
                           lineage_database_name=entity_target.database_name,
                           lineage_schema_name=entity_target.schema_name,
                           lineage_table_name=entity_target.entity_name,
                           lineage_full_qualified_name=entity_target.get_qualified_name()
                           )

    assert json.dumps(process.get_creation_payload_for_bulk_mode()) == json.dumps(TABLE_LINEAGE_DATA)


def test_column_lineage_payload():
    column_inp = Column(integration_type=INTEGRATION_TYPE,
                        database_name=DATABASE,
                        schema_name=SCHEMA,
                        entity_name=ENTITY,
                        column_name=COLUMN,
                        data_type=DATA_TYPE,
                        )
    column_op = Column(integration_type=INTEGRATION_TYPE,
                       database_name=DATABASE,
                       schema_name=SCHEMA,
                       entity_name=ENTITY_OUT,
                       column_name=COLUMNOP,
                       data_type=DATA_TYPE,
                       )
    col_lineage = ColumnLineage(column_inp,
                                lineage_type=TARGET,
                                lineage_integration_type=INTEGRATION_TYPE,
                                lineage_schema_name=column_op.schema_name,
                                lineage_entity_name=column_op.entity_name,
                                lineage_column_name=column_op.column_name,
                                lineage_full_qualified_name=column_op.get_qualified_name())

    assert json.dumps(col_lineage.get_creation_payload_for_bulk_mode()) == json.dumps(COLUMN_LINEAGE_DATA)
