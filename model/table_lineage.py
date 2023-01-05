from annotation import auto_str
from model import get_atlan_athena_unique_id, get_atlan_redshift_server_url

from atlanapi.requests import create_entity_lineage_request_payload
from constants import INTEGRATION_TYPE_DYNAMO_DB, INTEGRATION_TYPE_ATHENA, \
    INTEGRATION_TYPE_REDSHIFT, REDSHIFT_CONN_QN, DYNAMODB_CONN_QN, ATHENA_CONN_QN


@auto_str
class TableLineage:
    def __init__(self,
                 table,
                 lineage_type=None,
                 lineage_integration_type=None,
                 lineage_database_name=None,
                 lineage_schema_name=None,
                 lineage_table_name=None,
                 lineage_full_qualified_name=None):
        self.table = table
        self.lineage_type = lineage_type
        self.lineage_integration_type = lineage_integration_type.lower()
        self.lineage_database_name = lineage_database_name
        self.lineage_schema_name = lineage_schema_name
        self.lineage_table_name = lineage_table_name
        self.lineage_full_qualified_name = lineage_full_qualified_name

    def get_qualified_name(self):
        if self.lineage_integration_type == INTEGRATION_TYPE_DYNAMO_DB:
            qualified_name = DYNAMODB_CONN_QN + "/{}/{}/{}"
        elif self.lineage_integration_type == INTEGRATION_TYPE_ATHENA:
            qualified_name = ATHENA_CONN_QN + "/" + get_atlan_athena_unique_id(self) + "/{}/{}/{}"
        elif self.lineage_integration_type == INTEGRATION_TYPE_REDSHIFT:
            qualified_name = REDSHIFT_CONN_QN + "/" + get_atlan_redshift_server_url(self) + "/{}/{}/{}"
        else:
            raise Exception("Qualified name not supported yet for integration type {}"
                            .format(self.lineage_integration_type))
        return qualified_name.format(self.lineage_database_name, self.lineage_schema_name, self.lineage_table_name)

    def get_asset_name(self):
        raise Exception("Not implemented !")

    def get_atlan_type_name(self):
        return 'Process'

    def get_creation_payload_for_bulk_mode(self):
        return create_entity_lineage_request_payload(self)

    def get_creation_payload(self):
        raise Exception("Column are creating in bulk mode only")

    def __repr__(self):
        if self.lineage_type.lower() == "source":
            return "({},{},{},{}) ---> {}".format(
                self.lineage_integration_type,
                self.lineage_database_name,
                self.lineage_schema_name,
                self.lineage_table_name,
                self.table
            )
        else:
            return "{} ---> ({},{},{},{})".format(
                self.table,
                self.lineage_integration_type,
                self.lineage_database_name,
                self.lineage_schema_name,
                self.lineage_table_name,
            )

    def __hash__(self):
        return hash((self.table.__hash__(),
                     self.lineage_type,
                     self.lineage_integration_type,
                     self.lineage_database_name,
                     self.lineage_schema_name,
                     self.lineage_table_name))

    def __eq__(self, other):
        if not isinstance(other, type(self)): return NotImplemented
        return self.table == other.table \
               and self.lineage_type == other.lineage_type \
               and self.lineage_integration_type == other.lineage_integration_type \
               and self.lineage_database_name == other.lineage_database_name \
               and self.lineage_schema_name == other.lineage_schema_name \
               and self.lineage_table_name == other.lineage_table_name
