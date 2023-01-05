import logging
import os
from annotation import auto_str
from model import get_atlan_athena_unique_id, get_atlan_redshift_server_url

from atlanapi.requests import create_column_lineage_request_payload
from constants import INTEGRATION_TYPE_DYNAMO_DB, INTEGRATION_TYPE_ATHENA, \
    INTEGRATION_TYPE_REDSHIFT, DYNAMO_DB_DATABASE_NAME, REDSHIFT_CONN_QN, DYNAMODB_CONN_QN, ATHENA_DATABASE_NAME, \
    ATHENA_CONN_QN, REDSHIFT_DATABASE_NAME

logger = logging.getLogger('main_logger')

ATLAN_PROD_AWS_ACCOUNT_ID = os.environ.get('ATLAN_PROD_AWS_ACCOUNT_ID')
ATLAN_REDSHIFT_SERVER_URL = os.environ.get('ATLAN_REDSHIFT_SERVER_URL')


@auto_str
class ColumnLineage:
    def __init__(self, column, lineage_type=None, lineage_integration_type=None,
                 lineage_schema_name=None, lineage_entity_name=None, lineage_column_name=None, lineage_full_qualified_name=None):
        self.column = column
        self.lineage_type = lineage_type
        self.lineage_integration_type = lineage_integration_type.lower()
        if self.lineage_integration_type == INTEGRATION_TYPE_REDSHIFT:
            # Because lineage_schema_name is a concatenation of the database and schema name. Ex: dwhstats/dwh_stats
            self.lineage_database_name = REDSHIFT_DATABASE_NAME
            self.lineage_schema_name = lineage_schema_name
        elif self.lineage_integration_type == INTEGRATION_TYPE_ATHENA:
            self.lineage_database_name = ATHENA_DATABASE_NAME
            self.lineage_schema_name = lineage_schema_name
        elif self.lineage_integration_type == INTEGRATION_TYPE_DYNAMO_DB:
            self.lineage_database_name = DYNAMO_DB_DATABASE_NAME
            self.lineage_schema_name = lineage_schema_name
        self.lineage_entity_name = lineage_entity_name
        self.lineage_column_name = lineage_column_name
        self.lineage_full_qualified_name = lineage_full_qualified_name

    def get_qualified_name(self):
        if self.lineage_integration_type == INTEGRATION_TYPE_DYNAMO_DB:
            qualified_name = DYNAMODB_CONN_QN + "/{}/{}/{}/{}"
        elif self.lineage_integration_type == INTEGRATION_TYPE_ATHENA:
            qualified_name = ATHENA_CONN_QN + "/" + get_atlan_athena_unique_id(self) + "/{}/{}/{}/{}"
        elif self.lineage_integration_type == INTEGRATION_TYPE_REDSHIFT:
            qualified_name = REDSHIFT_CONN_QN + "/" + get_atlan_redshift_server_url(self) + "/{}/{}/{}/{}"
        else:
            raise Exception("Qualified name not supported yet for integration type {}"
                            .format(self.lineage_integration_type))
        return qualified_name.format(self.lineage_database_name,
                                     self.lineage_schema_name,
                                     self.lineage_entity_name,
                                     self.lineage_column_name)

    def get_asset_name(self):
        return self.lineage_column_name

    def get_atlan_type_name(self):
        return 'Column'

    def get_creation_payload_for_bulk_mode(self):
        return create_column_lineage_request_payload(self)

    def get_creation_payload(self):
        raise Exception("Column are creating in bulk mode only")
