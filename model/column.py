import logging
import os
from annotation import auto_str
from model import get_atlan_athena_connection_id, get_atlan_redshift_connection_id, get_database

from atlanapi.requests import create_column_request_payload
from constants import INTEGRATION_TYPE_DYNAMO_DB, INTEGRATION_TYPE_ATHENA, \
    INTEGRATION_TYPE_REDSHIFT, REDSHIFT_CONN_QN, DYNAMODB_CONN_QN, ATHENA_CONN_QN

logger = logging.getLogger('main_logger')


@auto_str
class Column:
    def __init__(self,
                 integration_type,
                 database_name,
                 schema_name,
                 entity_name,
                 column_name,
                 data_type=None,
                 description=None,
                 readme=None,
                 term=None,
                 glossary=None):
        self.integration_type = integration_type.lower()
        self.database_name = database_name
        self.schema_name = schema_name.lower()
        self.entity_name = entity_name.lower()
        self.column_name = column_name.lower()
        self.data_type = data_type
        self.description = description
        self.readme = readme
        self.term = term
        self.glossary = glossary

    def get_qualified_name(self):
        if self.integration_type == INTEGRATION_TYPE_DYNAMO_DB:
            qualified_name = DYNAMODB_CONN_QN  + "/" + \
                             get_database(self.integration_type) + "/{}/{}/{}"
        elif self.integration_type == INTEGRATION_TYPE_ATHENA:
            qualified_name = ATHENA_CONN_QN + "/" + get_atlan_athena_connection_id(self) + "/" + \
                             get_database(self.integration_type) + "/{}/{}/{}"
        elif self.integration_type == INTEGRATION_TYPE_REDSHIFT:
            qualified_name = REDSHIFT_CONN_QN + "/" + get_atlan_redshift_connection_id(self) + "/" + \
                             get_database(self.integration_type) + "/{}/{}/{}"
        else:
            raise Exception("Qualified name not supported yet for integration type {}"
                            .format(self.integration_type))
        return qualified_name.format(self.schema_name, self.entity_name, self.column_name)

    def get_asset_name(self):
        return self.column_name

    def get_atlan_type_name(self):
        return 'Column'

    def get_creation_payload_for_bulk_mode(self):
        return create_column_request_payload(self)

    def get_creation_payload(self):
        raise Exception("Column are creating in bulk mode only")

    def __eq__(self, other):
        if isinstance(other, Column):
            return self.integration_type == other.integration_type and self.database_name == other.database_name \
                   and self.schema_name == other.schema_name and self.entity_name == other.entity_name \
                   and self.column_name == other.column_name and self.data_type == other.data_type
        else:
            return False

    def __hash__(self):
        return hash((self.integration_type, self.database_name, self.schema_name,
                     self.entity_name, self.column_name, self.data_type))
