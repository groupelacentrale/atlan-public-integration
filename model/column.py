import logging
import os
from annotation import auto_str
from model import get_atlan_athena_unique_id, get_atlan_redshift_server_url

from atlanapi.requests import create_column_request_payload
from constants import INTEGRATION_TYPE_DYNAMO_DB, INTEGRATION_TYPE_ATHENA, \
    INTEGRATION_TYPE_REDSHIFT, REDSHIFT_CONN_QN, DYNAMODB_CONN_QN, ATHENA_CONN_QN

logger = logging.getLogger('main_logger')

ATLAN_PROD_AWS_ACCOUNT_ID = os.environ.get('ATLAN_PROD_AWS_ACCOUNT_ID')
ATLAN_REDSHIFT_SERVER_URL = os.environ.get('ATLAN_REDSHIFT_SERVER_URL')


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
        self.schema_name = schema_name
        self.entity_name = entity_name
        self.column_name = column_name
        self.data_type = data_type
        self.description = description
        self.readme = readme
        self.term = term
        self.glossary = glossary

    def get_qualified_name(self):
        if self.integration_type == INTEGRATION_TYPE_DYNAMO_DB:
            qualified_name = DYNAMODB_CONN_QN + "/{}/{}/{}/{}"
        elif self.integration_type == INTEGRATION_TYPE_ATHENA:
            qualified_name = ATHENA_CONN_QN + "/" + get_atlan_athena_unique_id(self) + "/{}/{}/{}/{}"
        elif self.integration_type == INTEGRATION_TYPE_REDSHIFT:
            qualified_name = REDSHIFT_CONN_QN + "/" + get_atlan_redshift_server_url(self) + "/{}/{}/{}/{}"
        else:
            raise Exception("Qualified name not supported yet for integration type {}"
                            .format(self.integration_type))
        return qualified_name.format(self.database_name, self.schema_name, self.entity_name, self.column_name)

    def get_asset_name(self):
        return self.column_name

    def get_atlan_type_name(self):
        return 'Column'

    def get_creation_payload_for_bulk_mode(self):
        return create_column_request_payload(self)

    def get_creation_payload(self):
        raise Exception("Column are creating in bulk mode only")