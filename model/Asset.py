import logging
import os
import json

from atlanapi.requests import create_entity_lineage_request_payload, create_column_lineage_request_payload, \
    create_schema_request_payload, create_column_request_payload, create_entity_request_payload
from constants import INTEGRATION_TYPE_DYNAMO_DB, INTEGRATION_TYPE_GLUE, INTEGRATION_TYPE_REDSHIFT, \
    GLUE_DATABASE_NAME, DYNAMO_DB_DATABASE_NAME

logger = logging.getLogger('main_logger')

ATLAN_PROD_AWS_ACCOUNT_ID = os.environ.get('ATLAN_PROD_AWS_ACCOUNT_ID')
ATLAN_REDSHIFT_SERVER_URL = os.environ.get('ATLAN_REDSHIFT_SERVER_URL')


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
            qualified_name = "dynamodb/dynamodb.atlan.com/{}/{}/{}/{}"
        elif self.integration_type == INTEGRATION_TYPE_GLUE:
            qualified_name = "{}/" + ATLAN_PROD_AWS_ACCOUNT_ID + "/{}/default/{}/{}"
        elif self.integration_type == INTEGRATION_TYPE_REDSHIFT:
            qualified_name = "redshift/" + ATLAN_REDSHIFT_SERVER_URL + "/{}/{}/{}/{}"
        else:
            raise Exception("Qualified name not supported yet for integration type {}"
                            .format(self.integration_type))
        return qualified_name.format(self.database_name, self.schema_name, self.entity_name, self.column_name).lower()

    def get_asset_name(self):
        return self.column_name

    def get_atlan_type_name(self):
        return 'AtlanColumn'

    def get_creation_payload_for_bulk_mode(self):
        return create_column_request_payload(self)

    def get_creation_payload(self):
        raise Exception("Column are creating in bulk mode only")


class Entity:
    def __init__(self, entity_name, database_name, schema_name, description=None, readme=None, term=None, glossary=None,
                 integration_type=INTEGRATION_TYPE_DYNAMO_DB):
        self.database_name = database_name
        self.entity_name = entity_name
        self.schema_name = schema_name
        self.readme = readme
        self.term = term
        self.glossary = glossary
        self.integration_type = integration_type.lower()
        self.description = description
        self.full_qualified_name = self.get_qualified_name() if integration_type == INTEGRATION_TYPE_DYNAMO_DB else None
        self.creation_payload = create_entity_request_payload(self)

    def get_qualified_name(self):
        if self.integration_type == INTEGRATION_TYPE_DYNAMO_DB:
            qualified_name = "dynamodb/dynamodb.atlan.com/{}/{}/{}"
        elif self.integration_type == INTEGRATION_TYPE_GLUE:
            qualified_name = "{}/" + ATLAN_PROD_AWS_ACCOUNT_ID + "/{}/default/{}"
        elif self.integration_type == INTEGRATION_TYPE_REDSHIFT:
            qualified_name = "redshift/" + ATLAN_REDSHIFT_SERVER_URL + "/{}/{}/{}"
        else:
            raise Exception("Qualified name not supported yet for integration type {}".format(self.integration_type))
        return qualified_name.format(self.database_name, self.schema_name, self.entity_name).lower()

    def get_asset_name(self):
        return self.entity_name

    def get_atlan_type_name(self):
        return 'AtlanTable'

    def get_creation_payload(self):
        table_info = {"entities": [
            self.creation_payload
        ]}
        return json.dumps(table_info)

    def get_creation_payload_for_bulk_mode(self):
        return self.creation_payload

    def get_lineage_payload(self):
        raise Exception("Not implemented !")


class Schema:
    def __init__(self, database_name, schema_name, description=None, readme=None, term=None, glossary=None,
                 integration_type=INTEGRATION_TYPE_DYNAMO_DB):
        self.database_name = database_name
        self.schema_name = schema_name
        self.description = description
        self.readme = readme
        self.term = term
        self.glossary = glossary
        self.integration_type = integration_type.lower()
        self.full_qualified_name = self.get_qualified_name() if self.integration_type == INTEGRATION_TYPE_DYNAMO_DB else None
        self.creation_payload = create_schema_request_payload(self)

    def get_qualified_name(self):
        if self.integration_type == INTEGRATION_TYPE_DYNAMO_DB:
            qualified_name = "dynamodb/dynamodb.atlan.com/{}/{}"
        elif self.integration_type == INTEGRATION_TYPE_GLUE:
            qualified_name = "{}/" + ATLAN_PROD_AWS_ACCOUNT_ID + "/{}"
        else:
            qualified_name = "redshift/" + ATLAN_REDSHIFT_SERVER_URL + "/{}/{}"
        return qualified_name.format(self.database_name, self.schema_name).lower()

    def get_asset_name(self):
        return self.schema_name

    def get_atlan_type_name(self):
        return 'AtlanSchema'

    def get_creation_payload(self):
        table_info = {"entities": [
            self.creation_payload
        ]}
        return json.dumps(table_info)

    def get_creation_payload_for_bulk_mode(self):
        return self.creation_payload

    def get_lineage_payload(self):
        raise Exception("Not implemented !")


class ColumnLineage:
    def __init__(self, column, lineage_type=None, lineage_integration_type=None,
                 lineage_schema_name=None, lineage_entity_name=None, lineage_column_name=None):
        self.column = column
        self.lineage_type = lineage_type
        self.lineage_integration_type = lineage_integration_type.lower()
        if self.lineage_integration_type == INTEGRATION_TYPE_REDSHIFT:
            # Because lineage_schema_name is a concatenation of the database and schema name. Ex: dwhstats/dwh_stats
            self.lineage_database_name = lineage_schema_name.split('/')[0]
            self.lineage_schema_name = lineage_schema_name.split('/')[1]
        elif self.lineage_integration_type == INTEGRATION_TYPE_GLUE:
            self.lineage_database_name = GLUE_DATABASE_NAME
            self.lineage_schema_name = lineage_schema_name
        elif self.lineage_integration_type == INTEGRATION_TYPE_DYNAMO_DB:
            self.lineage_database_name = DYNAMO_DB_DATABASE_NAME
            self.lineage_schema_name = lineage_schema_name
        self.lineage_entity_name = lineage_entity_name
        self.lineage_column_name = lineage_column_name
        self.lineage_full_qualified_name = None

    def get_qualified_name(self):
        if self.lineage_integration_type == INTEGRATION_TYPE_DYNAMO_DB:
            qualified_name = "dynamodb/dynamodb.atlan.com/{}/{}/{}/{}"
        elif self.lineage_integration_type == INTEGRATION_TYPE_GLUE:
            qualified_name = "{}/" + ATLAN_PROD_AWS_ACCOUNT_ID + "/{}/default/{}/{}"
        elif self.lineage_integration_type == INTEGRATION_TYPE_REDSHIFT:
            qualified_name = "redshift/" + ATLAN_REDSHIFT_SERVER_URL + "/{}/{}/{}/{}"
        else:
            raise Exception("Qualified name not supported yet for integration type {}"
                            .format(self.lineage_integration_type))
        return qualified_name.format(self.lineage_database_name,
                                     self.lineage_schema_name,
                                     self.lineage_entity_name,
                                     self.lineage_column_name).lower()

    def get_asset_name(self):
        return self.lineage_column_name

    def get_atlan_type_name(self):
        return 'AtlanColumn'

    def get_creation_payload_for_bulk_mode(self):
        return create_column_lineage_request_payload(self)

    def get_creation_payload(self):
        raise Exception("Column are creating in bulk mode only")


class EntityLineage:
    def __init__(self,
                 entity,
                 lineage_type=None,
                 lineage_integration_type=None,
                 lineage_database_name=None,
                 lineage_schema_name=None,
                 lineage_entity_name=None,
                 lineage_full_qualified_name=None):
        self.entity = entity
        self.lineage_type = lineage_type
        self.lineage_integration_type = lineage_integration_type.lower()
        self.lineage_database_name = lineage_database_name
        self.lineage_schema_name = lineage_schema_name
        self.lineage_entity_name = lineage_entity_name
        self.lineage_full_qualified_name = lineage_full_qualified_name

    def get_qualified_name(self):
        if self.lineage_integration_type == INTEGRATION_TYPE_DYNAMO_DB:
            qualified_name = "dynamodb/dynamodb.atlan.com/{}/{}/{}"
        elif self.lineage_integration_type == INTEGRATION_TYPE_GLUE:
            qualified_name = "{}/" + ATLAN_PROD_AWS_ACCOUNT_ID + "/{}/default/{}"
        elif self.lineage_integration_type == INTEGRATION_TYPE_REDSHIFT:
            qualified_name = "redshift/" + ATLAN_REDSHIFT_SERVER_URL + "/{}/{}/{}"
        else:
            raise Exception("Qualified name not supported yet for integration type {}"
                            .format(self.lineage_integration_type))
        return qualified_name.format(self.lineage_database_name, self.lineage_schema_name, self.lineage_entity_name)\
            .lower()

    def get_asset_name(self):
        raise Exception("Not implemented !")

    def get_atlan_type_name(self):
        return 'AtlanColumn'

    def get_creation_payload_for_bulk_mode(self):
        return create_entity_lineage_request_payload(self)

    def get_creation_payload(self):
        raise Exception("Column are creating in bulk mode only")
