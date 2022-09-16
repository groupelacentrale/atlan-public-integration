import logging
import os
import json

from atlanapi.requests import create_entity_lineage_request_payload, create_column_lineage_request_payload, \
    create_schema_request_payload, create_column_request_payload, create_entity_request_payload
from constants import INTEGRATION_TYPE_DYNAMO_DB, INTEGRATION_TYPE_GLUE, INTEGRATION_TYPE_REDSHIFT, \
    GLUE_DATABASE_NAME, DYNAMO_DB_DATABASE_NAME
from exception.EnvVariableNotFound import EnvVariableNotFound

logger = logging.getLogger('main_logger')

ATLAN_PROD_AWS_ACCOUNT_ID = os.environ.get('ATLAN_PROD_AWS_ACCOUNT_ID')
ATLAN_REDSHIFT_SERVER_URL = os.environ.get('ATLAN_REDSHIFT_SERVER_URL')


def get_atlan_prod_aws_account_id(asset):
    if not ATLAN_PROD_AWS_ACCOUNT_ID:
        raise EnvVariableNotFound(asset, 'ATLAN_PROD_AWS_ACCOUNT_ID')
    return ATLAN_PROD_AWS_ACCOUNT_ID


def get_atlan_redshift_server_url(asset):
    if not ATLAN_REDSHIFT_SERVER_URL:
        raise EnvVariableNotFound(asset, 'ATLAN_REDSHIFT_SERVER_URL')
    return ATLAN_REDSHIFT_SERVER_URL


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
            qualified_name = "{}/" + get_atlan_prod_aws_account_id(self) + "/{}/default/{}/{}"
        elif self.integration_type == INTEGRATION_TYPE_REDSHIFT:
            qualified_name = "redshift/" + get_atlan_redshift_server_url(self) + "/{}/{}/{}/{}"
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

    def get_qualified_name(self):
        if self.integration_type == INTEGRATION_TYPE_DYNAMO_DB:
            qualified_name = "dynamodb/dynamodb.atlan.com/{}/{}/{}"
        elif self.integration_type == INTEGRATION_TYPE_GLUE:
            qualified_name = "{}/" + get_atlan_prod_aws_account_id(self) + "/{}/default/{}"
        elif self.integration_type == INTEGRATION_TYPE_REDSHIFT:
            qualified_name = "redshift/" + get_atlan_redshift_server_url(self) + "/{}/{}/{}"
        else:
            raise Exception("Qualified name not supported yet for integration type {}".format(self.integration_type))
        return qualified_name.format(self.database_name, self.schema_name, self.entity_name).lower()

    def get_asset_name(self):
        return self.entity_name

    def get_atlan_type_name(self):
        return 'AtlanTable'

    def get_creation_payload(self):
        table_info = {"entities": [
            create_entity_request_payload(self)
        ]}
        return json.dumps(table_info)

    def get_creation_payload_for_bulk_mode(self):
        return create_entity_request_payload(self)

    def get_lineage_payload(self):
        raise Exception("Not implemented !")

    def __repr__(self):
        return """(integration_type = {}
            database_name = {}
            schema_name = {}
            entity_name = {})""".format(
            self.integration_type,
            self.database_name,
            self.schema_name,
            self.entity_name,
        )

    def __hash__(self):
        return hash((self.database_name, self.entity_name, self.schema_name, self.integration_type))

    def __eq__(self, other):
        if not isinstance(other, type(self)): return NotImplemented
        return self.database_name == other.database_name \
               and self.entity_name == other.entity_name \
               and self.schema_name == other.schema_name \
               and self.integration_type == other.integration_type


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

    def get_qualified_name(self):
        if self.integration_type == INTEGRATION_TYPE_DYNAMO_DB:
            qualified_name = "dynamodb/dynamodb.atlan.com/{}/{}"
        elif self.integration_type == INTEGRATION_TYPE_GLUE:
            qualified_name = "{}/" + get_atlan_prod_aws_account_id(self) + "/{}"
        elif self.integration_type == INTEGRATION_TYPE_REDSHIFT:
            qualified_name = "redshift/" + get_atlan_redshift_server_url(self) + "/{}/{}"
        else:
            raise Exception("Qualified name not supported yet for integration type {}"
                            .format(self.integration_type))
        return qualified_name.format(self.database_name, self.schema_name).lower()

    def get_asset_name(self):
        return self.schema_name

    def get_atlan_type_name(self):
        return 'AtlanSchema'

    def get_creation_payload(self):
        table_info = {"entities": [
            create_schema_request_payload(self)
        ]}
        return json.dumps(table_info)

    def get_creation_payload_for_bulk_mode(self):
        return create_schema_request_payload(self)

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
            qualified_name = "{}/" + get_atlan_prod_aws_account_id(self) + "/{}/default/{}/{}"
        elif self.lineage_integration_type == INTEGRATION_TYPE_REDSHIFT:
            qualified_name = "redshift/" + get_atlan_redshift_server_url(self) + "/{}/{}/{}/{}"
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
            qualified_name = "{}/" + get_atlan_prod_aws_account_id(self) + "/{}/default/{}"
        elif self.lineage_integration_type == INTEGRATION_TYPE_REDSHIFT:
            qualified_name = "redshift/" + get_atlan_redshift_server_url(self) + "/{}/{}/{}"
        else:
            raise Exception("Qualified name not supported yet for integration type {}"
                            .format(self.lineage_integration_type))
        return qualified_name.format(self.lineage_database_name, self.lineage_schema_name, self.lineage_entity_name) \
            .lower()

    def get_asset_name(self):
        raise Exception("Not implemented !")

    def get_atlan_type_name(self):
        return 'AtlanTable'

    def get_creation_payload_for_bulk_mode(self):
        return create_entity_lineage_request_payload(self)

    def get_creation_payload(self):
        raise Exception("Column are creating in bulk mode only")

    def __repr__(self):
        if self.lineage_type.lower() == "source":
            return """(integration_type={},
                         database_name={},
                         schema_name={},
                         entity_name={}) 
                        ---> 
                         {}""".format(
                self.lineage_integration_type,
                self.lineage_database_name,
                self.lineage_schema_name,
                self.lineage_entity_name,
                self.entity
            )
        else:
            return """{}
                        --->
                    (integration_type={},
                      database_name={},
                      schema_name={},
                      entity_name={})""".format(
                self.entity,
                self.lineage_integration_type,
                self.lineage_database_name,
                self.lineage_schema_name,
                self.lineage_entity_name,
            )

    def __hash__(self):
        return hash((self.entity.__hash__(),
                     self.lineage_type,
                     self.lineage_integration_type,
                     self.lineage_database_name,
                     self.lineage_schema_name,
                     self.lineage_entity_name))

    def __eq__(self, other):
        if not isinstance(other, type(self)): return NotImplemented
        return self.entity == other.entity \
               and self.lineage_type == other.lineage_type \
               and self.lineage_integration_type == other.lineage_integration_type \
               and self.lineage_database_name == other.lineage_database_name \
               and self.lineage_schema_name == other.lineage_schema_name \
               and self.lineage_entity_name == other.lineage_entity_name
