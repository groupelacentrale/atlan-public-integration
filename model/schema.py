import json
from annotation import auto_str
from model import get_atlan_athena_connection_id, get_atlan_redshift_connection_id, get_database

from atlanapi.requests import create_schema_request_payload, classification_request_payload, \
    detach_classification_request_payload, link_term_request_payload, unlink_term_request_payload
from constants import INTEGRATION_TYPE_DYNAMO_DB, INTEGRATION_TYPE_ATHENA, \
    INTEGRATION_TYPE_REDSHIFT, REDSHIFT_CONN_QN, DYNAMODB_CONN_QN, ATHENA_CONN_QN


@auto_str
class Schema:
    def __init__(self, database_name, schema_name, description=None, readme=None, term=None, glossary=None,
                 classification=None,
                 integration_type=INTEGRATION_TYPE_DYNAMO_DB):
        self.database_name = database_name
        self.schema_name = schema_name
        self.description = description
        self.readme = readme
        self.term = term
        self.glossary = glossary
        self.classification = classification
        self.integration_type = integration_type.lower()

    def get_qualified_name(self):
        if self.integration_type == INTEGRATION_TYPE_DYNAMO_DB:
            qualified_name = DYNAMODB_CONN_QN + "/" + \
                             get_database(self.integration_type) + "/{}"
        elif self.integration_type == INTEGRATION_TYPE_ATHENA:
            qualified_name = ATHENA_CONN_QN + "/" + get_atlan_athena_connection_id(self) + "/" + \
                             get_database(self.integration_type) + "/{}"
        elif self.integration_type == INTEGRATION_TYPE_REDSHIFT:
            qualified_name = REDSHIFT_CONN_QN + "/" + get_atlan_redshift_connection_id(self) + "/" + \
                             get_database(self.integration_type) + "/{}"
        else:
            raise Exception("Qualified name not supported yet for integration type {}"
                            .format(self.integration_type))
        return qualified_name.format(self.schema_name.lower())

    def get_asset_name(self):
        return self.schema_name

    def get_atlan_type_name(self):
        return 'Schema'

    def get_creation_payload(self):
        table_info = {"entities": [
            create_schema_request_payload(self)
        ]}
        return json.dumps(table_info)

    def get_creation_payload_for_bulk_mode(self):
        return create_schema_request_payload(self)

    def get_lineage_payload(self):
        raise Exception("Not implemented !")

    def get_classification_payload_for_bulk_mode(self):
        return classification_request_payload(self)

    def get_detach_classification_payload_for_bulk_mode(self):
        return detach_classification_request_payload(self)

    def get_link_term_payload_for_bulk_mode(self):
        return link_term_request_payload(self)

    def get_unlink_term_payload_for_bulk_mode(self):
        return unlink_term_request_payload(self)
