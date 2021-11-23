#!/usr/bin/env python3

"""
A script to read table metadata from a user-specified AWS Catalog database and table, enrich it with additional
metadata, and write to an output .csv file that can be imported into Atlan, the internal Data Governance tool.

Usage Options:
-t --table : name of the DynamoDB table --> Atlan Schema
-e --entity : name of the DynamoDB entity -> Atlan Table
-d --description : description of the entity
"""

import logging
from atlanapi.createtable import AtlanTable, AtlanTableSerializer
from atlanapi.createschema import AtlanSchema, AtlanSchemaSerializer
from atlanapi.atlanutils import AtlanApiRequest
from optparse import OptionParser
from ApiConfig import create_api_config

logger = logging.getLogger('main_logger')


def create_atlan_dynamodb_entity(table, entity, description):
    api_conf = create_api_config()

    headers = {
        'Content-Type': 'application/json',
        'APIKEY': api_conf.api_key
    }

    logger.info("Generating API request to create schema so it is searchable: {}".format(table))
    schema = AtlanSchema(integration_type="DynamoDb",
                         name=table,
                         qualified_name="dynamodb/dynamodb.atlan.com/dynamo_db/{}".format(table))
    s_payload = AtlanSchemaSerializer()
    schema_payload = s_payload.serialize(schema)

    logger.info("Posting API request to create schema")
    schema_post_url = 'https://{}/api/metadata/atlas/tenants/default/entity/bulk'.format(api_conf.instance)
    atlan_api_schema_request_object = AtlanApiRequest("POST", schema_post_url, headers, schema_payload)
    atlan_api_schema_request_object.send_atlan_request()

    logger.info("Generating API request to create schema.table: {}.{}".format(table, entity))
    entity = AtlanTable(integration_type="DynamoDb",
                        name=entity,
                        qualified_name="dynamodb/dynamodb.atlan.com/dynamo_db/{}/{}".format(table, entity),
                        description=description)
    e_payload = AtlanTableSerializer()
    entity_payload = e_payload.serialize(entity)

    logger.info("Posting API request to create entity")
    entity_post_url = 'https://{}/api/metadata/atlas/tenants/default/entity/bulk'.format(api_conf.instance)
    atlan_api_entity_request_object = AtlanApiRequest("POST", entity_post_url, headers, entity_payload)
    atlan_api_entity_request_object.send_atlan_request()


if __name__ == '__main__':
    # TODO : Add option for integration type to generalize the script
    # NOTE: -t --table should be equal to the title of the source file csv (cf le Modèle Physique DynamoDb NOE)
    parser = OptionParser(usage='usage: %prog [options] arguments')
    parser.add_option("-t", "--table", help="Name of the DynamoDB table -> Atlan Schema")
    parser.add_option("-e", "--entity", help="Name of the DynamoDB entity -> Atlan Table")
    parser.add_option("-d", "--description", help="Description of the entity")
    (options, args) = parser.parse_args()

    create_atlan_dynamodb_entity(options.table, options.entity, options.description)
