import logging
import sys
import os

BASE_PATH_ATLAN_DOCS = "/github/workspace/docs/datacatalog"
MANIFEST_FILE_NAME = "manifest.csv"


def setup_logger(logger_name, level=logging.INFO):
    logger = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(asctime)s : %(message)s')
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.setLevel(level)
    logger.addHandler(stream_handler)


def get_table_name(path):
    head, tail = os.path.split(path)
    return tail.split(".")[0]


def get_path(integration_type, table_name):
    return os.path.join(BASE_PATH_ATLAN_DOCS, integration_type, "{}.csv".format(table_name))


def get_manifest_path():
    return os.path.join(BASE_PATH_ATLAN_DOCS, MANIFEST_FILE_NAME)


def get_column_qualified_name(table_name, entity_name, column_name, integration_type="DynamoDb"):
    if integration_type == "DynamoDb":
        qualified_name = "dynamodb/dynamodb.atlan.com/dynamo_db/{}/{}/{}"
    elif integration_type == "glue":
        qualified_name = "{}/default/{}/{}"
    elif integration_type =="redshift":
        qualified_name = "{}/{}/{}"
    else:
        raise Exception("Qualified name not supported yet for integration type {}".format(integration_type))
    return qualified_name.format(table_name, entity_name, column_name).lower()


def get_entity_qualified_name(table_name, entity_name, prefix="dynamodb/dynamodb.atlan.com/dynamo_db/"):
    return prefix + "{}/{}".format(table_name, entity_name).lower()


def get_schema_qualified_name(table_name, prefix="dynamodb/dynamodb.atlan.com/dynamo_db/"):
    return prefix + "{}".format(table_name).lower()


def construct_qualified_name_prefix(integration_type):
    if integration_type == "DynamoDb":
        prefix = "dynamodb/dynamodb.atlan.com/dynamo_db/"
    elif integration_type == "glue":
        prefix = "{}/default/"
    elif integration_type == "redshift":
        prefix = ""
    return prefix


def check_key(dict, key):
    if key in dict.keys():
        key_present = 'true'
    else:
        key_present = 'false'
    return key_present


def get_qualified_name(dict, key_present):
    if key_present == 'true':
        return dict['entities'][0]['attributes']['qualifiedName']
    else:
        return None
