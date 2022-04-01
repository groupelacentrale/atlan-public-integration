import logging
import sys
import os

BASE_PATH_ATLAN_DOCS = "/github/workspace/docs/datacatalog"
MANIFEST_FILE_NAME = "manifest.csv"

INTEGRATION_TYPE_DYNAMO_DB = 'dynamodb'
INTEGRATION_TYPE_GLUE = 'glue'
INTEGRATION_TYPE_REDSHIFT = 'redshift'

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
    return os.path.join(BASE_PATH_ATLAN_DOCS, integration_type.lower(), "{}.csv".format(table_name))


def get_manifest_path():
    return os.path.join(BASE_PATH_ATLAN_DOCS, MANIFEST_FILE_NAME)


def get_template_source_file():
    script_dir = os.path.dirname(__file__)
    return os.path.join(script_dir, "config/template_source_file.yaml")


def construct_qualified_name_prefix(integration_type):
    if integration_type == INTEGRATION_TYPE_DYNAMO_DB:
        prefix = "dynamodb/dynamodb.atlan.com/dynamo_db/"
    elif integration_type == INTEGRATION_TYPE_GLUE:
        prefix = "{}/default/"
    elif integration_type == INTEGRATION_TYPE_REDSHIFT:
        prefix = ""
    return prefix
