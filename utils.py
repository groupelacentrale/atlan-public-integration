import logging
import sys
import os

from constants import BASE_PATH_ATLAN_DOCS, MANIFEST_FILE_NAME, INTEGRATION_TYPE_ATHENA, INTEGRATION_TYPE_DYNAMO_DB


def setup_logger(logger_name, level=logging.INFO):
    logger = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.setLevel(level)
    logger.addHandler(stream_handler)


def get_table_name(path):
    head, tail = os.path.split(path)
    return tail.split(".")[0]


def get_csv_file_name(schema_name, entity_name, integration_type):
    if integration_type.lower() == INTEGRATION_TYPE_DYNAMO_DB:
        return schema_name
    return entity_name


def get_path(integration_type, schema_name, table_or_entity_name):
    csv_file_name = get_csv_file_name(schema_name, table_or_entity_name, integration_type)

    return os.path.join(BASE_PATH_ATLAN_DOCS, integration_type.lower(), "{}.csv".format(csv_file_name))


def get_manifest_path():
    return os.path.join(BASE_PATH_ATLAN_DOCS, MANIFEST_FILE_NAME)


def get_template_source_file():
    script_dir = os.path.dirname(__file__)
    return os.path.join(script_dir, "config/template_source_file.yaml")
