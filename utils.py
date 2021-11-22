import logging
import sys
import os

BASE_PATH_ATLAN_DOCS = "/github/workspace/docs/data-catalogue-index"
MANIFEST_FILE_NAME = "atlan-table-definitions.csv"


def setup_logger(logger_name, level=logging.INFO):
    logger = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(asctime)s : %(message)s')
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.handlers.pop()
    logger.setLevel(level)
    logger.addHandler(stream_handler)


def get_table_name(path):
    head, tail = os.path.split(path)
    return tail.split(".")[0]


def get_path(table_name):
    return os.path.join(BASE_PATH_ATLAN_DOCS, "{}.csv".format(table_name))


def get_manifest_path():
    return os.path.join(BASE_PATH_ATLAN_DOCS, MANIFEST_FILE_NAME)
