import logging
import sys
import os


def init_logging():
    logging.basicConfig(stream=sys.stdout,
                        level=logging.INFO,
                        format='%(asctime)s %(message)s')


def get_table_name(path):
    head, tail = os.path.split(path)
    return tail.split(".")[0]

