import sys
import os

import utils
from validate_atlan_source_file import validate_atlan_source_file as validate
from create_atlan_dynamodb_entity import create_atlan_dynamodb_entity as create_table
from create_atlan_columns import create_atlan_columns
from create_atlan_column_lineage import create_atlan_column_lineage
import logging

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout,
                        level=logging.INFO,
                        format='%(asctime)s %(message)s')

#    path="/github/workspace/docs/data-catalogue-index/test-cible-table.csv"

    path="./source_files/test-cible-table.csv"
    table_name = utils.get_table_name(path)
    entity= "test"
    description = "created by github action"
    validate(path)
    create_table(path, entity, description)
    create_atlan_columns(path)
    #create_atlan_column_lineage(path, "DynamoDb")


  #/github/workspace/docs/
