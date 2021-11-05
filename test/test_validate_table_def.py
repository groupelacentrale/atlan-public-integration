import pandas as pd
from pandas.errors import ParserError
from io import StringIO
from unittest.mock import patch, mock_open
import pytest
import nose.tools
import unittest
from unittest.mock import patch
import json
from atlanapi.atlanutils import AtlanConfig, AtlanSourceFile, SourceFileValidator
from atlanapi.createschema import AtlanSchema, AtlanSchemaSerializer

HEADERS = {'Headers':
            ['Table/Entity',
             'Column/Attribute',
             'Summary (Description)']
           }

DATA_VALID_DF = pd.DataFrame.from_dict(
            {'Table/Entity': ['entity1', 'entity1', 'entity2'],
             'Column/Attribute': ['col1', 'col2', 'col3'],
             'Summary (Description)': ['desc', 'desc2', 'desc3']
            })

DATA_HEADERS_MISSING_OR_INVALID_NAMES_DF = pd.DataFrame.from_dict(
            {'Table/Entity ': ['entity1', 'entity1', 'entity2'],
             'Column / Attribute': ['col1', 'col2', 'col3'],
             'Summary (Description)': ['desc', 'desc2', 'desc3']
            })

IN_MEM_CSV_VALID = StringIO(
   "Table/Entity,Column/Attribute,Summary (Description)\n"
   "entity1,col1,desc\n"
   "entity1,col2,desc2\n"
   "entity2,col3,desc3\n"
)

IN_MEM_CSV_INVALID = StringIO(
    "Table/Entity,Column/Attribute,Summary (Description)\n"
    "entity1,col1\n"
    "entity1,col2,desc2,extra_col\n"
    "entity2,col3,desc3\n"
    "entity2,col4-withmissingcolumn,"
)

MOCK_CSV_INVALID = 'Table/Entity,Column/Attribute,Summary (Description)\n ' \
                   'entity1,col1-missing\n ' \
                   'entity1,col2,desc2,extra_col\n ' \
                   'entity2,col3,desc3\n ' \
                   'entity2,col4,'

MOCK_CSV_VALID = 'Table/Entity,Column/Attribute,Summary (Description)\n ' \
                 'entity1,col1,\n ' \
                 'entity1,col2,desc2\n ' \
                 'entity2,col3,desc3\n ' \
                 'entity2,col4,'

def test_load_csv():
    source_data = AtlanSourceFile(IN_MEM_CSV_VALID)
    source_data.load_csv()
    assert source_data.assets_def.equals(DATA_VALID_DF)


def test_load_csv_too_many_columns():
    source_data = AtlanSourceFile(IN_MEM_CSV_INVALID)
    try:
        source_data.load_csv()
    except ParserError:
        # The exception was raised as expected
        pass
    else:
        # If we get here, then the ValueError was not raised
        # raise an exception so that the test fails
        raise AssertionError("ParserError was not raised")

@patch("builtins.open", new_callable=mock_open, read_data=MOCK_CSV_INVALID)
def test_load_csv_too_few_columns(mock_file):
    source_data = AtlanSourceFile(mock_file)
    try:
        source_data.validate_csv_column_length(HEADERS['Headers'])
    except ParserError:
        # The exception was raised as expected
        pass
    else:
        # If we get here, then the ValueError was not raised
        # raise an exception so that the test fails
        raise AssertionError("ParserError was not raised")

@patch("builtins.open", new_callable=mock_open, read_data=MOCK_CSV_VALID)
def test_load_csv_correct_number_columns(mock_file):
    source_data = AtlanSourceFile(mock_file)
    assert source_data.validate_csv_column_length(HEADERS['Headers']) is None


def test_validate_table_headers_when_not_equal():
    validation_file = SourceFileValidator(DATA_HEADERS_MISSING_OR_INVALID_NAMES_DF)
    try:
        validation_file.validate_headers(HEADERS['Headers'])
    except ValueError:
        # The exception was raised as expected
        pass
    else:
        # If we get here, then the ValueError was not raised
        # raise an exception so that the test fails
        raise AssertionError("ValueError was not raised")

def test_validate_table_headers_when_equal():
    validation_file = SourceFileValidator(DATA_VALID_DF)
    assert validation_file.validate_headers(HEADERS['Headers']) is None