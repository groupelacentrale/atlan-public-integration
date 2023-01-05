import logging
import pandas as pd
from pandas.errors import ParserError
import requests
import sys
import yaml

logger = logging.getLogger('main_logger')

SUPPORTED_REQUEST_TYPES = ["POST", "GET", "DELETE"]

class AtlanApiRequest:
    def __init__(self, request_type, url, headers, data_payload):
        self.request_type = request_type
        self.url = url
        self.headers = headers
        self.data = data_payload
        if self.request_type not in SUPPORTED_REQUEST_TYPES:
            raise ValueError('Request must be of type {}'.format(SUPPORTED_REQUEST_TYPES))
        else:
            pass

    def send_atlan_request(self):
        try:
            api_response = requests.request(self.request_type,
                                               self.url, headers=self.headers, data=self.data)
            # this line will raise an error in the logs, if wanted:
            # api_response.raise_for_status()
            logger.debug("{} successful: {}\nAPI Response Text: {}".format(self.request_type, api_response.status_code, api_response.text))
        except requests.exceptions.HTTPError as errh:
            logger.error(errh)
            raise errh
        except requests.exceptions.ConnectionError as errc:
            logger.error(errc)
            raise errc
        except requests.exceptions.Timeout as errt:
            logger.error(errt)
            raise errt
        except requests.exceptions.RequestException as err:
            logger.error(err)
            raise err
        return api_response


class AtlanConfig:
    def __init__(self, config_path):
        self.config_path = config_path

    def load_yaml_configs(self):
        c = self.config_path
        with open(c, 'r') as template:
            try:
                self.params = yaml.safe_load(template)
            except yaml.YAMLERROR as exc:
                logger.error(exc)
        return self.params


class AtlanSourceFile:
    """
    Creates a dataframe object
    Arguments:
        csv_filepath: path to source documentation for Atlan asset(s)
        sep: character separator for the csv file (default=;)
        keep_default_na: set to false to avoid NAN errors
    """

    def __init__(self, csv_filepath, sep=",", keep_default_na=False, escapechar='\\', encoding='utf-8',
                 on_bad_lines='error'):
        self.csv_filepath = csv_filepath
        self.sep = sep
        self.keep_default_na = keep_default_na
        self.escapechar = escapechar
        self.encoding = encoding
        self.on_bad_lines = on_bad_lines

    def load_csv(self):
        """
        Load_csv file and return a dataframe object.
        """
        try:
            self.assets_def = pd.read_csv(self.csv_filepath, sep=self.sep, keep_default_na=self.keep_default_na,
                                          escapechar=self.escapechar, encoding=self.encoding,
                                          on_bad_lines=self.on_bad_lines)
        except ParserError as p_err:
            logger.error("{}: Problem parsing fields in source file {}. Verify the number of columns are consistent".format(p_err, self.csv_filepath))
            raise
        except Exception as e:
            logger.critical(sys.stderr, "Exception: {}\n".format(e))
            logger.critical("Problem loading source file {}. Verify that source file is present"
                .format(self.csv_filepath))
            sys.exit(1)
        else:
            return self.assets_def

    def validate_csv_column_length(self, header_template):
        expected_column_length = len(header_template)
        try:
            with open(self.csv_filepath, 'r') as csv_f:
                self.lines = csv_f.readlines()
        except IOError as i_err:
            logger.error("{}: Problem loading csv file {}. Verify the field exists".format(i_err, self.csv_filepath))
            raise

        for i, e in enumerate(self.lines):
            if (len(e.split(self.sep))) < expected_column_length:
                raise ParserError("The number of columns is different than expected in line {}: expected {} lines but instead found {}"
                                  .format(i + 1, expected_column_length, len(e.split(self.sep))))
            else:
                continue


class SourceFileValidator:
    """
    Performs multiple tests on the csv source file to make sure it matches the expected
    format for importing into Atlan.
    Arguments:
        atlansourcefile: the pandas dataframe initiated by the AtlanSourceFile class
    """
    def __init__(self, source_dataframe):
        self.atlansourcefile = source_dataframe


    def validate_headers(self, header_template):
        """
        Ensures that source file headers match the expected headers for loading the data into Atlan.
        Arguments:
            header_template: the list of expected headers to import into Atlan.
        Returns: None if ok, raises ValueError if the columns are not the same.
        """
        self.expected_headers = set(header_template)
        self.source_headers = set(self.atlansourcefile.columns)
        if self.expected_headers != self.source_headers:
            raise ValueError('Column headers are not compatible:\n'
                             'Missing expected headers: {}\n'
                             'Unexpected headers: {}'.format(self.expected_headers - self.source_headers,
                                                             self.source_headers - self.expected_headers))
        else:
            return None


    def validate_data_type_values(self, list_allowed_datatypes):
        """
        Ensures that the source file contains valid data types based on the integration type (e.g., dynamodb, glue)
        """
        self.list_allowed_datatypes = list_allowed_datatypes
        self.source_data_types = self.atlansourcefile["Type"].str.lower()

        for i in range(len(self.list_allowed_datatypes)):
            self.list_allowed_datatypes[i] = self.list_allowed_datatypes[i].lower()

        for i, e in enumerate(self.source_data_types):
            if len(self.source_data_types[i]) == 0:
                continue
            elif self.source_data_types[i] not in (self.list_allowed_datatypes):
                raise ValueError(
                    "The data type, '{}', in line {} is not a supported data type value. Supported data types are {}"
                    .format(e, i + 1, self.list_allowed_datatypes))
            else:
                continue
