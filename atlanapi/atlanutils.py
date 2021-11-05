import pandas as pd
from pandas.errors import ParserError
import requests
import sys
import yaml

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
            api_response.raise_for_status()
            print("{} successful: {}\n{}".format(self.request_type, api_response.status_code, api_response.text))
        except requests.exceptions.HTTPError as errh:
            print(errh)
        except requests.exceptions.ConnectionError as errc:
            print(errc)
        except requests.exceptions.Timeout as errt:
            print(errt)
        except requests.exceptions.RequestException as err:
            print(err)
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
                print(exc)
        return self.params


class AtlanSourceFile:
    """
    Creates a dataframe object
    Arguments:
        csv_filepath: path to source documentation for Atlan asset(s)
        sep: character separator for the csv file (default=;)
        keep_default_na: set to false to avoid NAN errors
    """
    def __init__(self, csv_filepath, sep=",", keep_default_na=False, escapechar='\\', encoding='utf-8', warn_bad_lines=True, error_bad_lines=True):
        self.csv_filepath = csv_filepath
        self.sep = sep
        self.keep_default_na = keep_default_na
        self.escapechar=escapechar
        self.encoding=encoding
        self.warn_bad_lines=warn_bad_lines
        self.error_bad_lines=error_bad_lines

    def load_csv(self):
        """
        Load_csv file and return a dataframe object.
        """
        try:
            self.assets_def = pd.read_csv(self.csv_filepath, sep=self.sep, keep_default_na=self.keep_default_na,
                                          escapechar=self.escapechar, encoding=self.encoding,
                                          warn_bad_lines=self.warn_bad_lines, error_bad_lines=self.error_bad_lines)
        except ParserError as p_err:
            print("{}: Problem parsing fields in source file {}. Verify the number of columns are consistent".format(p_err, self.csv_filepath))
            raise
        except Exception as e:
            print(sys.stderr, "Exception: {}\n".format(e))
            print("Problem loading source file {}. Verify that source file is present"
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
            print("{}: Problem loading csv file {}. Verify the field exists".format(i_err, self.csv_filepath))
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


    def validate_data_type_values(self, atlansourcefile):
        """

        """
        pass

    def validate_data_integration_values(self, atlansourcefile):
        """

        """
        pass

        





