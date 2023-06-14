#!/usr/bin/env python3

"""
A script to generate your api_config.yaml file to make and get requests using the AtlanApi

Usage Options:
-a --api_key : API key needed to make and get requests using the Atlan API
-i --instance_name : Server name of the Atlan instance)
"""

import yaml
import os
import sys
from optparse import OptionParser

DIR = "config"


def main(args):
    parser = OptionParser(usage='usage: %prog [options] arguments')
    parser.add_option("-a", "--api_key", help="API key needed to make and get requests using the Atlan API")
    parser.add_option("-i", "--instance_name", help="Server name of the Atlan instance")
    (options, args) = parser.parse_args()

    data = {
        "api_key": options.api_key,
        "instance": options.instance_name
    }

    if not os.path.isdir(DIR):
        os.makedirs(DIR)

    with open(os.path.join(DIR, "config/api_config.yaml"), 'w') as file:  # Use file to refer to the file object
        yaml.dump(data, file)


if __name__ == '__main__':
    main(sys.argv[1:])
