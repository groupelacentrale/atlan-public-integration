import requests

import utils
from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest, AtlanSourceFile
from atlanapi.createAsset import create_assets
from atlanapi.delete_asset import delete_asset
from atlanapi.searchAssets import get_asset_guid_by_qualified_name
from model.Asset import *
from constants import INTEGRATION_TYPE_DYNAMO_DB
from atlanapi.createReadme import create_readme
from atlanapi.linkTerm import link_term
from exception.EnvVariableNotFound import EnvVariableNotFound


def test_integrations():
    src_data = AtlanSourceFile(".docs/sample/datacatalog/manifest.csv", ",")
    src_data.load_csv()

