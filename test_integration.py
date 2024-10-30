import json
import logging
import os
import time
import requests

import utils
from atlanapi.atlanutils import AtlanApiRequest, AtlanSourceFile
from atlanapi.createAsset import create_assets
from atlanapi.searchAssets import get_asset_guid_by_qualified_name, get_asset_by_guid
from constants import INTEGRATION_TYPE_DYNAMO_DB
from atlanapi.createReadme import create_readme
from atlanapi.linkTerm import link_term
from exception.EnvVariableNotFound import EnvVariableNotFound
from model import Schema, Table, Column


def get_asset_updated_by(qualified_name, type):
    try:
        asset_guid = get_asset_guid_by_qualified_name(qualified_name, type)
        asset_infos = get_asset_by_guid(asset_guid)
        updated_by = asset_infos['entity']['updatedBy']
        return updated_by
    except KeyError as e:
        return f"Clé manquante dans le JSON: {e}"
    except Exception as e:
        return f"Erreur lors de la récupération des informations: {e}"

if __name__ == '__main__':
    #test_create_database("dynamodb", "test_db")
    # test_create_schema()
    #test_create_table()

    #test_delete_asset("e3df33ed-c5e3-4e3e-98ee-fb977981c4b6")

     #test_delete(list)

    #print(get_asset_updated_by("default/dynamodb/dynamodb-prod/dynamo_db/starwars_characters/heroes", "Table"))

    str = "service-account-apikey-e3df9f7b-42ba-4562-98ba-8bc1222221f8"

    sub = "service-account-apikey"

    if sub in str:
        print("it's in")
    else:
        print("It is not")


    #test_create_column()

    #create_connection()


    #test_integrate_sample()
    #print(test_db_exists("dynamo_db"))