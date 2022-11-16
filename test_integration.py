import time
import requests

import utils
from atlanapi.ApiConfig import create_api_config
from atlanapi.atlanutils import AtlanApiRequest, AtlanSourceFile
from atlanapi.createAsset import create_assets
from model.Asset import *
from constants import INTEGRATION_TYPE_DYNAMO_DB
from atlanapi.createReadme import create_readme
from atlanapi.linkTerm import link_term
from exception.EnvVariableNotFound import EnvVariableNotFound

api_conf = create_api_config()
token = 'eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJFUXhTd2pMVFBJYlFUeS1tMG9GeWtLVnlMSEllUHRfSGR0Q2VwazByaWZFIn0.eyJleHAiOjIwNzU0NTk2MzYsImlhdCI6MTY2NTQ5MTYzNiwianRpIjoiMGUzZTM0ZTAtMjNmNC00NTg4LWEwMGYtYjRjYTg0Y2QwM2IzIiwiaXNzIjoiaHR0cHM6Ly9ncm91cGVsYWNlbnRyYWxlLmF0bGFuLmNvbS9hdXRoL3JlYWxtcy9kZWZhdWx0IiwiYXVkIjpbInJlYWxtLW1hbmFnZW1lbnQiLCJhY2NvdW50Il0sInN1YiI6IjAzNGIxMTFlLTI2NzMtNDJkZC04Y2RmLWE3MDU4ODg4NTU3NCIsInR5cCI6IkJlYXJlciIsImF6cCI6ImFwaWtleS1iY2JkOGFmMC02ODRiLTRhYTQtYmM3Ni03NDNjOGZkOWM5ZGMiLCJhY3IiOiIxIiwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbIiRndWVzdCIsImRlZmF1bHQtcm9sZXMtZGVmYXVsdCIsIm9mZmxpbmVfYWNjZXNzIiwiRGF0YSBFbmdpbmVlciIsInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsicmVhbG0tbWFuYWdlbWVudCI6eyJyb2xlcyI6WyJ2aWV3LXJlYWxtIiwidmlldy1pZGVudGl0eS1wcm92aWRlcnMiLCJtYW5hZ2UtaWRlbnRpdHktcHJvdmlkZXJzIiwiaW1wZXJzb25hdGlvbiIsInJlYWxtLWFkbWluIiwiY3JlYXRlLWNsaWVudCIsIm1hbmFnZS11c2VycyIsInF1ZXJ5LXJlYWxtcyIsInZpZXctYXV0aG9yaXphdGlvbiIsInF1ZXJ5LWNsaWVudHMiLCJxdWVyeS11c2VycyIsIm1hbmFnZS1ldmVudHMiLCJtYW5hZ2UtcmVhbG0iLCJ2aWV3LWV2ZW50cyIsInZpZXctdXNlcnMiLCJ2aWV3LWNsaWVudHMiLCJtYW5hZ2UtYXV0aG9yaXphdGlvbiIsIm1hbmFnZS1jbGllbnRzIiwicXVlcnktZ3JvdXBzIl19LCJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6ImVtYWlsIHByb2ZpbGUgb2ZmbGluZV9hY2Nlc3MiLCJjcmVhdGVkQXQiOiIxNjY1NDkxNjMyMDg1IiwiY2xpZW50SWQiOiJhcGlrZXktYmNiZDhhZjAtNjg0Yi00YWE0LWJjNzYtNzQzYzhmZDljOWRjIiwiY2xpZW50SG9zdCI6IjE3Mi4yOS4xMC4xNDEiLCJlbWFpbF92ZXJpZmllZCI6ZmFsc2UsImdyb3VwcyI6W10sInJlYWxtIjoiZGVmYXVsdCIsInByZWZlcnJlZF91c2VybmFtZSI6InNlcnZpY2UtYWNjb3VudC1hcGlrZXktYmNiZDhhZjAtNjg0Yi00YWE0LWJjNzYtNzQzYzhmZDljOWRjIiwidXNlcklkIjoiMDM0YjExMWUtMjY3My00MmRkLThjZGYtYTcwNTg4ODg1NTc0IiwiY2xpZW50QWRkcmVzcyI6IjE3Mi4yOS4xMC4xNDEiLCJ1c2VybmFtZSI6InNlcnZpY2UtYWNjb3VudC1hcGlrZXktYmNiZDhhZjAtNjg0Yi00YWE0LWJjNzYtNzQzYzhmZDljOWRjIn0.HTLpaIZX4ilopzgxYUTjUbRWE8ZMZ4kjdrzmoFvL14pVjIZ11IKo6vHWeHxrVWKoZItABmyoYOKAgZ9ms1H_Nc7QdEOwxRhvxMPiN0lpW1pYMUUx6w1Q0aX3347lNvqnxWUkUpeY-bmJNEk3_Q2y-LItTMlk8Fe7nwE1axzihw8xSBiO8fnxnodgABUp19X11cbePlllWEgpjuF2Z-q2v8XYIuR_UxuyzOS5E8AkyydhgAfEkeqvUttG8u4PL0FDK34XKcr5YbvH5AJgwImZnwMfdFhCdW668O-14JFfMfHkGbQYgnF-pVt6rx45B2MGlicqU5szX3Rs7VYaYI_s4A'
auth_token = 'Bearer {}'.format(token)
logger = logging.getLogger('')

headers = {
    'Authorization': auth_token,
    'Content-Type': 'application/json'
}

header_delete = {
    'Authorization': auth_token
}

TABLE = "test_table"
ENTITY = "test_entity"
DATABASE = "test_db_dynamo"
DESCRIPTION = "Example Description : Michel test atlan V2 succeeded"
CONNECTOR_NAME = "dynamodb-prod"
SCHEMA = "test_schema"
README = "test_readme"
TERM = "test_term"
GLOSSARY = "test_glossary"


def test_create_database(connector_name, db_name):
    data = {
        "entities": [
            {
                "typeName": "Database",
                "attributes": {
                    "name": db_name,
                    "connectorName": connector_name,
                    "qualifiedName": "dynamodb/dynamodb.atlan.com/{}".format(db_name),
                    "description": "Test create Databse Integration SUCCEED",
                    "connectionQualifiedName": "dynamodb/dynamodb.atlan.com"
                }
            }
        ]
    }
    url = 'https://{}/api/meta/entity/bulk#{}'.format(api_conf.instance, 'createDatabases')
    request_object = AtlanApiRequest("POST", url, headers, json.dumps(data))
    request_object.send_atlan_request()


def test_create_schema():
    data = {
        "entities": [
            {
                "typeName": "Schema",
                "attributes": {
                    "name": "test_schema",
                    "qualifiedName": "dynamodb/dynamodb.atlan.com/{}/{}".format("dynamo_db", SCHEMA),
                    "databaseName": "dynamo_db",
                    "description": "Test integration schema test_schema",
                    "databaseQualifiedName": "dynamodb/dynamodb.atlan.com/{}".format("dynamo_db"),
                    "integrationType": "dynamo_db"
                }
            }
        ]
    }
    url = 'https://{}/api/meta/entity/bulk#{}'.format(api_conf.new_instance, 'createSchemas')
    request_object = AtlanApiRequest("POST", url, headers, json.dumps(data))
    response = request_object.send_atlan_request()
    print(response)
    print("--------------------------------------------------")
    print(response.content)


def test_create_table():
    data = {"entities": [
        {
            "typeName": "Table",
            "attributes": {
                "name": ENTITY,
                "description": "Test integration create table",
                "qualifiedName": "dynamodb/dynamodb.atlan.com/{}/{}/{}".format(DATABASE, SCHEMA, ENTITY),
                "schemaName": SCHEMA,
                "schemaQualifiedName": "dynamodb/dynamodb.atlan.com/{}/{}".format(DATABASE, SCHEMA),
                "databaseName": DATABASE,
                "databaseQualifiedName": "dynamodb/dynamodb.atlan.com/{}".format(DATABASE),
                "connectionQualifiedName": "dynamodb/dynamodb.atlan.com"
            }
        }
    ]}
    url = 'https://{}/api/meta/entity/bulk#{}'.format(api_conf.new_instance, 'createTables')
    request_object = AtlanApiRequest("POST", url, headers, json.dumps(data))
    response = request_object.send_atlan_request()
    print(response.content)


def test_restore_table():
    data = {"entities": [
        {
            "typeName": "Table",
            "attributes": {
                "name": "test database",
                "databaseName": DATABASE,
                "schemaName": SCHEMA,
                "qualifiedName": "dynamodb/dynamodb.atlan.com/{}/{}/{}".format(DATABASE, SCHEMA, ENTITY),
                "description": DESCRIPTION
            },
            "guid": "de0e287e-99b8-4442-bbd2-2a0038db3aaf",
            "status": "ACTIVE"
        }
    ]}
    url = 'https://{}/api/meta/entity/bulk#{}'.format(api_conf.new_instance, 'restoreTables')
    request_object = AtlanApiRequest("POST", url, headers, json.dumps(data))
    response = request_object.send_atlan_request()
    print(response)
    print(response.content)


def test_delete_asset(guid):
    url = 'https://{}/api/meta/entity/bulk?guid={}&deleteType=HARD#bulk_delete_assets"'.format(api_conf.instance, guid)
    response = requests.delete(url, headers=header_delete)
    print(response)


def test_db_exists(db_name):
    url = 'https://{}/api/meta/search/indexsearch#findAssetByExactName'.format(api_conf.instance)
    data = {
        "dsl": {
            "from": 0,
            "size": 1,
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "__state": "ACTIVE"
                            }
                        },
                        {
                            "match": {
                                "__typeName.keyword": "Database"
                            }
                        },
                        {
                            "match": {
                                "name.keyword": db_name
                            }
                        }
                    ]
                }
            }
        }
    }
    request_object = AtlanApiRequest("POST", url, headers, json.dumps(data))
    response = request_object.send_atlan_request()
    return response.json()['approximateCount']


def create_assets(assets, tag):
    try:
        if not assets:
            return
        logger.debug("Generating API request to create assets in bulk mode so it is searchable")
        payloads_for_bulk = map(lambda el: el.get_creation_payload_for_bulk_mode(), assets)

        payload = json.dumps({"entities": list(payloads_for_bulk)})

        print(payload)
        schema_post_url = 'https://{}/api/meta/entity/bulk#{}'.format(api_conf.instance, tag)
        atlan_api_schema_request_object = AtlanApiRequest("POST", schema_post_url, headers, payload)
        response = atlan_api_schema_request_object.send_atlan_request()
        time.sleep(1)

        logger.debug("Creating Readme and linking glossary terms...")
        for asset in assets:
            create_readme(asset)
            link_term(asset)
    except EnvVariableNotFound as e:
        logger.warning(e.message)
        raise e


def test_integrate_sample():
    logger.info("Loading sample manifest...")
    src_data = AtlanSourceFile("./docs/sample/datacatalog/manifest.csv", ",")
    src_data.load_csv()

    assets = []
    assets_info = []

    # CREATE SCHEMAS AND ENTITIES

    for index, row in src_data.assets_def.iterrows():
        if test_db_exists(row['Database']) == 0:
            test_create_database("dynamodb", row['Database'])
        if row['Table/Entity']:
            entity = Entity(entity_name=row['Table/Entity'],
                            database_name=row['Database'],
                            schema_name=row['Schema'],
                            description=row['Summary (Description)'],
                            readme=row['Readme'],
                            term=row['Term'],
                            glossary=row['Glossary'],
                            integration_type=row['Integration Type'])
            assets.append(entity)
            # Create schema from entity row in case schema row is missing
            schema = Schema(database_name=row['Database'],
                            schema_name=row['Schema'],
                            integration_type=row['Integration Type'])
            assets.append(schema)
            assets_info.append({
                'database_name': row['Database'],
                'schema_name': row['Schema'],
                'entity_name': row['Table/Entity'],
                'integration_type': row['Integration Type']
            })
        else:
            schema = Schema(database_name=row['Database'],
                            schema_name=row['Schema'],
                            description=row['Summary (Description)'],
                            readme=row['Readme'],
                            term=row['Term'],
                            glossary=row['Glossary'],
                            integration_type=row['Integration Type'])
            assets.append(schema)

        create_assets(assets, "createSchemas")

    ###########################################################
    # CREATE COLUMNS
    for asset_info in assets_info:

        database_name = asset_info['database_name']
        integration_type = asset_info['integration_type']
        schema_name = asset_info['schema_name']
        table_or_entity_name = asset_info['entity_name']

        path_csv_table = utils.get_path(integration_type, schema_name, table_or_entity_name)
        source_data = AtlanSourceFile(path_csv_table, sep=",")
        source_data.load_csv()
        includes_entity = False
        # Generate columns that are combinations of multiple variables

        if includes_entity and integration_type == INTEGRATION_TYPE_DYNAMO_DB:
            source_data.assets_def["Name"] = source_data.assets_def["Table/Entity"] + "." + source_data.assets_def[
                "Column/Attribute"]
            source_data.assets_def = source_data.assets_def.drop(columns=["Table/Entity"])
        else:
            source_data.assets_def["Name"] = source_data.assets_def["Column/Attribute"]

        logger.debug("Preparing API request to create columns for table: {}"
                     .format(schema_name if integration_type == INTEGRATION_TYPE_DYNAMO_DB else table_or_entity_name))
        columns = []

        for index, row in source_data.assets_def.iterrows():
            col = Column(integration_type=integration_type,
                         database_name=database_name,
                         entity_name=row["Table/Entity"],
                         schema_name=schema_name,
                         column_name=row['Name'],
                         data_type=row['Type'],
                         description=row['Summary (Description)'],
                         readme=row['Readme'],
                         term=row['Term'].strip(),
                         glossary=row['Glossary'].strip())
            columns.append(col)
        create_assets(columns, "createColumns")
    return assets_info


def test_create_column():
    data = {"entities": [
        {
        "typeName": "Column",
        "attributes": {
            "name": "cococococo",
            "description": "integration column test",
            "qualifiedName": "dynamodb/dynamodb.atlan.com/dynamo_db/test-table-cible/test_cible/cococococo",
            "connectorName": "dynamodb",
            "connectionQualifiedName": "dynamodb/dynamodb.atlan.com",
            "databaseName": "dynamo_db",
            "databaseQualifiedName": "dynamodb/dynamodb.atlan.com/dynamo_db",
            "schemaName": "test-table-ciblr",
            "schemaQualifiedName": "dynamodb/dynamodb.atlan.com/dynamo_db/test-table-cible",
            "tableName": "test_cible",
            "tableQualifiedName": "dynamodb/dynamodb.atlan.com/dynamo_db/test-table-cible/test_cible",
            "dataType": "String",
            "relationshipAttributes": {
                "table": {
                    "typeName": "Table",
                    "uniqueAttributes": {
                        "qualifiedName":  "dynamodb/dynamodb.atlan.com/dynamo_db/test-table-cible/test_cible"
                    }
                }
            }
        }
        }
    ]}
    url = 'https://{}/api/meta/entity/bulk#{}'.format(api_conf.instance, 'createColumns')
    request_object = AtlanApiRequest("POST", url, headers, json.dumps(data))
    response = request_object.send_atlan_request()
    print(response.content)


if __name__ == '__main__':
    # test_create_database()
    # test_create_schema()
    # test_create_table()

    # test_delete_asset("")
    #test_delete_asset("7d5ad3e7-309f-4351-8dbb-435a66f8bd48")
    #test_delete_asset("4ac7471c-8802-4968-88a5-c27c79708e35")


    #test_create_column()


    test_integrate_sample()

    # print(test_db_exists("dynamo_db"))
