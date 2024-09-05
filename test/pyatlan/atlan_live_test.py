from pyatlan.client.atlan import AtlanClient
import logging

from pyatlan.errors import AtlanError
from pyatlan.model.assets import Asset, Table

import utils

utils.setup_logger('main_logger')
logger = logging.getLogger('main_logger')
client = AtlanClient()


# Find term by name

def test_find_term_by_name():
    name = "Starwars"
    try:
        term = client.asset.find_term_by_name(  #
            name=name,  #
            glossary_name="Glossary (EN)",  #
            attributes=None)  #
        logger.info("Find term guid:{}".format(term.guid))
    except AtlanError as err:
        logger.error("Unable to find term %s", name)
        logger.error("%s", err)


def test_get_by_guid(guid):
    try:
        asset: Asset = client.asset.get_by_guid(guid=guid)
        logger.info("With Atlan CDK Python. Find asset qualified name:{} guid:{} description:{}".format(
            asset.attributes.qualified_name,
            asset.guid,
            asset.attributes.description))
        # logger.info("Detail {}".format(asset))
    except AtlanError as err:
        logger.error("Unable to find asset %s", guid)
        logger.error("%s", err)


# https://developer.atlan.com/snippets/advanced-examples/delete/
def test_delete_by_guid(guid):
    try:
        response = client.asset.delete_by_guid(guid)
        # logger.info("Delete response {}".format(response))
        if deleted := response.assets_deleted(asset_type=Asset):
            logger.info("Asset {} has been successfully deleted".format(deleted))
        # else:
        #    logger.error("Pas de deleted in the response")
    except AtlanError as err:
        logger.error("Unable to delete asset %s", guid)
        logger.error("%s", err)


def test_purge_by_guid(guid):
    try:
        response = client.asset.purge_by_guid(guid)
        # logger.info("Delete response {}".format(response))
        if purged := response.assets_deleted(asset_type=Asset):
            logger.info("Asset {} has been successfully purged".format(purged))
        # else:
        #    logger.error("Pas de deleted in the response")
    except AtlanError as err:
        logger.error("Unable to purge asset %s", guid)
        logger.error("%s", err)


def test_get_guid_by_qualified_name(qualified_name):
    try:
        asset: Asset = client.asset.get_by_qualified_name(
            asset_type=Table,
            qualified_name=qualified_name
        )
        logger.info("Find asset qualified name:{} guid:{} description:{}".format(asset.attributes.qualified_name,  #
                                                                                 asset.guid,
                                                                                 asset.attributes.description))
    except AtlanError as err:
        logger.error("Unable to purge asset %s", qualified_name)
        logger.error("%s", err)


def test_change_description(qualified_name, name):
    table = Table.updater(
        qualified_name=qualified_name,
        name=name,
    )
    table.description = "CDK Python Atlan integration"
    response = client.asset.save(table)
    logger.info(response)


def test_create_table_asset(name, schema_qualified_name):
    table = Table.creator(  #
        name=name,
        schema_qualified_name=schema_qualified_name,
    )

# DATACAT-304 (DONE)
# Try to delete assets "labelo"
# https://groupelacentrale.atlan.com/assets/2a15caea-c918-443a-b8a6-78461a8ddb95/overview
# guid:2a15caea-c918-443a-b8a6-78461a8ddb95
# https://groupelacentrale.atlan.com/assets/3bccdfb8-e908-4ba7-992a-fdcf2fbc1c61/overview
# guid:3bccdfb8-e908-4ba7-992a-fdcf2fbc1c61
# test_get_by_guid('2a15caea-c918-443a-b8a6-78461a8ddb95')
# test_purge_by_guid('2a15caea-c918-443a-b8a6-78461a8ddb95')
# test_get_by_guid('3bccdfb8-e908-4ba7-992a-fdcf2fbc1c61')
test_purge_by_guid('5d271ce0-d1a0-4fe7-abff-df91e40b7e27')

#test_get_by_guid("5bc074e2-3b44-4166-abfb-5ccff3265008")

# test_get_guid_by_qualified_name("default/dynamodb/dynamodb-prod/dynamo_db/test-table-cible2/test_cible2")

#test_change_description("default/dynamodb/dynamodb-prod/dynamo_db/test-table-cible3/test_cible3", "test_cible3")
