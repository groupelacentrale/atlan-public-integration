from pyatlan.client.atlan import AtlanClient
import logging

from pyatlan.errors import AtlanError

import utils
utils.setup_logger('main_logger')
logger = logging.getLogger('main_logger')
client = AtlanClient()
name="Starwars"
try:
    term = client.asset.find_term_by_name( #
        name=name, #
        glossary_name="Glossary (EN)", #
        attributes=None) #
    logger.info("Find term {}".format(term.guid))
except AtlanError as err:
    logger.error("Unable to find term %s", name )
    logger.error("%s",err)
