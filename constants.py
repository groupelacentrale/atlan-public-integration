# Convention compatible avec GitHub action ; le checkout du projet cible se fait dans /github/workspace.
BASE_PATH_ATLAN_DOCS = "/github/workspace/docs/datacatalog"

MANIFEST_FILE_NAME = "manifest.csv"

INTEGRATION_TYPE_DYNAMO_DB = 'dynamodb'
INTEGRATION_TYPE_ATHENA = 'glue'
INTEGRATION_TYPE_REDSHIFT = 'redshift'

DYNAMODB_CONN_QN = "default/dynamodb/dynamodb-prod"
REDSHIFT_CONN_QN = "default/redshift"
ATHENA_CONN_QN = "default/athena"

ATHENA_DATABASE_NAME = 'AwsDataCatalog'
DYNAMO_DB_DATABASE_NAME = 'dynamo_db'
REDSHIFT_DATABASE_NAME = 'dwhstats'

CLASSIFICATION_TAGS_DICT = {'PII':'GeJIlnTnPXuwGNAcH5f02x',
                            'Contractual': 'P2pyJIw6xIOuo73dHkP5gB',
                            'Open':'bm9LZV3AaKgAgldAq4FI4T'}

CRITICALITY_LEVEL = ['Minor', 'Major', 'Critical']

SERVICE_ACC_API_NAME = "service-account-apikey-e3df9f7b-42ba-4562-98ba-8bc1222221f8"
