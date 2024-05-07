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

CLASSIFICATION = ['PII', 'Contractual', 'Open']
CRITICALITY_LEVEL = ['Minor', 'Major', 'Critical']