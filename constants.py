BASE_PATH_ATLAN_DOCS = "/github/workspace/docs/datacatalog"
#BASE_PATH_ATLAN_DOCS = "./../atlan/data-dlh-fct-client-metrics-lc-loader/datacatalog"
#BASE_PATH_ATLAN_DOCS = "./docs/sample/datacatalog"
MANIFEST_FILE_NAME = "manifest.csv"

INTEGRATION_TYPE_DYNAMO_DB = 'dynamodb'
INTEGRATION_TYPE_ATHENA = 'glue'
INTEGRATION_TYPE_REDSHIFT = 'redshift'
INTEGRATION_TYPE_DICTIONARY = 'dictionary'

DYNAMODB_CONN_QN = "default/dynamodb/dynamodb-prod"
DICTIONARY_CONN_QN = "default/dictionary"
REDSHIFT_CONN_QN = "default/redshift"
ATHENA_CONN_QN = "default/athena"

ATHENA_DATABASE_NAME = 'AwsDataCatalog'
DYNAMO_DB_DATABASE_NAME = 'dynamo_db'
REDSHIFT_DATABASE_NAME = 'dwhstats'
DICTIONARY_DATABASE_NAME = 'default'

MANUAL_INTEGRATION = ['dynamodb', 'dictionary']

CLASSIFICATION = ['PII', 'Contractual', 'Open']
CRITICALITY_LEVEL = ['Minor', 'Major', 'Critical']