from google.oauth2 import service_account
from google.cloud import bigquery
from smallpie import SmallPie

key_path = 'secret_loc'

credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = SmallPie(credentials=credentials)


query_str = 'SELECT 1 as test_col'
#client.dataset('test_dataset').create()
client.query(query_str).run().save_as_table('test_dataset', 'test_table')

