from google.oauth2 import service_account
from google.cloud import bigquery
from smallpie import SmallPie



client = SmallPie()
client.table('test_dataset.test_table2')

def test_save_query_result_as_table():
    query_str = 'SELECT 1 as test_col'
    #client.dataset('test_dataset').create()
    client.query(query_str).run().save_as_table('test_dataset', 'test_table')

def test_load_rows():
    rows = [{'row1': 1, 'row2': 2, 'row3': 3}]
    client.load_job('test_dataset.test_table2', autodetect=True).upload(rows)
test_load_rows()