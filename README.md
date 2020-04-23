# SmallPie

SmallPie is an opininated Python library for Google BigQuery that abstracts many common BigQuery use-cases. A few of the features:

* Memory-conscious chunked file uploads
* Automated data pre-processing
* Save query results to table

## Examples

#### Instantiate
```python
from smallpie import SmallPie
client = SmallPie()
```

#### Run and return a query as a list of dicts
```python
query_str = 'SELECT 1 as test_col'
rows = client.query(query_str).run().to_dict()
```

#### Save query results as a table
```python
query_str = 'SELECT 1 as test_col'
client.query(query_str).run().save_as_table('my_dataset.my_table')
```

#### A rows to a table
```python
query_str = 'SELECT 1 as test_col'
client.table('my_dataset.my_table').add_rows()
```

#### Initialize with your own credentials

```python
from google.oauth2 import service_account
from smallpie import SmallPie

key_path = '/path/to/secret'

credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = SmallPie(credentials=credentials)
```
