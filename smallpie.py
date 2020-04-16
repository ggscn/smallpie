import json

from io import StringIO
from google.cloud import bigquery

class SmallPie:
    def __init__(self, project_name=None, credentials=None):
        if credentials is None:
            self.bq_client = bigquery.Client()
        else:
            self.project_name = credentials.project_id
            self.bq_client = bigquery.Client(
                credentials=credentials, project=self.project_name)

    def dataset(self, dataset_name):
        return Dataset(self.bq_client, dataset_name)

    def table(self, dataset_name, table_name):
        return Table(self.bq_client, self.project_name, dataset_name, table_name)

    def query(self, query_str):
        return Query(self.bq_client, query_str)

    def copy_table(self, source_table_name, destination_table_name):
        self.bq_client.copy_table(
            source_table_name, destination_table_name)
    

    

class Dataset:
    def __init__(self, bq_client, dataset_name):
        self.bq_client = bq_client
        self.dataset_name = dataset_name

    def create(self):
        self.bq_client.create_dataset(self.dataset_name)

    def delete(self):
        self.bq_client.delete_dataset(self.dataset_name)

class Table:
    def __init__(self, bq_client, project_name, dataset_name, table_name):
        self.bq_client = bq_client
        self.project_name = project_name
        self.dataset_name = dataset_name
        self.table_name = table_name

        self.table_ref = '{}.{}.{}'.format(
            self.project_name, self.dataset_name, self.table_name)

    def create(self, schema=None):
        table = bigquery.Table(
            self.table_ref, schema)
        self.bq_client.create_table(table)

    def create_from_query(self, query_str, write_disposition='WRITE_TRUNCATE'):
        pass

    def delete(self):
        pass 

    def add_rows(self, truncate=False):
        pass

    

class LoadJob:
    def __init__(self, bq_client, chunksize=10000):
        self.bq_client = bq_client
        self.chunksize = chunksize

    def preprocess_rows(self, rows):
        file_obj = StringIO('\n'.join([json.dumps(r) for r in rows]))

    def chunkify_rows(self, rows, chunksize):
        if chunksize is None:
            chunksize = self.chunksize
        for i in range(0, len(rows), chunksize):
            yield rows[i:i + chunksize]

    def load(self):
        pass

    def clean_nans(self, rows):
        for row in rows:
            for k, v in row.items():
                if v == v:
                    pass
                else:
                    row[k] = None
        return rows

class Query:
    def __init__(self, bq_client, query_str):
        self.bq_client = bq_client
        self.query_str = query_str

    def run(self, to_dict=True):
        query_result = self.bq_client.query(
            self.query_str)
        return QueryResult(
            self.bq_client, query_result)


    

class QueryResult:
    def __init__(self, bq_client, query_result):
        self.bq_client = bq_client
        self.query_result = query_result
        
    def save_as_table(self, dataset_name, table_name, project_name=None):
        source_table_ref = self.query_result.destination

        if project_name is None:
            project_name = source_table_ref.project

        destination_table_ref = '{}.{}.{}'.format(
            project_name, dataset_name, table_name)

        self.bq_client.copy_table(
            source_table_ref, destination_table_ref)


