import json

from io import StringIO
from google.cloud import bigquery
from utils import stringify_rows

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

    def table(self, table_address):
        return Table(self.bq_client, self.project_name, table_address)

    def query(self, query_str):
        return Query(self.bq_client, query_str)

    def copy_table(self, source_table_name, destination_table_name):
        self.bq_client.copy_table(
            source_table_name, destination_table_name)

    def load_job(self, table_address, *args, **kwargs):
        return LoadJob(
            self.bq_client, table_address, *args, **kwargs)
    

    

class Dataset:
    def __init__(self, bq_client, dataset_name):
        self.bq_client = bq_client
        self.dataset_name = dataset_name

    def create(self):
        self.bq_client.create_dataset(self.dataset_name)

    def delete(self):
        self.bq_client.delete_dataset(self.dataset_name)

class Table:
    def __init__(self, bq_client, project_name, table_address):
        self.bq_client = bq_client
        self.project_name = project_name
        self.dataset_name, self.table_name = table_address.split('.')

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
    def __init__(self, bq_client, table_address, chunksize=10000, cast_data=False, **kwargs):
        
        self.bq_client = bq_client
        self.chunksize = chunksize
        self.job_config = bigquery.LoadJobConfig()
        self.job_config.source_format = 'NEWLINE_DELIMITED_JSON'

        self.job_config.autodetect = kwargs.get(
            'autodetect', False)
        self.job_config.write_disposition = kwargs.get(
            'write_disposition', 'WRITE_APPEND')

        if 'autodetect' in kwargs and kwargs['autodetect']:
            self.job_config.create_disposition = 'CREATE_IF_NEEDED'

        self.dataset_name, self.table_name = table_address.split('.')
        
        self.table_ref = '{}.{}.{}'.format(
            bq_client.project, self.dataset_name, self.table_name)


    def chunkify_rows(self, rows):
        for i in range(0, len(rows), self.chunksize):
            yield rows[i:i + self.chunksize]

    def upload(self, rows):
        for chunk in self.chunkify_rows(rows):
            string_obj = stringify_rows(chunk)
            self.load_file(string_obj)
            string_obj.close()

    def transform_rows(self):
        pass

    def load_file(self, file_obj):

            load_job = self.bq_client.load_table_from_file(
                file_obj, self.table_ref, job_config=self.job_config)

            load_job.result()
            loaded_rows = load_job.output_rows
            return loaded_rows

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

    def __iter__(self):
        return self.to_dict()

    def save_as_table(self, table_address, project_name=None):
        
        source_table_ref = self.query_result.destination
        dataset_name, table_name = table_address.split('.')

        if project_name is None:
            project_name = source_table_ref.project

        destination_table_ref = '{}.{}.{}'.format(
            project_name, dataset_name, table_name)

        self.bq_client.copy_table(
            source_table_ref, destination_table_ref)
    
    def to_dict(self):
        schema = self.query_result.schema
        results = self.query_result

        cols = [x.name for x in schema]
        rows = [{c: r[i] for i, c in enumerate(cols)} for r in results]

        return rows



