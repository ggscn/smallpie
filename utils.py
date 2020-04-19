import pickle
import json
from io import StringIO

def rows_to_bytes(rows):
    for row in rows:
        yield json.dumps(row)

def stringify_rows(rows):

    string_obj = StringIO(
        '\n'.join(list(rows_to_bytes(rows))))

    return string_obj


