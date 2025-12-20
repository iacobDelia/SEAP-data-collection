
from datetime import datetime
import time
from zoneinfo import ZoneInfo
import os
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pyarrow as pa

# convert date from string to datetime
def convert_date(string_data):
    if not string_data:
        return None
    dt_obj = datetime.fromisoformat(string_data)
    return dt_obj.replace(tzinfo=None)

# removes all other characters from the CUI besides the numerical ones
def clean_CUI(CUI):
    if not CUI:
        return None
    return ''.join(filter(str.isdigit, CUI))

def write_to_dataset(list, parition_column, root_path):
    data_table = pa.Table.from_pylist(list)
    pq.write_to_dataset(
        data_table,
        partition_cols=[parition_column],
        root_path=root_path
    )
    return data_table

def save_entities(entity_list, folder_name):
    if entity_list:
        table_new = pa.Table.from_pylist(entity_list)
        # save with a name based on the current timestamp to make sure the file is unique
        ts = time.time_ns()
        file_path = f'seap_dataset/{folder_name}/batch_{ts}.parquet'
        pq.write_table(table_new, file_path)