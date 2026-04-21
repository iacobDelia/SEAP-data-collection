import pyarrow.parquet as pq
import pyarrow.compute as pc
import subprocess
import sys
import os
from datetime import datetime

if __name__ == "__main__":
    table = pq.read_table('seap_dataset/contract_awards_IF', columns=['caPublicationDate'])

    latest_date_val = pc.max(table['caPublicationDate']).as_py()
    start_date = latest_date_val.strftime('%Y-%m-%d')
    # current date
    end_date = datetime.now().strftime('%Y-%m-%d')
    CPV = '72000000'
    batch_size = "500"
    print("start api extraction")
    subprocess.run([
        sys.executable, 
        "extract_api.py", 
        start_date, 
        end_date, 
        batch_size,
        CPV
    ])
    print("start spect extraction")
    subprocess.run([
        sys.executable, 
        "extract_spec.py", 
        start_date, 
        end_date, 
    ])
    print("start llm extraction")
    subprocess.run([sys.executable, "llm_extract_spec.py"])
    print("start running isolation forest")
    subprocess.run([sys.executable, "run_IF.py"])
    print("now copying and pushing to github")
