import json
import os
import sys
import re
import pyarrow as pa
import pyarrow.parquet as pq
import time
import random
from tqdm import tqdm
from dotenv import load_dotenv
from google import genai
from google.genai import types
from concurrent.futures import ThreadPoolExecutor, as_completed
load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("SERVICE_ACCOUNT_KEY")

client = genai.Client(
    vertexai=True, 
    project="my-project-seap", 
    location="us-central1"
)

MODEL_ID = "gemini-2.5-flash" 

SYSTEM_PROMPT = """
Ești un expert în achiziții publice IT (CPV 72000000). Analizează textul și extrage datele într-un SINGUR obiect JSON strict (nu listă).

Reguli de calcul și agregare:
1. nr_module_software: Suma totală a componentelor funcționale distincte. Dacă documentul descrie mai multe sisteme/sub-sisteme, adună-le pe toate într-un singur număr (integer).
2. nr_experti_cheie: Numărul total de PERSOANE solicitate în tot documentul (ex: 3 programatori + 1 manager = 4). Dacă sunt menționate echipe diferite pentru module diferite, adună totalul persoanelor (integer).
3. durata_proiect_luni: Cea mai lungă durată de implementare calendaristică identificată în document, excluzând mentenanța (integer).

Constrângere format:
- Returnează strict un obiect de tip dicționar: {"nr_module_software": X, "nr_experti_cheie": Y, "durata_proiect_luni": Z}.
- NU returna o listă de obiecte, chiar dacă documentul are mai multe secțiuni.
- Dacă o informație lipsește, folosește null.
"""

def analyze_document(file_path):
    if not os.path.exists(file_path):
        print(f"Error: {file_path} not found")
        return None

    max_retries = 20
    base_delay = 5

    for attempt in range(max_retries):
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                document_text = file.read()

            response = client.models.generate_content(
                model=MODEL_ID,
                contents=document_text,
                config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_PROMPT,
                    response_mime_type="application/json",
                ),
            )

            return json.loads(response.text)

        except Exception as e:
            error_msg = str(e)
            if "429" in error_msg or "RESOURCE_EXHAUSTED" in error_msg:
                delay = base_delay * (attempt + 1)
                
                tqdm.write(f"\n Limit hit for {os.path.basename(file_path)}. Retrying in {delay:.1f}s (Try {attempt+1}/{max_retries})...")
                
                time.sleep(delay)
                continue
            else:
                print(f"\n Error analyzing file {file_path}: {e}")
                return None
    
    print(f"\n gave up on {file_path} after {max_retries} tries")
    return None

def update_parquet_with_results(result_list, output_path):
    new_data_table = pa.Table.from_pylist(result_list)

    if os.path.exists(output_path):
        existing_table = pq.read_table(output_path)

        new_data_table = new_data_table.set_column(
            new_data_table.schema.get_field_index("caNoticeId"), 
            "caNoticeId", 
            new_data_table.column("caNoticeId").cast(pa.int64())
        )

        # left join
        final_table = existing_table.join(new_data_table, keys="caNoticeId", join_type="left outer")
        
        print("done merging!")

    base, ext = os.path.splitext(output_path)
    new_path = f"{base}_merged{ext}"
    pq.write_table(final_table, new_path)
    print(f"Saved to: {new_path}")

def worker(filename, directory_path):
    full_path = os.path.join(directory_path, filename)
    json_result = analyze_document(full_path)
    
    if json_result:
        try:
            caNoticeId_match = re.search(r'\d+', filename)
            if caNoticeId_match:
                caNoticeId = int(caNoticeId_match.group())
                return {
                    "caNoticeId": caNoticeId,
                    "softwareModules": json_result.get("nr_module_software"),
                    "experts": json_result.get("nr_experti_cheie"),
                    "projectDuration": json_result.get("durata_proiect_luni")
                }
        except Exception as e:
            print(f"Error processing {filename}: {e}")
    
    return None


def iterate_files(directory_path, num_threads):
    all_files = [f for f in os.listdir(directory_path) if f.endswith(".txt")]
    results = []

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = {executor.submit(worker, file, directory_path): file for file in all_files}

        for future in tqdm(as_completed(futures), total=len(all_files), desc="Spec files"):
            try:
                res = future.result()
                if res:
                    results.append(res)
            except Exception as e:
                print(f"Error: {e}")
    return results

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("please specify input files and output files :(")
    else:
        num_threads = int(sys.argv[3]) if len(sys.argv) > 3 else 5

        file_to_analyze = sys.argv[1]
        result_list = iterate_files(sys.argv[1], num_threads)
        
        update_parquet_with_results(result_list, sys.argv[2])
