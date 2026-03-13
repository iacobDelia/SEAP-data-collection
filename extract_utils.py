
import requests
import sys
import fitz
from asn1crypto import cms
from ocr_extract import extract_text_from_scanned_pdf
import tempfile
import os
import pyarrow.parquet as pq
import pyarrow.compute as pc
from tqdm import tqdm
import io
import magic
import aspose.words as aw
import zipfile
import py7zr
import re
import rarfile
from dotenv import load_dotenv

load_dotenv()
rarfile.UNRAR_TOOL = os.getenv("UNRAR_PATH")
# need to remember cookies for extracting the document
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://e-licitatie.ro/pub/notices/contract-notices/list/0/0'
})

def get_file_type(file_bytes):
    if not file_bytes:
        return None
    mime = magic.from_buffer(file_bytes, mime=True)
    return mime

# get the list of docs
def get_Cnotice_docs(cnotice_id):
    url = f'https://www.e-licitatie.ro/api-pub/NoticeCommon/GetDfNoticeSectionFiles/?initNoticeId={cnotice_id}&sysNoticeTypeId=2'
    r = session.get(url, timeout=10)
    return r.json()

# get the specific document
def get_document(url):
    url = f'https://www.e-licitatie.ro/{url}'
    r = session.get(url, timeout=30, stream=True)
    return r.content

def is_string_cs(text):
    text_low = text.lower()

    hints = ['caiet', 'sarcini', 'c.s.']
    avoid_hints = ['anexa', 'contract', 'formular']

    found_hint = any(hint in text_low for hint in hints)
    
    # check if we can find 'cs' by itself
    if not found_hint:
        found_hint = bool(re.search(r'(_|[^a-z]|^)cs(_|[^a-z]|$)', text_low))

    is_avoid = any(ah in text_low for ah in avoid_hints)
    return found_hint and not is_avoid

# go through the list of documents and extract the relevant one
def extract_specifications_url(response):
    doc_list = response.get('dfNoticeDocs', [])
    for doc in doc_list:
        if is_string_cs(doc.get('noticeDocumentName', '')):
            specs = get_document(doc.get('noticeDocumentUrl', ''))
            if not specs:
                raise Exception("No spec file found")
            return specs
    return ''

# extract the pdf from digitally signed file
def extract_pdf_from_p7s(p7s_bytes):
    content_info = cms.ContentInfo.load(p7s_bytes)
    compressed_data = content_info['content']['encap_content_info']['content'].native

    if isinstance(compressed_data, bytes):
        return compressed_data
    else:
        return None

# extract the raw text from a pdf
def extract_pdf_text(pdf_file):
    try:
        doc = fitz.open(stream=pdf_file, filetype="pdf")
        text_content = ""
        for page in doc:
             text_content += page.get_text()
        doc.close()
        return text_content
    except Exception as e:
         print(f"error extracting raw text from pdf: {e}")

def extract_doc_text(file):
    stream = io.BytesIO(file)
    doc = aw.Document(stream)
    return doc.get_text()


def extract_text_file(doc):
    if not doc:
        raise Exception("No doc has been received to be extracted")
    mime_type = get_file_type(doc)
    text_final = ""
    # signed document
    if "pkcs7" in mime_type or "octet-stream" in mime_type:
        doc = extract_pdf_from_p7s(doc)
        return extract_text_file(doc)
    elif mime_type == 'application/pdf':
        # extract text from pdf
        text_final = extract_pdf_text(doc)
        # scanned document
        if len(text_final) < 2000:
            # have to write to a temporary file for paddleocr
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                tmp.write(doc)
                tmp_path = tmp.name
            try:
                text_final = extract_text_from_scanned_pdf(tmp_path)
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
    # docx and doc
    elif mime_type == 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' or mime_type == 'application/msword':
        text_final = extract_doc_text(doc)
    elif mime_type == 'application/zip' or mime_type == 'application/x-7z-compressed' or "rar" in mime_type:
        with tempfile.TemporaryDirectory() as tmp_dir:
                if mime_type == 'application/zip':
                    with zipfile.ZipFile(io.BytesIO(doc)) as z:
                        z.extractall(tmp_dir)
                elif mime_type =='application/x-7z-compressed':
                    with py7zr.SevenZipFile(io.BytesIO(doc), mode='r') as z:
                            z.extractall(tmp_dir)
                elif "rar" in mime_type:
                    # need named file for rar
                    tmp_rar = tempfile.NamedTemporaryFile(delete=False, suffix=".rar")
                    tmp_rar_path = tmp_rar.name
                    try:
                        tmp_rar.write(doc)
                        tmp_rar.close() 
                        with rarfile.RarFile(tmp_rar_path) as rf:
                            rf.extractall(tmp_dir)
                    finally:
                        if os.path.exists(tmp_rar_path):
                            os.remove(tmp_rar_path)
                archive_texts = []
                # look through the extracted files and call this function agan for each of them!
                for root_dir, _, files in os.walk(tmp_dir):
                    for file in files:
                        if is_string_cs(file):
                            
                            with open(os.path.join(root_dir, file), "rb") as extracted_f:
                                content = extract_text_file(extracted_f.read())
                                archive_texts.append(f"\n--- SURSA: {file} ---\n{content}")
                text_final = "\n".join(archive_texts)

    else:
        raise Exception(f"Unsupported file type: {mime_type}")
    
    return text_final

def extract_text_cnid(cnid):
    rez = get_Cnotice_docs(cnid)
    doc = extract_specifications_url(rez)
    if not doc:
        raise Exception(f"No spec doc found")
    text_final = extract_text_file(doc)

    os.makedirs("caiete_text", exist_ok = True)
    file_path = os.path.join("caiete_text", f"caiet_sarcini_{cnid}.txt")
    with open(file_path, "w", encoding="utf-8") as f:
                f.write(text_final)


def process_ca_dataset():
    table = pq.read_table('seap_dataset/contract_awards/', columns=['cNoticeId'])
    # filter notices that dont have cnoticeid and convert to a list
    filtered_table = table.filter(pc.field("cNoticeId").is_valid())
    records = filtered_table.to_pylist()

    pbar = tqdm(records, desc="Cnotice_ids", position=0, leave=False)
    for record in pbar:
        try:
            if os.path.exists(os.path.join("caiete_text", f"caiet_sarcini_{record['cNoticeId']}.txt")):
                continue
            extract_text_cnid(record['cNoticeId'])
        except Exception as e:
            tqdm.write(f"Exception processing {record['cNoticeId']}, exception {e}")

if __name__ == "__main__":
    #extract_text_cnid(sys.argv[1])
    process_ca_dataset()
