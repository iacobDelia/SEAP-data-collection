
import requests
import sys
import fitz
from asn1crypto import cms
from ocr_extract import extract_text_from_scanned_pdf
import tempfile
import os

# need to remember cookies for extracting the document
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://e-licitatie.ro/pub/notices/contract-notices/list/0/0'
})

# get the list of docs
def get_Cnotice_docs(cnotice_id):
    headers = {'Content-Type': 'application/json;charset=UTF-8',
               'Referer': 'https://e-licitatie.ro/pub/notices/contract-notices/list/0/0'}
    url = f'https://www.e-licitatie.ro/api-pub/NoticeCommon/GetDfNoticeSectionFiles/?initNoticeId={cnotice_id}&sysNoticeTypeId=2'
    r = session.get(url, timeout=10)
    return r.json()

# get the specific document
def get_document(url):
    url = f'https://www.e-licitatie.ro/{url}'
    print(url)
    r = session.get(url, timeout=30, stream=True)
    print(r)
    return r.content

# go through the list of documents and extract the relevant one
def extract_specifications_url(response):
    doc_list = response.get('dfNoticeDocs', [])
    hints = ['caiet', 'sarcini', 'Caiet', 'Sarcini', 'CAIET', 'SARCINI', 'CS', 'cs']
    avoid_hints = ['anexa', 'Anexa', 'ANEXA']
    for doc in doc_list:
        if any(hint in doc.get('noticeDocumentName', '') for hint in hints) and not any(hint in doc.get('noticeDocumentName', '') for hint in avoid_hints):
            specs = get_document(doc.get('noticeDocumentUrl', ''))
            return specs
    return ''

# extract the pdf from digitally signed file
def extract_pdf_from_p7s(p7s_bytes):
    try:
        content_info = cms.ContentInfo.load(p7s_bytes)
        compressed_data = content_info['content']['encap_content_info']['content'].native

        if isinstance(compressed_data, bytes):
            return compressed_data
        else:
            return None
    except Exception as e:
        print(f"error extracting pdf from p7s: {e}")

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


if __name__ == "__main__":
    rez = get_Cnotice_docs(sys.argv[1])
    print(rez)
    doc = extract_specifications_url(rez)
    my_pdf = extract_pdf_from_p7s(doc)
    text_pdf = extract_pdf_text(my_pdf)
    # scanned document
    if len(text_pdf) < 100:
        print("intrat in extragere ocr")
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(my_pdf)
            tmp_path = tmp.name
        try:
            text_pdf = extract_text_from_scanned_pdf(tmp_path)
        finally:
             if os.path.exists(tmp_path):
                os.remove(tmp_path)

    with open(f"caiet_sarcini_{sys.argv[1]}.txt", "w", encoding="utf-8") as f:
                f.write(text_pdf)
