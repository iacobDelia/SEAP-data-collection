
from datetime import datetime
import time
from zoneinfo import ZoneInfo
import os
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pyarrow as pa
import seap_requests

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


# load entity ids
def load_entity_ids(entity_type, entity_id):
    path = f'seap_dataset/{entity_type}'
    if os.path.exists(path) and any(f.endswith('.parquet') for f in os.listdir(path)):
        table = pq.read_table(path, columns=[entity_id])
        return set(table[entity_id].to_pylist())
    return set()

def get_notice_entry(item, date, info_dict):
    

    # these sections have a suffix for utility acquisitions    
    suffix = "_U" if info_dict.get('caNoticeEdit_New') is None else ""
    root = info_dict.get(f'caNoticeEdit_New{suffix}')
    section2 = root.get(f'section2_New{suffix}')

    lots = section2.get(f'section2_2_New{suffix}', {}).get('descriptionList', [])
    totalAcquisitionValue = section2.get(f'section2_1_New{suffix}', {}).get('totalAcquisitionValue', None)
    mainCPVCode = section2.get(f'section2_1_New{suffix}', {}).get('mainCPVCode', {}).get('localeKey', None)
    caPublicationDate = convert_date(root.get('publicationDetailsModel', {}).get('caPublicationDate', None))
    publicationDate = convert_date(root.get('publicationDetailsModel').get('publicationDate', None))

    authority_address = root.get(f'section1_New{suffix}').get('section1_1', {}).get('caAddress', {})
    final_item = {
        'caNoticeId': item.get('caNoticeId', None),
        'noticeId': item.get('noticeId', None),
        'sysNoticeTypeId': item.get('sysNoticeTypeId', None),
        'sysProcedureState': item.get('sysProcedureState', {}).get('id', None),
        'sysProcedureType': item.get('sysProcedureType', {}).get('id', None),
        'contractTitle': item.get('contractTitle', None),

        'sysAcquisitionContractType': item.get('sysAcquisitionContractType', {}).get('id', None),
        'sysProcedureType': item.get('sysProcedureType', {}).get('id', None),
        'sysContractAssigmentType': (item.get('sysContractAssigmentType', {}) or {}).get('id', None),

        'ronContractValue': item.get('ronContractValue', None),
        'title': info_dict.get('title', None),
        'totalAcquisitionValue': totalAcquisitionValue,
        'estimatedValue': sum(lot.get('estimatedValue', None) for lot in lots if lot.get('estimatedValue') is not None),
        'mainCPVCode': mainCPVCode,
        # if at least one lot is EU funded, mark the whole CA as so
        'isEUFunded': any(lot.get('isEUFunded', False) for lot in lots),
        'authorityId': authority_address.get('entityId', None),

        'caPublicationDate': caPublicationDate,
        'publicationDate': publicationDate,

        'year': date.year
    }
    return final_item

def get_authority_entry(info_dict):
    suffix = "_U" if info_dict.get('caNoticeEdit_New') is None else ""
    root = info_dict.get(f'caNoticeEdit_New{suffix}')
    authority_address = root.get(f'section1_New{suffix}').get('section1_1', {}).get('caAddress', {})

    nuts_text = (authority_address.get('nutsCodeItem') or {}).get('text', "")
    _, _, county = nuts_text.partition(" ")
    new_authority = {
        'authorityId': authority_address.get('entityId', None),
        'officialName': authority_address.get('officialName', None),
        'county':  county,
        'country': authority_address.get('country', None)
    }
    return new_authority