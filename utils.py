
from datetime import datetime
import time
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

# writes the batches for contract_awards and contracts, separated by year
def write_to_dataset(list, parition_column, root_path):
    data_table = pa.Table.from_pylist(list)
    pq.write_to_dataset(
        data_table,
        partition_cols=[parition_column],
        root_path=root_path
    )
    return data_table

# writes the batches for contractors and authorities
def save_entities(entity_list, folder_name):
    if entity_list:
        table_new = pa.Table.from_pylist(entity_list)
        # save with a name based on the current timestamp to make sure the file is unique
        ts = time.time_ns()
        file_path = f'seap_dataset/{folder_name}/batch_{ts}.parquet'
        pq.write_table(table_new, file_path)

# load entity ids (contactors and authorities)
def load_entity_ids(entity_type, entity_id):
    path = f'seap_dataset/{entity_type}'
    if os.path.exists(path) and any(f.endswith('.parquet') for f in os.listdir(path)):
        # load only the column that is needed
        table = pq.read_table(path, columns=[entity_id])
        return set(table[entity_id].to_pylist())
    return set()

# generates a contract award entry
def get_notice_entry(item, date, info_dict):
    # these sections have a suffix for utility acquisitions    
    suffix = "_U" if info_dict.get('caNoticeEdit_New') is None else ""
    root = info_dict.get(f'caNoticeEdit_New{suffix}')
    section2 = root.get(f'section2_New{suffix}')

    lots = section2.get(f'section2_2_New{suffix}', {}).get('descriptionList', [])
    totalAcquisitionValue = section2.get(f'section2_1_New{suffix}', {}).get('totalAcquisitionValue', None)
    mainCPVCode = section2.get(f'section2_1_New{suffix}', {}).get('mainCPVCode', {}).get('localeKey', None)
    caPublicationDate = convert_date(root.get('publicationDetailsModel', {}).get('caPublicationDate', None))
    pub_details = (root.get('publicationDetailsModel') or {})
    publicationDate = convert_date(pub_details.get('publicationDate'))

    authority_address = root.get(f'section1_New{suffix}').get('section1_1', {}).get('caAddress', {})
    return {
        'caNoticeId': item.get('caNoticeId', None),
        'noticeId': item.get('noticeId', None),
        'sysNoticeTypeId': item.get('sysNoticeTypeId', None),
        'sysProcedureState': item.get('sysProcedureState', {}).get('text', None),
        'sysProcedureType': item.get('sysProcedureType', {}).get('text', None),
        'contractTitle': item.get('contractTitle', None),

        'sysAcquisitionContractType': item.get('sysAcquisitionContractType', {}).get('text', None),
        'sysProcedureType': item.get('sysProcedureType', {}).get('text', None),
        'sysContractAssigmentType': (item.get('sysContractAssigmentType', {}) or {}).get('text', None),

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

# generates an authority entry
def get_authority_entry(info_dict):
    suffix = "_U" if info_dict.get('caNoticeEdit_New') is None else ""
    root = info_dict.get(f'caNoticeEdit_New{suffix}')
    authority_address = root.get(f'section1_New{suffix}').get('section1_1', {}).get('caAddress', {})

    nuts_text = (authority_address.get('nutsCodeItem') or {}).get('text', "")
    _, _, county = nuts_text.partition(" ")
    return {
        'authorityId': authority_address.get('entityId', None),
        'officialName': authority_address.get('officialName', None),
        'county':  county,
        'country': authority_address.get('country', None)
    }

# generates a contract entry
def get_contract_entry(date, contract, winnerCUI, detailed_contract):
    return {
    'caNoticeContractId': contract.get('caNoticeContractId', None),
    'caNoticeId': contract.get('caNoticeId', None),
    'contractTitle': contract.get('contractTitle', None),
    'contractDate': convert_date(contract.get('contractDate')),
    'winnerCUI': winnerCUI,
    'estimatedContractValue': (detailed_contract.get('section524') or {}).get('estimatedContractValue', None),
    'contractValue': contract.get('defaultCurrencyContractValue', None),
    'numberOfReceivedOffers': (detailed_contract.get('section522') or {}).get('numberOfReceivedOffers', None),
    'year': date.year,
    }

# generates a contractor entry
def get_contractor_entry(winnerCUI, address, isIndividual):
    return {
        'CUI': winnerCUI,
        'isIndividual': isIndividual,
        'officialName': address.get('officialName', None),
        # county may be blank sometimes
        'county': (address.get('county') or {}).get('text', None),
        'country': address.get('country', None),
        'isSME': address.get('isSME', None)
    }

def merge_everything():
    path_authorities = 'seap_dataset/authorities'
    path_contract_awards = 'seap_dataset/contract_awards'
    path_contracts = 'seap_dataset/contracts'
    path_contractors = 'seap_dataset/contractors'

    authorities_list = [os.path.join(path_authorities, f) for f in os.listdir(path_authorities)]
    ts = time.time_ns()
    merge_parquet(authorities_list, f'seap_dataset/authorities/authorities_{ts}.parquet')
    contractors_list = [os.path.join(path_contractors, f) for f in os.listdir(path_contractors)]
    ts = time.time_ns()
    merge_parquet(contractors_list, f'seap_dataset/contractors/contractors_{ts}.parquet')
    for directory in os.listdir(path_contract_awards):
        crt_folder_path = os.path.join(path_contract_awards, directory)
        ca_list = [os.path.join(crt_folder_path, f) for f in os.listdir(crt_folder_path)]
        ca_list = list(filter(lambda f: f.endswith('.parquet'), ca_list))
        ts = time.time_ns()
        merge_parquet(ca_list, os.path.join(crt_folder_path, f'contract_awards_{ts}.parquet'))
    
    for directory in os.listdir(path_contracts):
        crt_folder_path = os.path.join(path_contracts, directory)
        ca_list = [os.path.join(crt_folder_path, f) for f in os.listdir(crt_folder_path)]
        ca_list = list(filter(lambda f: f.endswith('.parquet'), ca_list))
        ts = time.time_ns()
        merge_parquet(ca_list, os.path.join(crt_folder_path, f'contracts_{ts}.parquet'))


def merge_parquet(files, name):
    if not files:
        return
    schema = pq.ParquetFile(files[0]).schema_arrow
    with pq.ParquetWriter(name, schema=schema) as writer:
        for file in files:
            writer.write_table(pq.read_table(file, schema=schema))
    for file in files:
        os.remove(file)
