
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

# writes the batches for contract_awards and contracts, separated by year, not used anymore
def write_to_dataset(list, parition_column, root_path):
    data_table = pa.Table.from_pylist(list)
    pq.write_to_dataset(
        data_table,
        partition_cols=[parition_column],
        root_path=root_path
    )
    return data_table

# writes the batches for contractors and authorities
def save_entities(entity_list, folder_name, start_date, final_date):
    if entity_list:
        table_new = pa.Table.from_pylist(entity_list)
        # save with a name based on the current timestamp to make sure the file is unique
        ts = time.time_ns()
        file_path = f'seap_dataset/{folder_name}/batch_{ts}_{start_date:%Y-%m-%d}_{final_date:%Y-%m-%d}.parquet'
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
def get_notice_entry(item, info_dict):
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

# generates a lot entry
def get_lots_entry(lot_item, caNoticeId):
    cpv = lot_item.get('mainCPVCodes', {}).get('text')
    return {
        'lotId': lot_item.get('noticeLotID', None),
        'caNoticeId': caNoticeId,
        'contractTitle': lot_item.get('contractTitle', None),
        'CPV': cpv[:10] if cpv else '',
        'estimatedValue': lot_item.get('estimatedValue', None),
        'sysAwardCriteriaType': lot_item.get('sysAwardCriteriaType', {}).get('text', None),
        'caNoticeContractId': None
    }

# generates a contract entry
def get_contract_entry(contract, detailed_contract):
    return {
    'caNoticeContractId': contract.get('caNoticeContractId', None),
    'contractId': detailed_contract.get('contractId', None),
    'caNoticeId': contract.get('caNoticeId', None),
    'contractTitle': contract.get('contractTitle', None),
    'contractDate': convert_date(contract.get('contractDate')),
    #'winnerCUI': winnerCUI,
    'estimatedContractValue': (detailed_contract.get('section524') or {}).get('estimatedContractValue', None),
    'contractValue': contract.get('defaultCurrencyContractValue', None),
    'isFrameworkAgreement': detailed_contract.get('isFrameworkAgreement', None),
    'numberOfReceivedOffers': (detailed_contract.get('section522') or {}).get('numberOfReceivedOffers', None),
    'numberOfWinners': len(detailed_contract.get('winnerList', []))
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

# merges all the parquet files
def merge_everything():
    tables = ['authorities', 'contract_awards', 'contracts',
             'contractors', 'lots', 'contract_winners']

    for table_name in tables:
        path = 'seap_dataset/' + table_name
        file_list = [os.path.join(path, f) for f in os.listdir(path)]
        ts = time.time_ns()
        merge_parquet(file_list, f'seap_dataset/{table_name}/{table_name}_{ts}.parquet')

# merges all parquet files from a given list
def merge_parquet(files, name):
    if not files:
        return
    schema = pq.ParquetFile(files[0]).schema_arrow
    with pq.ParquetWriter(name, schema=schema) as writer:
        for file in files:
            writer.write_table(pq.read_table(file, schema=schema))
    for file in files:
        os.remove(file)
