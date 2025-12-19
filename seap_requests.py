
import requests
import time
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from zoneinfo import ZoneInfo
import os



# get detailed info for a contract award
def get_info_CANotice(id):
    headers = {'Content-Type': 'application/json;charset=UTF-8',
               'Referer': 'https://e-licitatie.ro/pub/notices/contract-notices/list/0/0'}
    url = 'http://e-licitatie.ro/api-pub/C_PUBLIC_CANotice/get/' + id
    r = requests.get(url, headers = headers)
    return r.json()

# convert date from string to datetime
def convert_date(string_data):
    if not string_data:
        return None
    dt_obj = datetime.fromisoformat(string_data)
    return dt_obj.astimezone(ZoneInfo("Europe/Bucharest"))

def send_req():
    headers = {'Content-Type': 'application/json;charset=UTF-8',
               'Referer': 'https://e-licitatie.ro/pub/notices/contract-notices/list/0/0'}
    url = 'http://e-licitatie.ro/api-pub/NoticeCommon/GetCANoticeList/'
    body = {
    'sysNoticeTypeIds': [
        3,
        13,
        18,
        16,
        8
    ],
    'sortProperties': [],
    'pageSize': 2000,
    'sysNoticeStateId': None,
    'contractingAuthorityId': None,
    'winnerId': None,
    'cPVCategoryId': None,
    'sysContractAssigmentTypeId': None,
    'cPVId': None,
    'assignedUserId': None,
    'sysAcquisitionContractTypeId': None,
    'pageIndex': 0,
    'startPublicationDate': '2025-04-30',
    'endPublicationDate': '2025-04-30'
    }
    r = requests.post(url, headers = headers, json = body, timeout = 5)
    print(len(r.json()['items']))

    cnt = 0
    final_list = []
    for item in r.json()['items']:
        # ignore framework agreements
        if item['sysContractAssigmentType']['id'] != 3:
            print(item['caNoticeId'])
            info_dict = get_info_CANotice(str(item['caNoticeId']))
            # These sections look different for utility acquisitions
            if(info_dict['caNoticeEdit_New'] == None):
                lots = info_dict['caNoticeEdit_New_U']['section2_New_U']['section2_2_New_U']['descriptionList']
                totalAcquisitionValue =  info_dict['caNoticeEdit_New_U']['section2_New_U']['section2_1_New_U']['totalAcquisitionValue']
                mainCPVCode = info_dict['caNoticeEdit_New_U']['section2_New_U']['section2_1_New_U']['mainCPVCode']['localeKey']
                caPublicationDate = convert_date(info_dict['caNoticeEdit_New_U']['publicationDetailsModel']['caPublicationDate'])
                publicationDate = convert_date(info_dict['caNoticeEdit_New_U']['publicationDetailsModel']['publicationDate'])
            else:
                lots = info_dict['caNoticeEdit_New']['section2_New']['section2_2_New']['descriptionList']
                totalAcquisitionValue =  info_dict['caNoticeEdit_New']['section2_New']['section2_1_New']['totalAcquisitionValue']
                mainCPVCode = info_dict['caNoticeEdit_New']['section2_New']['section2_1_New']['mainCPVCode']['localeKey']
                caPublicationDate = convert_date(info_dict['caNoticeEdit_New']['publicationDetailsModel']['caPublicationDate'])
                publicationDate = convert_date(info_dict['caNoticeEdit_New']['publicationDetailsModel']['publicationDate'])
            final_item = {
                'caNoticeId': item['caNoticeId'],
                'noticeId': item['noticeId'],
                'sysNoticeTypeId': item['sysNoticeTypeId'],
                'sysProcedureState': item['sysProcedureState']['id'], # 5 = Atribuita, 2 = Anulata
                'sysProcedureType': item['sysProcedureType']['id'],
                'contractTitle': item['contractTitle'],

                'sysAcquisitionContractType': item['sysAcquisitionContractType']['id'],
                'sysProcedureType': item['sysProcedureType']['id'],
                'sysContractAssigmentType': item['sysContractAssigmentType']['id'],
                'ronContractValue': item['ronContractValue'],
                'title': info_dict['title'],
                'totalAcquisitionValue': totalAcquisitionValue,
                'estimatedValue': sum(lot['estimatedValue'] for lot in lots if lot['estimatedValue'] != None),
                'mainCPVCode': mainCPVCode,
                # if at least one lot is EU funded, mark the whole notice as so
                'isEUFunded': any(lot['isEUFunded'] for lot in lots),
                'entityId': info_dict['entityId'],
                'caPublicationDate': caPublicationDate,
                'publicationDate': publicationDate
                }
            final_list.append(final_item)
            cnt +=1
            time.sleep(2)
        if cnt == 15:
            break
    
    data_dict_table = pa.Table.from_pylist(final_list)
    pq.write_table(data_dict_table, 'contract_awards.parquet')

    print(pq.read_schema('contract_awards.parquet'))

# removes all other characters from the CUI besides the numerical ones
def clean_CUI(CUI):
    if not CUI:
        return None
    return ''.join(filter(str.isdigit, CUI))

def get_contract_info(caNoticeId):
    headers = {'Content-Type': 'application/json;charset=UTF-8',
               'Referer': 'https://e-licitatie.ro/pub/notices/contract-notices/list/0/0'}
    url = 'http://e-licitatie.ro/api-pub/C_PUBLIC_CANotice/GetCANoticeContracts'
    body = {
    "caNoticeId": caNoticeId,
    "contractNo": None,
    "winnerTitle": None,
    "winnerFiscalNumber": None,
    "contractDate": {
        "from": None,
        "to": None
    },
    "contractValue": {
        "from": None,
        "to": None
    },
    "contractMinOffer": {
        "from": None,
        "to": None
    },
    "contractMaxOffer": {
        "from": None,
        "to": None
    },
    "contractTitle": None,
    "lots": None,
    "sortOrder": [],
    "sysContractFrameworkType": {},
    "skip": 0,
    "take": 5
    }
    r = requests.post(url, headers = headers, json = body, timeout = 5)
    return r.json()['items']

def get_contracts():
    if not os.path.exists('contract_awards.parquet'):
        print("File 'contract_awards.parquet' could not be found.")
        return
    if os.path.exists('contractors.parquet'):
        pq_table_contractors = pq.read_table('contractors.parquet', columns = ["CUI"])
        CUI_set = set(pq_table_contractors['CUI'].to_pylist())
    else:
        CUI_set = set()
    
    pq_table_contracts =  pq.read_table('contract_awards.parquet', columns = ["caNoticeId"])
    new_contractors_list = []
    new_contracts_list = []

    for id in pq_table_contracts['caNoticeId']:
        caNoticeId = id.as_py()
        contract_items = get_contract_info(str(caNoticeId))
        print(f"Now saving info for caNoticeId {caNoticeId}")
        for contract in contract_items:
            winnerCUI = clean_CUI(contract['winner']['fiscalNumber'])
            final_contract = {
                'caNoticeContractId': contract['caNoticeContractId'],
                'caNoticeId': contract['caNoticeId'],
                'contractTitle': contract['contractTitle'],
                'contractDate': convert_date(contract['contractDate']),
                'winnerCUI': winnerCUI,
                'contractValue': contract['defaultCurrencyContractValue']
            }
            new_contracts_list.append(final_contract)
            
            if(winnerCUI not in CUI_set):
                address = contract['winner']['address']
                new_contractor = {
                    'CUI': winnerCUI,
                    'officialName': address['officialName'],
                    'city': address['city'],
                    'country': address['country'],
                    'isSME': address['isSME']
                }
                CUI_set.add(winnerCUI)
                new_contractors_list.append(new_contractor)
        time.sleep(2)
    
    new_contracts_table = pa.Table.from_pylist(new_contracts_list)
    pq.write_table(new_contracts_table, 'contracts.parquet')
    new_contractors_table = pa.Table.from_pylist(new_contractors_list)
    pq.write_table(new_contractors_table, 'contractors.parquet')

    print(pq.read_schema('contract_awards.parquet'))
    print(pq.read_schema('contractors.parquet'))

if __name__ == "__main__":
    #send_req()
    get_contracts()