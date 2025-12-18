
import requests
import time
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from zoneinfo import ZoneInfo

# get detailed info for a notice
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
    pq.write_table(data_dict_table, 'temp.parquet')

    print(pq.read_schema('temp.parquet'))

if __name__ == "__main__":
    send_req()