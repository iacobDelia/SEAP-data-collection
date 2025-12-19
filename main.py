
import time
import pyarrow as pa
import pyarrow.parquet as pq
from utils import *
from seap_requests import *
import os

def save_contract_awards(start_date, end_date):
    ca_list = get_contract_award_list(start_date, end_date)
    print(len(ca_list))
    cnt = 0
    batch_size = 10
    final_list = []
    for item in ca_list:
        assignment_type = item.get('sysContractAssigmentType')
        if not assignment_type:
            assignment_type_id = 0
        else:
            assignment_type_id = assignment_type.get('id')
        # ignore framework agreements
        if assignment_type_id != 3:
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
                'caNoticeId': item.get('caNoticeId'),
                'noticeId': item.get('noticeId'),
                'sysNoticeTypeId': item.get('sysNoticeTypeId'),
                'sysProcedureState': item.get('sysProcedureState', {}).get('id'),
                'sysProcedureType': item.get('sysProcedureType', {}).get('id'),
                'contractTitle': item.get('contractTitle'),

                'sysAcquisitionContractType': item.get('sysAcquisitionContractType', {}).get('id'),
                'sysProcedureType': item.get('sysProcedureType', {}).get('id'),
                'sysContractAssigmentType': item.get('sysContractAssigmentType', {}).get('id'),
                'ronContractValue': item.get('ronContractValue'),
                'title': info_dict.get('title'),
                'totalAcquisitionValue': totalAcquisitionValue,
                'estimatedValue': sum(lot.get('estimatedValue', 0) for lot in lots if lot.get('estimatedValue') is not None),
                'mainCPVCode': mainCPVCode,
                # if at least one lot is EU funded, mark the whole CA as so
                'isEUFunded': any(lot.get('isEUFunded', False) for lot in lots),
                'entityId': info_dict.get('entityId'),

                'caPublicationDate': caPublicationDate,
                'publicationDate': publicationDate,

                'year': item.get('noticeStateDate')[:4],
            }
            final_list.append(final_item)
            
            if len(final_list) == batch_size:
                ca_table_ids = write_to_dataset(final_list, 'year', 'seap_dataset/contract_awards')
                final_list = []
                ca_table_ids = ca_table_ids['caNoticeId']
                save_contracts(ca_table_ids)

            time.sleep(1)

    ca_table_ids = write_to_dataset(final_list, 'year', 'seap_dataset/contract_awards')
    final_list = []
    ca_table_ids = ca_table_ids['caNoticeId']
    save_contracts(ca_table_ids)


def save_contracts(ca_table_ids):
    os.makedirs('seap_dataset/contractors', exist_ok=True)
    if os.path.exists('seap_dataset/contractors') and any(f.endswith('.parquet') for f in os.listdir('seap_dataset/contractors')):
        pq_table_contractors = pq.read_table('seap_dataset/contractors', columns = ["CUI"])
        CUI_set = set(pq_table_contractors['CUI'].to_pylist())
    else:
        CUI_set = set()
    
    new_contractors_list = []
    new_contracts_list = []

    for id in ca_table_ids:
        caNoticeId = id.as_py()
        contract_items = get_contract_info(str(caNoticeId))
        print(f"Now saving info for caNoticeId {caNoticeId}")
        for contract in contract_items:
            isIndividual = False
            address = contract['winner']['address']
            winnerCUI = clean_CUI(contract['winner']['fiscalNumber'])
            if winnerCUI == '':
                winnerCUI = f"I_{address['noticeEntityAddressId']}"
                isIndividual = True
            contractDate = convert_date(contract['contractDate'])
            final_contract = {
                'caNoticeContractId': contract.get('caNoticeContractId'),
                'caNoticeId': contract.get('caNoticeId'),
                'contractTitle': contract.get('contractTitle'),
                'contractDate': contractDate,
                'year': contractDate.year if contractDate else None,
                'winnerCUI': winnerCUI,
                'contractValue': contract.get('defaultCurrencyContractValue')
            }
            new_contracts_list.append(final_contract)
            
            if(winnerCUI not in CUI_set):
                if address.get('county') == None:
                    county = None
                else:
                    county = address.get('county', {}).get('text')
                new_contractor = {
                    'CUI': winnerCUI,
                    'isIndividual': isIndividual,
                    'officialName': address.get('officialName'),
                    'county': county,
                    'country': address.get('country'),
                    'isSME': address.get('isSME')
                }
                CUI_set.add(winnerCUI)
                new_contractors_list.append(new_contractor)
        time.sleep(1)
    
    write_to_dataset(new_contracts_list, 'year', 'seap_dataset/contracts')
    # save the contractors
    if new_contractors_list:
        table_new = pa.Table.from_pylist(new_contractors_list)
        # save with a name based on the current timestamp to make sure the file is unique
        ts = time.time_ns()
        file_path = f'seap_dataset/contractors/batch_{ts}.parquet'
        pq.write_table(table_new, file_path)

if __name__ == "__main__":
    save_contract_awards('2023-04-30', '2023-04-30')