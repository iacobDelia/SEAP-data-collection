
import time
import pyarrow as pa
import pyarrow.parquet as pq
from utils import *
from seap_requests import *
import os
import sys

def save_contract_awards(start_date, end_date):
    os.makedirs('seap_dataset/authorities', exist_ok=True)
    if os.path.exists('seap_dataset/authorities') and any(f.endswith('.parquet') for f in os.listdir('seap_dataset/authorities')):
        pq_table_authorities = pq.read_table('seap_dataset/authorities', columns = ["authorityId"])
        authorityId_set = set(pq_table_authorities['authorityId'].to_pylist())
    else:
        authorityId_set = set()

    ca_list = get_contract_award_list(start_date, end_date)
    print(len(ca_list))
    cnt = 0
    batch_size = 20
    final_list = []
    new_authority_list = []
    for item in ca_list:
        # sysContractAssigmentType may be null sometimes
        assignment_type_id = (item.get('sysContractAssigmentType') or {}).get('id', 0)
        # ignore framework agreements
        if assignment_type_id != 3:
            print(item['caNoticeId'])
            info_dict = get_info_CANotice(str(item['caNoticeId']))

            # these sections have a suffix for utility acquisitions    
            suffix = "_U" if info_dict.get('caNoticeEdit_New') is None else ""
            root = info_dict.get(f'caNoticeEdit_New{suffix}')
            section2 = root.get(f'section2_New{suffix}')

            lots = section2.get(f'section2_2_New{suffix}').get('descriptionList')
            totalAcquisitionValue = section2.get(f'section2_1_New{suffix}', {}).get('totalAcquisitionValue', 0)
            mainCPVCode = section2.get(f'section2_1_New{suffix}', {}).get('mainCPVCode', {}).get('localeKey', None)
            caPublicationDate = convert_date(root.get('publicationDetailsModel', {}).get('caPublicationDate', None))
            publicationDate = convert_date(root.get('publicationDetailsModel').get('publicationDate', None))

            authority_address = root.get(f'section1_New{suffix}').get('section1_1', {}).get('caAddress', {})
            #print(f'auth addr: {authority_address}')
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

                'ronContractValue': item.get('ronContractValue', 0),
                'title': info_dict.get('title', None),
                'totalAcquisitionValue': totalAcquisitionValue,
                'estimatedValue': sum(lot.get('estimatedValue', 0) for lot in lots if lot.get('estimatedValue') is not None),
                'mainCPVCode': mainCPVCode,
                # if at least one lot is EU funded, mark the whole CA as so
                'isEUFunded': any(lot.get('isEUFunded', False) for lot in lots),
                'authorityID': authority_address.get('entityId', None),

                'caPublicationDate': caPublicationDate,
                'publicationDate': publicationDate,

                'year': int(item.get('noticeStateDate')[:4]) if item.get('noticeStateDate') else None,
            }
            final_list.append(final_item)
            
            if(authority_address.get('entityId') not in authorityId_set):
                nuts_text = (authority_address.get('nutsCodeItem') or {}).get('text', "")
                _, _, county = nuts_text.partition(" ")
                new_authority = {
                    'authorityId': authority_address.get('entityId', None),
                    'officialName': authority_address.get('officialName', None),
                    'county':  county,
                    'country': authority_address.get('country', None)
                }
                new_authority_list.append(new_authority)
                authorityId_set.add(authority_address.get('entityId', None))

            if len(final_list) == batch_size:
                ca_table_ids = write_to_dataset(final_list, 'year', 'seap_dataset/contract_awards')
                write_to_dataset(final_list, 'year', 'seap_dataset/contract_awards')
                save_entities(new_authority_list, 'authorities')
                new_authority_list = []
                final_list = []
                ca_table_ids = ca_table_ids['caNoticeId']
                save_contracts(ca_table_ids)



            time.sleep(1)

    ca_table_ids = write_to_dataset(final_list, 'year', 'seap_dataset/contract_awards')
    save_entities(new_authority_list, 'authorities')
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
        contract_items = get_contracts_info(str(caNoticeId))
        print(f"Now saving info for caNoticeId {caNoticeId}")

        for contract in contract_items:

            isIndividual = False
            address = contract['winner']['address']
            # save the CUI
            winnerCUI = clean_CUI(contract['winner']['fiscalNumber'])
            # if the contractor is an individual the CUI will be blank
            if winnerCUI == '':
                winnerCUI = f"I_{address['noticeEntityAddressId']}"
                isIndividual = True
            contractDate = convert_date(contract['contractDate'])

            detailed_contract = get_contract_details(str(contract.get('caNoticeContractId')))
            
            final_contract = {
                'caNoticeContractId': contract.get('caNoticeContractId'),
                'caNoticeId': contract.get('caNoticeId'),
                'contractTitle': contract.get('contractTitle'),
                'contractDate': contractDate,
                'year': int(contractDate.year) if contractDate else None,
                'winnerCUI': winnerCUI,
                'estimatedContractValue': detailed_contract.get('section524').get('estimatedContractValue'),
                'contractValue': contract.get('defaultCurrencyContractValue'),
                'numberOfReceivedOffers':detailed_contract.get('section522').get('numberOfReceivedOffers')
            }
            new_contracts_list.append(final_contract)
            
            if(winnerCUI not in CUI_set):
                new_contractor = {
                    'CUI': winnerCUI,
                    'isIndividual': isIndividual,
                    'officialName': address.get('officialName'),
                    # county may be blank sometimes
                    'county': (address.get('county') or {}).get('text'),
                    'country': address.get('country'),
                    'isSME': address.get('isSME')
                }
                CUI_set.add(winnerCUI)
                new_contractors_list.append(new_contractor)
        time.sleep(1)
    
    write_to_dataset(new_contracts_list, 'year', 'seap_dataset/contracts')
    # save the contractors
    save_entities(new_contractors_list, 'contractors')

def get_data(start_date_string, end_date_string):
    current_date = datetime.strptime(start_date_string, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_string, "%Y-%m-%d")
    delta = datetime.timedelta(days=1)
    while current_date <= end_date:
        save_contract_awards(current_date.strftime("%Y-%m-%d"), current_date.strftime("%Y-%m-%d"))
        current_date += delta

if __name__ == "__main__":
    save_contract_awards('2023-04-30', '2023-04-30')