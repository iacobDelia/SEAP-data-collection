
import time
import pyarrow as pa
import pyarrow.parquet as pq
from utils import *
from seap_requests import *
import os
import sys
import argparse
import datetime
from tqdm import tqdm

accumulated_notices = []
accumulated_authorities = []
accumulated_contracts = []
accumulated_contractors = []

authorityId_set = load_entity_ids('authorities', 'authorityId')
CUI_set = load_entity_ids('contractors', 'CUI')

interval = 0.5

def save_current_batch():
    global accumulated_notices, accumulated_contracts, accumulated_contractors, accumulated_authorities
    if accumulated_notices:
        write_to_dataset(accumulated_notices, 'year', 'seap_dataset/contract_awards')
    if accumulated_authorities:
        save_entities(accumulated_authorities, 'authorities')
    if accumulated_contracts:
        write_to_dataset(accumulated_contracts, 'year', 'seap_dataset/contracts')
    if accumulated_contractors:
        save_entities(accumulated_contractors, 'contractors')

    accumulated_authorities = []
    accumulated_notices = []
    accumulated_contractors = []
    accumulated_contracts = []
    
def save_contract_awards(date):
    date_str = date.strftime("%Y-%m-%d")
    global accumulated_notices, accumulated_authorities, authorityId_set

    ca_list = get_contract_award_list(date_str)
    pbar = tqdm(ca_list, desc=f" Saving awards and authorities for day: {date_str}", position = 1, leave=False)
    notice_ids = []
    for item in pbar:
        # sysContractAssigmentType may be null sometimes
        assignment_type_id = (item.get('sysContractAssigmentType') or {}).get('id', None)
        # ignore framework agreements
        if assignment_type_id != 3:
            info_dict = get_info_CANotice(str(item['caNoticeId']))

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

            notice_ids.append(item.get('caNoticeId', None))
            accumulated_notices.append(final_item)
            
            if(authority_address.get('entityId') not in authorityId_set):
                nuts_text = (authority_address.get('nutsCodeItem') or {}).get('text', "")
                _, _, county = nuts_text.partition(" ")
                new_authority = {
                    'authorityId': authority_address.get('entityId', None),
                    'officialName': authority_address.get('officialName', None),
                    'county':  county,
                    'country': authority_address.get('country', None)
                }
                accumulated_authorities.append(new_authority)
                authorityId_set.add(authority_address.get('entityId', None))
            time.sleep(interval)
    return notice_ids

def save_contracts(ca_table_ids, date):
    date_str = date.strftime("%Y-%m-%d")
    global CUI_set, accumulated_contracts, accumulated_contractors
    pbar = tqdm(ca_table_ids, desc=f" Saving contracts and contractors for day: {date_str}", position = 1, leave=False)

    for caNoticeId in pbar:
        #caNoticeId = id.as_py()
        contract_items = get_contracts_info(str(caNoticeId))
        #print(f"Now saving info for caNoticeId {caNoticeId}")
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
                'caNoticeContractId': contract.get('caNoticeContractId', None),
                'caNoticeId': contract.get('caNoticeId', None),
                'contractTitle': contract.get('contractTitle', None),
                'contractDate': contractDate,
                'winnerCUI': winnerCUI,
                'estimatedContractValue': detailed_contract.get('section524', {}).get('estimatedContractValue', None),
                'contractValue': contract.get('defaultCurrencyContractValue', None),
                'numberOfReceivedOffers':detailed_contract.get('section522', {}).get('numberOfReceivedOffers', None),
                'year': date.year,
            }
            accumulated_contracts.append(final_contract)
            
            if(winnerCUI not in CUI_set):
                new_contractor = {
                    'CUI': winnerCUI,
                    'isIndividual': isIndividual,
                    'officialName': address.get('officialName', None),
                    # county may be blank sometimes
                    'county': (address.get('county') or {}).get('text', None),
                    'country': address.get('country', None),
                    'isSME': address.get('isSME', None)
                }
                CUI_set.add(winnerCUI)
                accumulated_contractors.append(new_contractor)
        time.sleep(interval)
    
def get_data(start_date, end_date, batch_size):
    os.makedirs('seap_dataset/contract_awards', exist_ok=True)
    os.makedirs('seap_dataset/authorities', exist_ok=True)
    os.makedirs('seap_dataset/contracts', exist_ok=True)
    os.makedirs('seap_dataset/contractors', exist_ok=True)
    total_days = (end_date - start_date).days + 1
    dates = [start_date + datetime.timedelta(days=i) for i in range(total_days)]
    pbar = tqdm(dates, desc="Total progress", position=0, leave=False)

    for current_date in pbar:
        notice_ids = save_contract_awards(current_date)
        save_contracts(notice_ids, current_date)
        if len(accumulated_notices) > batch_size:
            save_current_batch()
    save_current_batch()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'date_start',
        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d'),
        help = "Beginning date, format: YYYY-MM-DD",
        metavar = "YYYY-MM-DD"
    )
    parser.add_argument(
        'date_end',
        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d'),
        help = "Ending date, format: YYYY-MM_DD",
        metavar = "YYYY-MM-DD"
    )
    parser.add_argument(
        'batch_size',
        type=int,
        help = "Batch size",
    )
    args = parser.parse_args()
    get_data(args.date_start, args.date_end, args.batch_size)
    print("All done! Hooray!")