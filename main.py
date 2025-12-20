
import time
import utils
import seap_requests
import os
import argparse
import datetime
from tqdm import tqdm

accumulated_notices = []
accumulated_authorities = []
accumulated_contracts = []
accumulated_contractors = []

# use sets for looking up existing ids, it's faster
authorityId_set = utils.load_entity_ids('authorities', 'authorityId')
CUI_set = utils.load_entity_ids('contractors', 'CUI')

# interval of time between API requests
interval = 0.8

def save_current_batch():
    global accumulated_notices, accumulated_contracts, accumulated_contractors, accumulated_authorities
    if accumulated_notices:
        utils.write_to_dataset(accumulated_notices, 'year', 'seap_dataset/contract_awards')
    if accumulated_authorities:
        utils.save_entities(accumulated_authorities, 'authorities')
    if accumulated_contracts:
        utils.write_to_dataset(accumulated_contracts, 'year', 'seap_dataset/contracts')
    if accumulated_contractors:
        utils.save_entities(accumulated_contractors, 'contractors')

    # reset the lists to free up space in memory
    accumulated_authorities = []
    accumulated_notices = []
    accumulated_contractors = []
    accumulated_contracts = []
    
def process_CA_and_authorities(date):
    date_str = date.strftime("%Y-%m-%d")
    global accumulated_notices, accumulated_authorities, authorityId_set

    ca_list = seap_requests.get_contract_award_list(date_str)
    pbar = tqdm(ca_list, desc=f" Saving awards and authorities for day: {date_str}", position = 1, leave=False)
    notice_ids = []
    for item in pbar:
        try:
            # sysContractAssigmentType may be null sometimes
            assignment_type_id = (item.get('sysContractAssigmentType') or {}).get('id', None)
            # ignore framework agreements
            if assignment_type_id != 3:
                info_dict = seap_requests.get_info_CANotice(str(item['caNoticeId']))
                final_item = utils.get_notice_entry(item, date, info_dict)

                notice_ids.append(item.get('caNoticeId', None))
                accumulated_notices.append(final_item)
                
                # get the entity id from the json
                suffix = "_U" if info_dict.get('caNoticeEdit_New') is None else ""
                root = info_dict.get(f'caNoticeEdit_New{suffix}')
                authority_address = root.get(f'section1_New{suffix}').get('section1_1', {}).get('caAddress', {})

                if(authority_address.get('entityId') not in authorityId_set):
                    new_authority = utils.get_authority_entry(info_dict)
                    accumulated_authorities.append(new_authority)
                    authorityId_set.add(authority_address.get('entityId', None))
                time.sleep(interval)
        except Exception as e:
            print(f"Error for notice {item.get('caNoticeId')} caused by exception {e}\n Skipping this item.")
            continue
    return notice_ids

def process_contracts_and_contractors(ca_table_ids, date):
    date_str = date.strftime("%Y-%m-%d")
    global CUI_set, accumulated_contracts, accumulated_contractors
    pbar = tqdm(ca_table_ids, desc=f" Saving contracts and contractors for day: {date_str}", position = 1, leave=False)

    for caNoticeId in pbar:
        try:
            contract_items = seap_requests.get_contracts_info(str(caNoticeId))
            for contract in contract_items:
                isIndividual = False
                address = contract.get('winner', {}).get('address', {})

                winnerCUI = utils.clean_CUI(contract.get('winner', {}).get('fiscalNumber', ""))
                # if the contractor is an individual the CUI will be blank, use I_{noticeEntityAddressId} as a placeholder
                if winnerCUI == '':
                    winnerCUI = f"I_{address.get('noticeEntityAddressId', "")}"
                    isIndividual = True

                detailed_contract = seap_requests.get_contract_details(str(contract.get('caNoticeContractId')))
                
                # generate another contract entry and append it to the list
                final_contract = utils.get_contract_entry(date, contract, winnerCUI, detailed_contract)
                accumulated_contracts.append(final_contract)
                
                # generate another contractor entry and append it to the list
                if(winnerCUI not in CUI_set):
                    new_contractor = utils.get_contractor_entry(winnerCUI,address, isIndividual)
                    # append its CUI to the set
                    CUI_set.add(winnerCUI)
                    accumulated_contractors.append(new_contractor)
            time.sleep(interval * 2)
        except Exception as e:
            print(f"Error for contract with the notice {caNoticeId} caused by exception {e}\n Skipping this item.")
            continue
        
    
def get_data(start_date, end_date, batch_size):
    os.makedirs('seap_dataset/contract_awards', exist_ok=True)
    os.makedirs('seap_dataset/authorities', exist_ok=True)
    os.makedirs('seap_dataset/contracts', exist_ok=True)
    os.makedirs('seap_dataset/contractors', exist_ok=True)
    total_days = (end_date - start_date).days + 1
    dates = [start_date + datetime.timedelta(days=i) for i in range(total_days)]
    pbar = tqdm(dates, desc="Total progress", position=0, leave=False)

    for current_date in pbar:
        try:
            notice_ids = process_CA_and_authorities(current_date)
            process_contracts_and_contractors(notice_ids, current_date)
            if len(accumulated_notices) > batch_size:
                save_current_batch()
        except Exception as e:
            print(f"Error for day {current_date}, caused by exception {e}\n Skipping day")
            continue
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