
import time
import utils
import seap_requests
import os
import argparse
import datetime
from tqdm import tqdm
from itertools import pairwise

accumulated_notices = []
accumulated_authorities = []
accumulated_contracts = []
accumulated_contractors = []
accumulated_lots = []
accumulated_contract_winners = []

# use sets for looking up existing ids, it's faster
authorityId_set = utils.load_entity_ids('authorities', 'authorityId')
CUI_set = utils.load_entity_ids('contractors', 'CUI')
lots_map = {}
# interval of time between API requests
interval = 0.8

def save_current_batch(start_date, final_date):
    global accumulated_notices, accumulated_contracts, accumulated_contractors, accumulated_authorities, accumulated_lots, accumulated_contract_winners
    if accumulated_notices:
        utils.save_entities(accumulated_notices, 'contract_awards', start_date, final_date)
    if accumulated_authorities:
        utils.save_entities(accumulated_authorities, 'authorities', start_date, final_date)
    if accumulated_contracts:
        utils.save_entities(accumulated_contracts, 'contracts', start_date, final_date)
    if accumulated_contractors:
        utils.save_entities(accumulated_contractors, 'contractors', start_date, final_date)
    if accumulated_lots:
        utils.save_entities(accumulated_lots, 'lots', start_date, final_date)
    if accumulated_contract_winners:
        utils.save_entities(accumulated_contract_winners, 'contract_winners', start_date, final_date)

    # reset the lists to free up space in memory
    accumulated_authorities = []
    accumulated_notices = []
    accumulated_contractors = []
    accumulated_contracts = []
    accumulated_lots = []
    accumulated_contract_winners = []
    lots_map = {}
    
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
                final_item = utils.get_notice_entry(item, info_dict)

                notice_ids.append(item.get('caNoticeId', None))
                accumulated_notices.append(final_item)
                
                # get the entity id from the json
                suffix = "_U" if info_dict.get('caNoticeEdit_New') is None else ""
                root = info_dict.get(f'caNoticeEdit_New{suffix}')
                authority_address = root.get(f'section1_New{suffix}').get('section1_1', {}).get('caAddress', {})

                section2 = root.get(f'section2_New{suffix}')
                lots = section2.get(f'section2_2_New{suffix}', {}).get('descriptionList', [])

                for lot in lots:
                    new_lot = utils.get_lots_entry(lot, item.get('caNoticeId', None))
                    accumulated_lots.append(new_lot)
                    crt_id = new_lot.get('lotId')
                    if crt_id:
                        lots_map[crt_id] = new_lot

                if(authority_address.get('entityId') not in authorityId_set):
                    new_authority = utils.get_authority_entry(info_dict)
                    accumulated_authorities.append(new_authority)
                    authorityId_set.add(authority_address.get('entityId', None))
                time.sleep(interval)
        except Exception as e:
            tqdm.write(f"Error for notice {item.get('caNoticeId')} caused by exception {e}\n Skipping this item.")
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
                winner_data = (contract.get('winner') or {})

                detailed_contract = seap_requests.get_contract_details(str(contract.get('caNoticeContractId')))
                
                # generate another contract entry and append it to the list
                final_contract = utils.get_contract_entry(contract, detailed_contract)
                accumulated_contracts.append(final_contract)
                
                # get info for contractors
                contractors_address_list = detailed_contract.get('section523').get('nameAndAddresses')
                for contractor_address in contractors_address_list:
                    CUI = utils.clean_CUI(contractor_address.get('nationalIDNumber', ''))
                    # if the contractor is an individual the CUI will be blank, use I_{noticeEntityAddressId} as a placeholder
                    if CUI == '':
                        CUI = f"I_{contractor_address.get('noticeEntityAddressId', "")}"
                        isIndividual = True

                    # generate another contractor entry and append it to the list
                    if CUI not in CUI_set:
                        new_contractor = utils.get_contractor_entry(CUI, contractor_address, isIndividual)
                        # append its CUI to the set
                        CUI_set.add(CUI)
                        accumulated_contractors.append(new_contractor)
                        contract_winner_entry = {
                            'CUI': CUI,
                            'caNoticeContractId': str(contract.get('caNoticeContractId'))
                        }
                        accumulated_contract_winners.append(contract_winner_entry)

                # add the contract id to the lot
                for lot_item in detailed_contract.get('contractLotList', []):
                    lot_id = lot_item.get('lotId', None)
                    if lot_id and lot_id in lots_map:
                        lots_map[lot_id]['caNoticeContractId'] = contract.get('caNoticeContractId', None)

            time.sleep(interval * 2)
        except Exception as e:
            tqdm.write(f"Error for contract with the notice {caNoticeId} caused by exception {e}\n Skipping this item.")
            continue
        

def get_data(start_date, end_date, batch_size):
    os.makedirs('seap_dataset/contract_awards', exist_ok=True)
    os.makedirs('seap_dataset/authorities', exist_ok=True)
    os.makedirs('seap_dataset/contracts', exist_ok=True)
    os.makedirs('seap_dataset/contractors', exist_ok=True)
    os.makedirs('seap_dataset/lots', exist_ok=True)
    os.makedirs('seap_dataset/contract_winners', exist_ok=True)
    total_days = (end_date - start_date).days + 1

    # for the given period, go through each day
    dates = [start_date + datetime.timedelta(days=i) for i in range(total_days)]
    pbar = tqdm(dates, desc="Total progress", position=0, leave=False)
    start_date = dates[0]

    for current_date, next_date in pairwise(pbar):
        try:
            notice_ids = process_CA_and_authorities(current_date)
            process_contracts_and_contractors(notice_ids, current_date)

            if len(accumulated_notices) > batch_size:
                save_current_batch(start_date, current_date)
                start_date = next_date

        except Exception as e:
            tqdm.write(f"Error for day {current_date}, caused by exception {e}\n Skipping day")
            continue

    save_current_batch(start_date, current_date)
    utils.merge_everything()

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