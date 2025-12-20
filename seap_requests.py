import requests

# get detailed info for a contract award
def get_info_CANotice(id):
    headers = {'Content-Type': 'application/json;charset=UTF-8',
               'Referer': 'https://e-licitatie.ro/pub/notices/contract-notices/list/0/0'}
    url = 'http://e-licitatie.ro/api-pub/C_PUBLIC_CANotice/get/' + id
    r = requests.get(url, headers = headers, timeout = 5)
    return r.json()

def get_contract_details(id):
    headers = {'Content-Type': 'application/json;charset=UTF-8',
               'Referer': 'https://e-licitatie.ro/pub/notices/contract-notices/list/0/0'}
    url = 'https://www.e-licitatie.ro/api-pub/C_PUBLIC_CANotice/GetContractView/?contractId=' + id
    r = requests.get(url, headers = headers, timeout = 5)
    return r.json()


def get_contract_award_list(date):
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
    'startPublicationDate': date,
    'endPublicationDate': date
    }
    r = requests.post(url, headers = headers, json = body, timeout = 5)
    return r.json()['items']

def get_contracts_info(caNoticeId):
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