import requests

# get detailed info for a contract award
def get_info_CANotice(id):
    headers = {'Content-Type': 'application/json;charset=UTF-8',
               'Referer': 'https://e-licitatie.ro/pub/notices/contract-notices/list/0/0'}
    url = 'http://e-licitatie.ro/api-pub/C_PUBLIC_CANotice/get/' + id
    r = requests.get(url, headers = headers)
    return r.json()

def get_contract_award_list(start_date, end_date):
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
    'startPublicationDate': start_date,
    'endPublicationDate': end_date
    }
    r = requests.post(url, headers = headers, json = body, timeout = 5)
    return r.json()['items']

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