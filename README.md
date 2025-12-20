



# SEAP data collection tool
Python tool for collecting contract data from the SEAP site.
## Usage
```
python main.py --help
usage: main.py [-h] YYYY-MM-DD YYYY-MM-DD batch_size

positional arguments:
  YYYY-MM-DD            Beginning date, format: YYYY-MM-DD
  YYYY-MM-DD            Ending date, format: YYYY-MM_DD
  batch_size            Batch size

options:
  -h, --help            show this help message and exit
```
Note that SEAP has a [limit for the number of API requests](https://e-licitatie.ro/pub/archive/news-feed/100002361) you can send. To avoid this, the program waits approximately 0.8 seconds between each request. This value can be changed in main.py... at your own risk :]
## Data format
The script saves the data in parquet files in batches. The approximate batch size can be chosen by the user using command line arguments.
File structure:
```
SEAP_DATASET
├───authorities
├───contractors
├───contracts
└───contract_awards
```
The data for contracts and contract_awards is further split into years because of its bigger amount.
## Features
### Contract awards
| Feature | Data Type | Description |
| :--- | :--- | :--- |
| **caNoticeId** | `int64` | Unique internal identifier |
| **noticeId** | `int64` | General identifier for the public notice |
| **sysNoticeTypeId** | `int64` | System code representing the category of the notice |
| **sysProcedureState** | `int64` | Numerical code indicating the current status of the procedure |
| **sysProcedureType** | `int64` | System code for the procurement method |
| **contractTitle** | `string` | The title given to the specific contract awarded |
| **sysAcquisitionContractType** | `int64` | Code for the nature of the contract |
| **sysContractAssigmentType** | `int64` | Code for the type of assignment |
| **ronContractValue** | `double` | The final value of the awarded contract |
| **title** | `string` | The title of the public notice |
| **totalAcquisitionValue** | `double` | The value of the entire acquisition process |
| **estimatedValue** | `double` | The initial estimated budget for the acquisition before bidding |
| **mainCPVCode** | `string` | CPV code |
| **isEUFunded** | `bool` | Whether the project is financed with EU funds |
| **authorityId** | `int64` | Foreign key referencing the unique ID of the Contracting Authority |
| **caPublicationDate** | `timestamp` | Date when the Contract Award Notice was officially published |
| **publicationDate** | `timestamp` | Date for when the Contract Award Notice was published on SEAP |
### Contracting Authorities
| Feature | Data Type | Description |
| :--- | :--- | :--- |
| **authorityId** | `int64` | Unique internal identifier |
| **officialName** | `string` | The full name of the organization |
| **county** | `string` | The administrative county (Judet) where the Contracing Authority is located |
| **country** | `string` | The country of residence |
### Contracts
| Feature | Data Type | Description |
| :--- | :--- | :--- |
| **caNoticeContractId** | `int64` | Unique internal identifier |
| **caNoticeId** | `int64` | Foreign key referencing the Contract Award Notice |
| **contractTitle** | `string` | The official name or title of the specific contract lot. |
| **contractDate** | `timestamp` | The date when the contract awarded |
| **winnerCUI** | `string` | CUI of the winner, or a placeholder for individuals; Foreign key for contractors |
| **estimatedContractValue** | `double` | The initial estimated value for this contract before bidding |
| **contractValue** | `double` | The total value agreed upon for the awarded contract |
| **numberOfReceivedOffers** | `int64` | The total number of bids received for this specific lot |
### Contractors
| Feature | Data Type | Description |
| :--- | :--- | :--- |
| **CUI** | `string` | Fiscal identification number for the contractor or a placeholder for individuals |
| **isIndividual** | `bool` | Whether the contractor is an individual or a company |
| **officialName** | `string` | The name of the winning company or individual |
| **county** | `string` | The administrative county (Judet) where the contractor is located |
| **country** | `string` | The country where the contractor is located |
| **isSME** | `bool` | Whether the contractor is a small or medium enterprise |

No personal information or contact info was collected.

## System ids
### sysNoticeTypeid
- 3: Anunt de atribuire la Anunt de participare (CN)
- 13: Anunt de atribuire la invitatia de participare (RFQ)
- 18: Anunt de atribuire la anunt simplificat (SCN)
- 16: Anunt de atribuire la concesionari (PC)
- 4: Anunt de rezultat la concursul de solutii (DC)
- 20: Anunt de atribuire la invitatia de depunere oferta la sistemul de achizitie dinamic (RFDA)

### sysProcedureState
- 2: In desfasurare
- 3: Anulata
- 5: Atribuita

### sysProcedureType
- 1: Licitatie deschisa
- 7: Negociere fara publicare prealabila
- 16: Procedura simplificata proprie
- 17: Norme proprii (Anexa 2)
- 20: Procedura simplificata

### sysAcquisitionContractType
- 1: Furnizare
- 2: Servicii
- 3: Lucrari

### sysContractAssigmentType
- 1: Contract de achizitii publice
- 3: Acord-cadru
- null

## Notes
- There is also a field ```noticeAwardCriteriaList``` that the script doesn't save. It contains the weights of the reasons for choosing a bidder. For example:
    - Price: 60%
    - Warranty: 20%
    - Quality: 20%

- There are instances of an association of companies winning a single contract, the script only saves the leader

- Contractors that show up as individuals don't have a CUI. For them, I used ```I_noticeEntityAddressId``` as a fallback primary key, and added an ```isIndividual``` column
