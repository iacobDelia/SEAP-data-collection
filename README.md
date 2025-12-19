



## Useful
SysNoticeTypeids:
- 3:  Anunt de atribuire la Anunt de participare (CN) 
- 13:  Anunt de atribuire la invitatia de participare (RFQ)
- 18:  Anunt de atribuire la anunt simplificat (SCN) 
- 16:  Anunt de atribuire la concesionari (PC)
- 4: Anunt de rezultat la concursul de solutii (DC) 
- 20:  Anunt de atribuire la invitatia de depunere oferta la sistemul de achizitie dinamic (RFDA) 

## Notes
- There is also a field ```noticeAwardCriteriaList``` that the script doesn't save. It contains the weights of the reasons for choosing a bidder. For example:
    - Price: 60%
    - Warranty: 20%
    - Quality: 20%

- There are instances of an association of companies winning a single contract, the script only saves the leader

