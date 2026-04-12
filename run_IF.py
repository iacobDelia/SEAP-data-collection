import pyarrow.dataset as ds
import pyarrow.compute as pc
import pandas as pd
import os
from scipy.stats import entropy
from sklearn.ensemble import IsolationForest

def get_info_about_df(df):
    open_auctions_df = {}
    closed_auctions = {}
    print(f"Total number of entries: {len(df)}")

    open_auctions_df = df[
        (df['sysProcedureType'] == 'Licitatie deschisa')
        | (df['sysProcedureType'] == 'Procedura simplificata')]

    print(f"Number of entries that are simplified procedures/ open auctions: {len(open_auctions_df)}")

    closed_auctions = df[
        (df['sysProcedureType'] == 'Negociere fara publicare prealabila')]
    print(f"Number of entries without prior publication: {len(closed_auctions)}")

def write_parquet(df, filename="rez.parquet"):
    df.to_parquet(filename, engine='pyarrow', index=False)

def run_isolation_forest(df):
    X = df.copy().drop(columns = ['caNoticeId', 'sysProcedureType'])
    model = IsolationForest(contamination=0.01, random_state=42)
    df['anomaly_label'] = model.fit_predict(X)
    df['anomaly_score'] = model.decision_function(X)

    anomalies = df[df['anomaly_label'] == -1]
    print(f"There are {len(anomalies)} potential anomalies.")

if __name__ == "__main__":
    pd.set_option('display.max_columns', None)
    os.makedirs("seap_dataset/contract_awards_IF", exist_ok=True)

    df_ca = pd.read_parquet(f'seap_dataset/contract_awards_extra', engine = 'pyarrow')
    df_contracts = pd.read_parquet(f'seap_dataset/contracts', engine = 'pyarrow')
    df_lots = pd.read_parquet(f'seap_dataset/lots', engine = 'pyarrow')
    print("info in total")
    get_info_about_df(df_ca)

    df_ca['budgetDifference'] = df_ca['totalAcquisitionValue'].fillna(0) - df_ca['estimatedValue'].fillna(0)
    df_ca['budgetDiffPercentage'] = (df_ca['budgetDifference'] / df_ca['totalAcquisitionValue'].replace(0, 1)) * 100
    df_ca['publicationTime'] = df_ca['caPublicationDate'] - df_ca['publicationDate']
    
    # select only the columns we need
    df_final = df_ca[['caNoticeId', 'softwareModules',
                      'experts', 'projectDuration',
                      'totalAcquisitionValue', 'estimatedValue',
                      'budgetDifference', 'ronContractValue',
                      'budgetDiffPercentage', 'publicationTime',
                      'isEUFunded', 'sysProcedureType']]
    
    # the total number of received offers and winners per contract award
    contracts_agg = df_contracts.groupby('caNoticeId').agg({
            'numberOfReceivedOffers': 'sum',
            'numberOfWinners': 'sum',
                }).reset_index()
    # the total number of lots and the mean value per contract award
    lots_agg = df_lots.groupby('caNoticeId').agg(
        numberOfLots=('caNoticeId', 'size'),
        meanLotValue=('estimatedValue', 'mean'),
    ).reset_index()

    df_final = (
        df_final.merge(contracts_agg, on='caNoticeId', how='left')
        .merge(lots_agg, on='caNoticeId', how='left')
    )
    df_final['valuePerOffer'] = df_final['ronContractValue'].fillna(0) / df_final['numberOfReceivedOffers'].replace(0, 1)
    df_final['publicationTimeDays'] = df_final['publicationTime'].dt.total_seconds() / (24 * 3600)
    df_final = df_final.drop(columns=['publicationTime'])
    # handle missing values
    df_final['softwareModules'] = df_final['softwareModules'].fillna(0)
    df_final['experts'] = df_final['experts'].fillna(0)
    df_final['totalAcquisitionValue'] = df_final['totalAcquisitionValue'].fillna(0)
    df_final['meanLotValue'] = df_final['meanLotValue'].fillna(0)
    df_final['numberOfReceivedOffers'] = df_final['numberOfReceivedOffers'].fillna(0)
    df_final['numberOfWinners'] = df_final['numberOfWinners'].fillna(0)
    df_final['valuePerOffer'] = df_final['valuePerOffer'].fillna(0)
    df_final['isEUFunded'] = df_final['isEUFunded'].astype(int)
    median_duration = df_final['projectDuration'].median()
    df_final['projectDuration'] = df_final['projectDuration'].fillna(median_duration)


    # handle these two differently
    open_auctions_df = df_final[
        (df_final['sysProcedureType'] != 'Negociere fara publicare prealabila')].copy()

    closed_auctions_df = df_final[
        (df_final['sysProcedureType'] == 'Negociere fara publicare prealabila')].copy()

    run_isolation_forest(open_auctions_df)

    closed_auctions_df = closed_auctions_df.drop(columns=['softwareModules', 'experts',
                                                          'projectDuration', 'estimatedValue',
                                                          'budgetDifference', 'meanLotValue',
                                                          'valuePerOffer', 'publicationTimeDays',
                                                          'budgetDiffPercentage'])
    run_isolation_forest(closed_auctions_df)

    # merge results and new calculated columns with old data
    cols_to_add = df_ca.columns.difference(open_auctions_df.columns).tolist()
    cols_to_add.append('caNoticeId')

    open_auctions_complete_df = open_auctions_df.merge(
    df_ca[cols_to_add], 
    on='caNoticeId', 
    how='left'
    )

    write_parquet(open_auctions_complete_df, filename="seap_dataset/contract_awards_IF/open_auctions.parquet")

    cols_to_add = df_ca.columns.difference(closed_auctions_df.columns).tolist()
    cols_to_add.append('caNoticeId')

    closed_auctions_complete_df = closed_auctions_df.merge(
    df_ca[cols_to_add], 
    on='caNoticeId', 
    how='left'
    )

    write_parquet(closed_auctions_complete_df, filename="seap_dataset/contract_awards_IF/closed_auctions.parquet")
