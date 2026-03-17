import pyarrow.dataset as ds
import pyarrow.compute as pc
import pandas as pd
if __name__ == "__main__":
    c_awards_df = pd.read_parquet('seap_dataset/contract_awards', engine = 'pyarrow')

    print(f"Total number of entries: {len(c_awards_df)}")
    non_zero_cNoticeId = c_awards_df['cNoticeId'].count()
    print(f"Number of entries with cNoticeId (Licitatie deschisa): {non_zero_cNoticeId}")

    contracts_df = pd.read_parquet('seap_dataset/contracts', engine = 'pyarrow')

    print(f"Number of contracts: {len(contracts_df)}")