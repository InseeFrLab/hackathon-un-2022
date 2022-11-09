import pandas as pd
import utils.functions as fc


BUCKET = "projet-hackathon-un-2022/open-data/FAO/trade_matrix_fao/"

fs = fc.create_s3_fs()

def import_fao_matrix():
    path2data = BUCKET + "Trade_DetailedTradeMatrix_E_All_Data_(Normalized).csv"
    with fs.open(path2data, mode="rb") as file_in:
        fao = pd.read_csv(file_in, error_bad_lines=False, encoding='iso-8859-1')
    return fao