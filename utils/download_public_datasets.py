import zipfile
from cartiflette.utils import download_pb

url="https://fenixservices.fao.org/faostat/static/bulkdownloads/Trade_DetailedTradeMatrix_E_All_Data_(Normalized).zip"
download_pb(url, "trade_matrix_fao.zip")

with zipfile.ZipFile("trade_matrix_fao.zip", 'r') as zip_ref:
    zip_ref.extractall("trade_matrix_fao")

#mc cp trade_matrix_fao s3/projet-hackathon-un-2022/open-data/FAO/ --recursive
#rm -rf trade_matrix_fao