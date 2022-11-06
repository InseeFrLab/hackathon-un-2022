import zipfile
from cartiflette.utils import download_pb


# FAO MATRIX FLOWS ------------------------

url="https://fenixservices.fao.org/faostat/static/bulkdownloads/Trade_DetailedTradeMatrix_E_All_Data_(Normalized).zip"
download_pb(url, "trade_matrix_fao.zip")

with zipfile.ZipFile("trade_matrix_fao.zip", 'r') as zip_ref:
    zip_ref.extractall("trade_matrix_fao")

#mc cp trade_matrix_fao s3/projet-hackathon-un-2022/open-data/FAO/ --recursive
#rm -rf trade_matrix_fao

# PORT FLOWS FROM WORLD BANK -------------------

# mc cp s3/projet-hackathon-un-2022/open-data/world_bank_ports --recursive

# PORT LOCATIONS

url="https://geonode.wfp.org/geoserver/wfs?srsName=EPSG%3A4326&typename=geonode%3Awld_trs_ports_wfp&outputFormat=json&version=1.0.0&service=WFS&request=GetFeature"
download_pb(url, "port.json")
ports = gpd.read_file("port.json")

# mc cp port.json s3/projet-hackathon-un-2022/open-data/wfp_ports/ports.json