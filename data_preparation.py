import utils.functions as fc

ship_data_enriched = fc.create_ship_data_enriched()


AIS = fc.read_ais_all()
AIS_enriched = fc.enrich_AIS_data(
    AIS, ship_data_enriched
)
AIS_start_black = AIS_enriched[(AIS_enriched['region'] == "Black") & (AIS_enriched['start_date'] == "2019-04-01")]
AIS_start_suez = AIS_enriched[(AIS_enriched['region'] == "Suez") & (AIS_enriched['start_date'] == "2019-04-01")]
AIS_end_black = AIS_enriched[(AIS_enriched['region'] == "Black") & (AIS_enriched['start_date'] == "2022-04-01")]
AIS_end_suez = AIS_enriched[(AIS_enriched['region'] == "Suez") & (AIS_enriched['start_date'] == "2021-03-21")]


import pyarrow
from pyarrow import fs
import pyarrow as pa
import pyarrow.parquet as pq

s3 = fs.S3FileSystem(endpoint_override=fc.ENDPOINT)

def write_parquet_s3(df, path, s3):
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path, filesystem = s3)

write_parquet_s3(
    AIS_start_black, "projet-hackathon-un-2022/AIS/preprocessed/Black-2019-04-01.parquet", s3
)
write_parquet_s3(
    AIS_start_suez, "projet-hackathon-un-2022/AIS/preprocessed/Suez-2019-04-01.parquet", s3
)
write_parquet_s3(
    AIS_end_black, "projet-hackathon-un-2022/AIS/preprocessed/Black-2022-04-01.parquet", s3
)
write_parquet_s3(
    AIS_end_suez, "projet-hackathon-un-2022/AIS/preprocessed/Suez-2021-03-21.parquet", s3
)
