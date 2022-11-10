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


# RANDOM BOATS FOR THE FIRST MAP -------------------------

boat_position_black_start = pd.DataFrame(fc.random_sample_position(AIS_start_black)).loc[:, ['mmsi', 'longitude', 'latitude']]
boat_position_suez_end = pd.DataFrame(fc.random_sample_position(AIS_start_suez)).loc[:, ['mmsi', 'longitude', 'latitude']]
boat_position_black_start = pd.DataFrame(fc.random_sample_position(AIS_end_black)).loc[:, ['mmsi', 'longitude', 'latitude']]
boat_position_suez_end = pd.DataFrame(fc.random_sample_position(AIS_end_suez)).loc[:, ['mmsi', 'longitude', 'latitude']]

write_parquet_s3(
    boat_position_black_start, "projet-hackathon-un-2022/AIS/preprocessed/boat_Black-2019-04-01.parquet", s3
)
write_parquet_s3(
    boat_position_suez_end, "projet-hackathon-un-2022/AIS/preprocessed/boat_Suez-2019-04-01.parquet", s3
)
write_parquet_s3(
    boat_position_black_start, "projet-hackathon-un-2022/AIS/preprocessed/boat_Black-2022-04-01.parquet", s3
)
write_parquet_s3(
    boat_position_suez_end, "projet-hackathon-un-2022/AIS/preprocessed/boat_Suez-2021-03-21.parquet", s3
)

# WAFFLE CHART -------------------------

AIS_enriched = AIS_start_black.copy()


def create_waffle(region_name = "Black", start_date = "2019-04-01"):
    AIS_subset = AIS_enriched.loc[(AIS_enriched['region'] == region_name) & (AIS_enriched['start_date'] == start_date)]
    fc.waffle_chart_zone(AIS_subset, by="ShiptypeLevel1")
    plt.savefig(f"waffle-{region_name}-{start_date}.png", format="png") # save to the above file object
    plt.close()
    s3_path = f"projet-hackathon-un-2022/output/waffle-{region_name}-{start_date}.png"
    fs.put(f"waffle-{region_name}-{start_date}.png", s3_path)


fs = fc.create_s3_fs()

create_waffle(region_name = "Black", start_date = "2019-04-01")
create_waffle(region_name = "Black", start_date = "2022-04-01")
create_waffle(region_name = "Suez", start_date = "2019-04-01")
create_waffle(region_name = "Suez", start_date = "2021-03-21")


def count_boats(region_name = "Black", start_date = "2019-04-01"):
    AIS_subset = AIS_enriched.loc[(AIS_enriched['region'] == region_name) & (AIS_enriched['start_date'] == start_date)]
    nb_boats = int(
            fc.count_boats(AIS_enriched, unique_id = "mmsi")
    )
    return nb_boats

import pandas as pd

[
    pd.DataFrame(
        {"region": "Black", "date": "2019-04-01", "count": count_boats(
            region_name = "Black",
            start_date = "2019-04-01")
        }, index=[0]
    ),
    
]


region = "Black"
date = "2019-04-03"

def share_international_trade(region, date):
    df_circulation = pd.read_csv("output/ais_ship_number_percentage.csv")
    df_circulation.loc[df_circulation["area"] == "azov_black", 'area'] = "Black"
    df_circulation.loc[df_circulation["area"] == "suez", 'area'] = "Suez"
    df_circulation.loc[df_circulation["date"] == "2019-04-03", 'date'] = "2019-04-01"
    df_circulation.loc[df_circulation["date"] == "2022-04-03", 'date'] = "2022-04-01"
    df_circulation.loc[df_circulation["date"] == "2021-03-26", 'date'] = "2021-03-21"

    p1 = float(df_circulation.loc[
        (df_circulation["area"] == region) & 
        (df_circulation["date"] == date), "ship_number"
        ])
    p2 = float(df_circulation.loc[
        (df_circulation["area"] == 'all') & 
        (df_circulation["date"] == date), "ship_number"
        ])
    p = p1/p2
    return f'During this period, this area represents {p:.1%} of ships circulation'