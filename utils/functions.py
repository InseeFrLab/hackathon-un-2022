import pandas as pd
import geopandas as gpd

import s3fs

PATH_SHIP_DATA = "IHS/ship_data.parquet"
PATH_SHIP_CODES = PATH_SHIP_DATA.replace("data","codes")
PATH_AIS_PARQUET = "AIS/ais_azov_black_20220001_20220007.parquet"
BUCKET = "projet-hackathon-un-2022"
ENDPOINT = 'https://minio.lab.sspcloud.fr'


def create_s3_fs(endpoint=ENDPOINT):
    fs = s3fs.S3FileSystem(
    client_kwargs={'endpoint_url': endpoint}
    )
    return fs



def create_ship_data_enriched(
    fs = None,
    bucket=BUCKET,
    path_ship_data = PATH_SHIP_DATA,
    path_ship_codes = PATH_SHIP_CODES,
    endpoint=ENDPOINT):

    if fs is None:
        fs = create_s3_fs(endpoint=ENDPOINT)

    ship_data = pd.read_parquet(
        fs.open(f'{bucket}/{path_ship_data}',
                            mode='rb'))

    ship_codes = pd.read_parquet(
        fs.open(f'{bucket}/{path_ship_codes}'.replace("data", "codes"),
                            mode='rb')
                    )

    ship_data_enriched = ship_data.merge(
        ship_codes,
        on="StatCode5"
        )

    return ship_data_enriched


def read_ais_parquet(
    fs=None,
    bucket=BUCKET,
    path_parquet=PATH_AIS_PARQUET,
    endpoint=ENDPOINT
    ):

    if fs is None:
        fs = create_s3_fs(endpoint=ENDPOINT)

    ais_data = pd.read_parquet(
        fs.open(f'{bucket}/{path_parquet}',
        mode='rb')
        )

    return ais_data


def bbox_geopandas(
    df: gpd.GeoDataFrame,
    latitude_var: str = "latitude",
    longitude_var: str = "longitude"
):

    center = df[[latitude_var, longitude_var]].mean().values.tolist()
    sw = df[[latitude_var, longitude_var]].min().values.tolist()
    ne = df[[latitude_var, longitude_var]].max().values.tolist()

    return {"center": center, "sw": sw, "ne": ne}

def enrich_AIS_data(
    AIS,
    ship_data_enriched,
    left_on='mmsi',
    right_on="MaritimeMobileServiceIdentityMMSINumber"
):

    AIS_enriched = AIS.merge(
        ship_data_enriched, left_on, right_on
    )
    return AIS_enriched