import matplotlib.pyplot as plt

import pandas as pd
import geopandas as gpd

from pywaffle import Waffle

import s3fs

PATH_SHIP_DATA = "IHS/ship_data.parquet"
PATH_SHIP_CODES = PATH_SHIP_DATA.replace("data","codes")
PATH_AIS_PARQUET = "AIS/ais_azov_black_20220401_20220408_full_traces.parquet"
BUCKET = "projet-hackathon-un-2022"
ENDPOINT = 'https://minio.lab.sspcloud.fr'
PATH_PORT = 'https://msi.nga.mil/api/publications/download?type=view&key=16920959/SFH00000/UpdatedPub150.csv'

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

    return (center, sw, ne)

def enrich_AIS_data(
    AIS,
    ship_data_enriched,
    left_on='mmsi',
    right_on="MaritimeMobileServiceIdentityMMSINumber"
):

    AIS_enriched = AIS.merge(
        ship_data_enriched,
        left_on = left_on,
        right_on = right_on
    )
    return AIS_enriched


def count_boats(
    df,
    unique_id = "mmsi",
    by = None,
    normalize = False
    ):
    if by is None:
        x = df.drop_duplicates(
            unique_id
            ).agg(
                {unique_id: "nunique"}
                )
    else:
        x = df.drop_duplicates(
            subset = unique_id
            ).value_counts(by)

    return x


def waffle_chart_zone(
    df,
    by=None,
    share_blocked = 0
):
    temp = count_boats(df, by=by).to_dict()

    fig = plt.figure(
        FigureClass=Waffle,
        rows=10,
        values=temp,
        columns=10, 
        icons='ship',
        legend={'loc': 'lower left', 'bbox_to_anchor': (0, -0.4), 'ncol': 2, 'framealpha': 0},
        font_size=12,
        icon_legend=True
    )

    return fig


def import_ports(path_port = PATH_PORT):
    
    ports = pd.read_csv(path_port)
    
    ports = gpd.GeoDataFrame(
        ports, geometry = gpd.points_from_xy(ports['Longitude'], ports['Latitude']),
        crs = 4326
    )

    return ports