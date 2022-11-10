from datetime import datetime, timedelta
import matplotlib.pyplot as plt


import numpy as np
import pandas as pd
import geopandas as gpd


from pywaffle import Waffle

import plotly.express as px

import s3fs

PATH_SHIP_DATA = "IHS/ship_data.parquet"
PATH_SHIP_CODES = PATH_SHIP_DATA.replace("data","codes")
PATH_AIS_PARQUET = "AIS/ais_azov_black_20220401_20220408_full_traces.parquet"
PATH_AIS_PARQUET = "AIS/|region|_|date_start|_|date_end|_full_traces_before.parquet"

path = PATH_AIS_PARQUET

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


def read_ais_all(
    fs=None,
    bucket=BUCKET,
    path_parquet=PATH_AIS_PARQUET,
    endpoint=ENDPOINT    
):
    df_black_2019 = read_ais_parquet(
            region = "ais_azov_black",
            start_date = "2019-04-01"
            )
    df_black_2022 = read_ais_parquet(
            region = "ais_azov_black",
            start_date = "2022-04-01"
            )
    df_suez_2019 = read_ais_parquet(
            region = "ais_suez",
            start_date = "2019-04-01"
            )
    df_suez_2021 = read_ais_parquet(
            region = "ais_suez",
            start_date = "2021-03-21",
            end_date = "2021-04-01"
            )
    data = pd.concat(
        [
            df_black_2019, df_black_2022,
            df_suez_2019, df_suez_2021
        ]
    )
    return data



def read_ais_parquet(
    fs=None,
    bucket=BUCKET,
    path_parquet=PATH_AIS_PARQUET,
    endpoint=ENDPOINT,
    region = "ais_azov_black",
    start_date = "2019-04-01",
    end_date = None
    ):

    if fs is None:
        fs = create_s3_fs(endpoint=ENDPOINT)

    if end_date is None:
        end_date = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=7)
        date_end = end_date.strftime('%Y%m%d')
    else:
        date_end = end_date.replace("-", "")
    
    date_start = start_date.replace("-", "")
    path_parquet = path_parquet.replace("|date_start|", date_start)
    path_parquet = path_parquet.replace("|date_end|", date_end)
    path_parquet = path_parquet.replace("|region|", region)

    ais_data = pd.read_parquet(
        fs.open(f'{bucket}/{path_parquet}',
        mode='rb')
        )

    ais_data['region'] = region.rsplit("_", maxsplit = 1)[-1].capitalize()
    ais_data["start_date"] = start_date

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

def count_boats_blocked(data, share_not_usable = 0.3, by='ShiptypeLevel1'):

    temp = count_boats(data, by=by).reset_index()
    total = temp[0].sum()
    temp[0] = temp[0]*(1-share_not_usable)
    temp = pd.concat(
        [temp, pd.DataFrame(["Ships blocked"], columns=[by])]
    ).reset_index(drop=True)

    temp.loc[
        temp[temp.columns[0]] == "Ships blocked", 0
        ] = total*share_not_usable

    temp = temp.loc[
        temp[0]/temp[0].sum() > 0.01
        ]

    temp = temp.set_index(by)

    return temp

def waffle_chart_zone(
    df,
    by=None,
    share_blocked = None
):
    if share_blocked is None:
        temp = count_boats(df, by=by).to_dict()
        icons = "ship"
    else:
        temp = count_boats_blocked(
            df,
            share_not_usable=share_blocked,
            by=by
            )
        classes = temp.index.nunique()
        temp = temp.to_dict()[0]
        icons  = ['ship']*(classes - 1) + ['anchor']


    fig = plt.figure(
        FigureClass=Waffle,
        rows=10,
        values=temp,
        columns=10, 
        icons=icons,
        legend={
            'loc': 'lower left',
            'bbox_to_anchor': (0, -0.4),
            'ncol': 2, 'framealpha': 0},
        font_size=12,
        icon_legend=True
    )

    return fig


def import_ports(path_port=PATH_PORT):
    
    ports = pd.read_csv(path_port)
    
    ports = gpd.GeoDataFrame(
        ports, geometry = gpd.points_from_xy(ports['Longitude'], ports['Latitude']),
        crs = 4326
    )

    return ports


def subset_ports(ports, region = "Black"):
    ports['region'] = np.where(ports["World Water Body"].str.contains(region, regex = True), True, False)
    mapping = {'Very Small': 1, 'Small': 2, 'Medium': 3, 'Large': 4, ' ': None}
    ports = ports.assign(size = ports['Harbor Size'].map(mapping))
    ports2 = ports.dropna(subset = "size")
    ports2 = ports2.loc[ports2['size']>1]
    return ports2

def plot_worldmap_ports(ports, boat_position = None, region = "Black"):
    ports2 = subset_ports(ports, region)
    ports2_region = ports2.loc[ports2['region']]
    worldmap = px.scatter_mapbox(ports2,
                        lon=ports2.geometry.x, lat=ports2.geometry.y,
                        size="size", # which column to use to set the color of markers
                        color="region",
                        opacity = 0.6,
                        #marker_symbol = 'harbor',
                        mapbox_style = "stamen-toner",
                        center=dict(lat=ports2_region.geometry.y.mean(), lon=ports2_region.geometry.x.mean()),
                        zoom = 4,
                        hover_name="Main Port Name" # column added to hover information
    )

    if boat_position is None:
        return worldmap
    

    worldmap.add_trace(
        go.Scattermapbox(
            lat = boat_position.geometry.y,
            lon = boat_position.geometry.x,
            marker=go.scattermapbox.Marker(
                size=3, color = "green"
            )        
        )
    )

    return worldmap


def filter_cargo(ais_enriched):
    ais_enriched = ais_enriched.loc[ais_enriched["ShiptypeLevel1"] == "Cargo Carrying",:]
    ais_enriched = ais_enriched.loc[ais_enriched["ShipTypeLevel3"]
                                    .isin(["General Cargo", "Bulk Dry",  "Container", 
                                           "Other Bulk Dry", "Refrigerated Cargo",
                                           "Other Dry Cargo", "Bulk Dry / Oil",                                                                                  "Self Discharging Bulk Dry"]),:]
    return ais_enriched


def random_sample_position(
    AIS_enriched
):
    boat_position = AIS_enriched.sample(frac=1).drop_duplicates(subset = "mmsi")
    boats_position = gpd.GeoDataFrame(
        boat_position.loc[:,["mmsi","longitude","latitude"]],
        geometry = gpd.points_from_xy(
            x = boat_position.longitude, 
            y = boat_position.latitude
        )
    )
    return boats_position