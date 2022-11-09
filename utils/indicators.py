import pandas as pd
import utils.functions as fc
import numpy as np

BUCKET = "projet-hackathon-un-2022"

fs = fc.create_s3_fs()

def assign_country_origin(ais_enriched):
    mmsi_number = pd.read_csv(fs.open(f'{BUCKET}/AIS/mmid.csv',
                         mode='rb'), error_bad_lines=False, encoding='iso-8859-1', sep=";")
    ais_enriched["Digit"] = ais_enriched["mmsi"].astype(str).str[0:3]
    mmsi_number["Digit"] = mmsi_number["Digit"].astype(str)
    ais_enriched = pd.merge(ais_enriched, mmsi_number, on=["Digit"], how="left")
    ais_enriched = ais_enriched.rename({"Allocated to" : "origin_country"}, axis=1)
    print("Classification of origin country")
    origin_countries = ais_enriched.groupby("mmsi")["origin_country"].first()
    print(origin_countries.value_counts())
    print("Number of missing origin country")
    print(ais_enriched.loc[ais_enriched["origin_country"].isna() | ais_enriched["origin_country"] == "", :].shape)
    return ais_enriched, origin_countries


def assign_destination_country(ais_enriched):
    print("Percentage missing destination port")
    destination_ports = ais_enriched.groupby("mmsi")["destination"].first().reset_index()
    print(destination_ports.loc[(destination_ports["destination"].isna()) | (destination_ports["destination"] == ""), :].shape[0]/destination_ports.shape[0])
    print("Number missing destination port")
    print(destination_ports.loc[destination_ports["destination"].isna(), :].shape[0])
    # Download port data
    ports = pd.read_csv('https://msi.nga.mil/api/publications/download?type=view&key=16920959/SFH00000/UpdatedPub150.csv')
    ports = ports.loc[:, ["Main Port Name", "Country Code"]]
    ports["Main Port Name"] = ports["Main Port Name"].str.upper()
    ports = ports.rename({"Main Port Name" : "destination"}, axis=1)
    ais_enriched = pd.merge(ais_enriched, ports, on = ["destination"], how="left")
    # cleaning destination
    print(ais_enriched.groupby("mmsi")["destination"].unique())
    #ais_enriched["destination2"] = ais_enriched["destination"].replace("None", np.nan)
    # ais_enriched["destination2"] = ais_enriched["destination"].replace()
    ais_enriched = ais_enriched.rename({"Country Code": "destination_country"}, axis=1)
    print("Classification of destination countries")
    destination_ports = ais_enriched.groupby("mmsi")["destination_country"].first().reset_index()
    print(destination_ports["destination_country"].value_counts())
    print(destination_ports.loc[destination_ports["destination_country"].isna(), :].shape)
    print(destination_ports.loc[destination_ports["destination_country"].isna(), :].shape[0]/destination_ports.shape[0])
    return ais_enriched, destination_ports