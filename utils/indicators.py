import pandas as pd
import utils.functions as fc


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
    print(ais_enriched["origin_country"].value_counts())
    print("Number of missing origin country")
    print(ais_enriched.loc[ais_enriched["origin_country"].isna() | ais_enriched["origin_country"] == "", :].shape)
    return ais_enriched


def assign_destination_country(ais_enriched):
    print("Percentage missing destination port")
    print(ais_enriched.loc[(ais_enriched["destination"].isna()) | (ais_enriched["destination"] == ""), :].shape[0]/ais_enriched.shape[0])
    print("Number missing destination port")
    print(ais_enriched.loc[ais_enriched["destination"].isna(), :].shape[0])
    # Download port data
    ports = pd.read_csv('https://msi.nga.mil/api/publications/download?type=view&key=16920959/SFH00000/UpdatedPub150.csv')
    ports = ports.loc[:, ["Main Port Name", "Country Code"]]
    ports["Main Port Name"] = ports["Main Port Name"].str.upper()
    ports = ports.rename({"Main Port Name" : "destination"}, axis=1)
    ais_enriched = pd.merge(ais_enriched, ports, on = ["destination"], how="left")
    ais_enriched = ais_enriched.rename({"Country Code": "destination_country"}, axis=1)
    print("Classification of destination countries")
    print(ais_enriched["destination_country"].value_counts())
    return ais_enriched