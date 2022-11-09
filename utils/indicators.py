import pandas as pd
import utils.functions as fc

fs = fc.create_s3_fs()
BUCKET = "projet-hackathon-un-2022"


def assign_country_origin(AIS_enriched):
    mmsi_number = pd.read_csv(fs.open(f'{BUCKET}/AIS/mmid.csv',
                         mode='rb'), error_bad_lines=False, encoding='iso-8859-1', sep=";")
    AIS_enriched["Digit"] = AIS_enriched["mmsi"].astype(str).str[0:3]
    mmsi_number["Digit"] = mmsi_number["Digit"].astype(str)
    AIS_enriched = pd.merge(AIS_enriched, mmsi_number, on=["Digit"], how="left")
    AIS_enriched = AIS_enriched.rename({"Allocated to" : "origin_country"}, axis=1)
    return AIS_enriched