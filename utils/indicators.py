import pandas as pd
import utils.functions as fc
import numpy as np

BUCKET = "projet-hackathon-un-2022"

fs = fc.create_s3_fs()

def assign_country_flag(ais_enriched):
    """
    Function that assigns a country flag to each ship and returns the traces with country flag 
    and the dataset with unique mmsi with origin flag
    """
    mmsi_number = pd.read_csv(fs.open(f'{BUCKET}/AIS/mmid.csv',
                         mode='rb'), error_bad_lines=False, encoding='iso-8859-1', sep=";")
    ais_enriched["Digit"] = ais_enriched["mmsi"].astype(str).str[0:3]
    mmsi_number["Digit"] = mmsi_number["Digit"].astype(str)
    ais_enriched = pd.merge(ais_enriched, mmsi_number, on=["Digit"], how="left")
    ais_enriched = ais_enriched.rename({"Allocated to" : "origin_flag"}, axis=1)
    print("Classification of flag country")
    origin_countries = ais_enriched.groupby("mmsi")["origin_flag"].first()
    print(origin_countries.value_counts())
    print("Number of missing origin flag")
    print(ais_enriched.loc[ais_enriched["origin_flag"].isna() | ais_enriched["origin_flag"] == "", :].shape)
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


def check_distance_levenshtein(data: pd.DataFrame, variables=None) -> pd.DataFrame:
    """
    Compute levenshtein distance between list of columns of a Pandas DataFrame
    Parameters
    ----------
    data: Pandas dataframe with some columns to be compared
    variables: List of variables from which distance will be computed. The first one will be used as benchmark
    Returns
    -------
    Initial dataframe with new columns representing levenshtein ratio (computed using :meth:`rapidfuzz.fuzz.partial_ratio`)
    between variables
    """

    if variables is None:
        variables = ['libel_clean_relevanc', 'libel_clean_OFF', 'libel_clean_IRI']

    data[variables] = data[variables].astype(str)
    newvars = ["ratio{}".format(str(i)) for i in range(1, len(variables))]
    data[newvars] = pd.concat(
        [data.apply(lambda x: rapidfuzz.fuzz.partial_ratio(x[variables[0]], x[y]), axis=1) for y in variables[1:]],
        axis=1
    )
    return data


def fuzzy_match_destination_country(ais_data):
    """
    Function that cleans up port data and fuzzy matches variable destination
    to all ports in the world to get the country of destination
    """
    ais_data["destination_c"] = (ais_data["destination"].replace("None", np.nan)
                        .replace('B 2 A', np.nan)
                        .replace('J0', np.nan)
                        .replace('0', np.nan)
                        .replace(',',np.nan)
                        .replace('G', np.nan)
                        .replace('I0', np.nan)
                        .replace('C', np.nan)
                        .replace('D', np.nan)
                        .replace('C0', np.nan)
                        .replace('S0', np.nan).replace('M', np.nan)
                        .replace('X', np.nan).replace('A', np.nan)
                        .replace('ZRP', np.nan)
                        .replace('AT SEA', np.nan)
                        .replace("$", np.nan)
                        .replace('#HDCR*H!!!)JM0I*!&', np.nan)
                        .replace("%", np.nan)
                        .replace("-", np.nan).replace('-C>-',np.nan)
                        .replace('-C?M',np.nan)
                        .replace('-[C?<)RD\\K$OQ8)',np.nan)
                        .replace( '.',np.nan)
                        .replace( '...',np.nan)
                        .replace('0XA F3L86F,[E^CON',np.nan)
                        .replace('17 B#',np.nan)
                        .replace('2',np.nan)
                        .replace('3',np.nan)
                        .replace('450',np.nan)
                        .replace(r'(\s\/\s[a-zA-Z]+)', '', regex=True)
                        .replace(r'(\/[a-zA-Z]+)', '', regex=True)
                        .replace('FISHING AREA', np.nan))
    list_errors = ['AREA3 FISHING GROUND', 'ARM GAURD ON BOARD',
     'ARM GUARD ON BOARD', 'ARM.GUARDS ON BOARD', 'ARMED GUARD ON BOARD', 'ARMED GUARDS ON BOAP',
     'ARMED GUARDS ON BOAR', 'ARMED GUARDS ONB', 'ARMED GUARDS ONBOARD', 'ARMED ON BOARD',
     'ARMGUARD O.B', 'ARMGUARD ONBOARD', 'ARMS GUARDS ON BOARD']
    ais_data.loc[ais_data["destination_c"].isin(list_errors), "destination_c"] = np.nan
    ais_data["length_dest"] = ais_data["destination_c"].str.len()
    ais_data.loc[ais_data["length_dest"] == 2, "destination_c"] = np.nan
    print("Number of unique destination ports")
    print(ais_data["destination_c"].nunique())
    print("shape of ais")
    print(ais_data.shape)
    # import port data
    ports = pd.read_csv('https://msi.nga.mil/api/publications/download?type=view&key=16920959/SFH00000/UpdatedPub150.csv')
    ports = ports.loc[:, ["Main Port Name", "Country Code"]]
    ports["Main Port Name"] = ports["Main Port Name"].str.upper()
    ports = ports.rename({"Main Port Name" : "destination"}, axis=1)
    # filter boats with destination ports
    ais_data = ais_data.loc[~ais_data["destination_c"].isna(),:]
    print("shape of ais filtered with no na destination")
    print(ais_data.shape)
    
    # select one destination by boat
    destination_ais = ais_data.groupby("mmsi")["destination_c"].first()
    destination_ais = destination_ais.reset_index().copy()
    # cartesian product of ports and destination by boat
    cart_prod = destination_ais.merge(ports, how='cross')
    # fuzzy matching using rapidfuzz
    print("Fuzzy matching in process")
    res_fuz_match = check_distance_levenshtein(cart_prod, ["destination_c", "destination"])
    idx = res_fuz_match.groupby(['destination_c'])['ratio1'].transform(max) == res_fuz_match['ratio1']
    res_fuz_match2 = res_fuz_match[idx]
    res_fuz_match2["true_match"] = np.nan
    res_fuz_match2.loc[res_fuz_match2["ratio1"]>80, "true_match"] = \
        res_fuz_match2.loc[res_fuz_match2["ratio1"]>80, "destination"]
    return ais_data, res_fuz_match2