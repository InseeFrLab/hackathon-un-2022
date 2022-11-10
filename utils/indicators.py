import pandas as pd
import utils.functions as fc
import numpy as np
import rapidfuzz

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
    # Keep only matched ports above 80 partial ratio
    res_fuz_match2.loc[res_fuz_match2["ratio1"]>80, "true_match"] = \
        res_fuz_match2.loc[res_fuz_match2["ratio1"]>80, "destination"]
    # Rename destination country and take it only when above 80 partial ratio
    res_fuz_match2["matched_destination_country"] = np.nan
    res_fuz_match2.loc[~res_fuz_match2["true_match"].isna(), "matched_destination_country"] = \
        res_fuz_match2.loc[~res_fuz_match2["true_match"].isna(), "Country Code"]
    res_fuz_match2 = res_fuz_match2.rename({"true_match" : "matched_destination_port",
                                           "destination_c": "raw_port_destination"}, axis=1)
    # keep only boat that have matched
    res_fuz_match2 = res_fuz_match2.loc[~res_fuz_match2["matched_destination_country"].isna(),
                                        ["mmsi", "matched_destination_country", "matched_destination_port", 
                                            "ratio1", "raw_port_destination"]]
    res_fuz_match2 = res_fuz_match2.sort_values("ratio1", ascending=False)
    res_fuz_match2 = res_fuz_match2.groupby("mmsi")[["matched_destination_country", "matched_destination_port", 
                                            "ratio1", "raw_port_destination"]].first()
    res_fuz_match2 = res_fuz_match2.reset_index()
    # check some stats about matched  countries
    print("Classification of destination countries")
    print(res_fuz_match2["matched_destination_country"].value_counts())
    # print(res_fuz_match2.loc[res_fuz_match2["matched_destination_country"].isna(), :].shape)
    print("Percentage of boat with matched countries")
    print(res_fuz_match2.loc[~res_fuz_match2["matched_destination_country"].isna(), :].shape[0]/ais_data["mmsi"].nunique())
    return ais_data, res_fuz_match2

def find_countries_aggregate_table(df, origin=True):
    if origin:
        var_name = 'origin'
    else: var_name = 'destination'
    
    # Full port list to look in
    port_data = pd.read_csv('https://msi.nga.mil/api/publications/download?type=view&key=16920959/SFH00000/UpdatedPub150.csv')
    port_codes_data = port_data[['UN/LOCODE', 'Country Code']]
    port_data = port_data.loc[:, ["Main Port Name", "Country Code"]]
    port_data["Main Port Name"] = port_data["Main Port Name"].str.upper()
    port_data = port_data.rename({"Main Port Name" : "port", "Country Code": "country"}, axis=1)
    
    port_codes_data = port_codes_data[['UN/LOCODE', 'Country Code']]
    port_codes_data = port_codes_data[port_codes_data['UN/LOCODE'] != ' ']
    port_codes_data = port_codes_data[port_codes_data['UN/LOCODE'] != '']
    port_codes_data = port_codes_data.drop_duplicates(subset=['UN/LOCODE']).rename(
        columns={'UN/LOCODE': 'port',
                 "Country Code": "country"}
    )
    port_data = pd.concat([port_data, port_codes_data])
    port_data = port_data.drop_duplicates(subset=['port'])

    # Clean
    list_errors = ['AREA3 FISHING GROUND', 'ARM GAURD ON BOARD',
                   'ARM GUARD ON BOARD', 'ARM.GUARDS ON BOARD', 'ARMED GUARD ON BOARD', 'ARMED GUARDS ON BOAP',
                   'ARMED GUARDS ON BOAR', 'ARMED GUARDS ONB', 'ARMED GUARDS ONBOARD', 'ARMED ON BOARD',
                   'ARMGUARD O.B', 'ARMGUARD ONBOARD', 'ARMS GUARDS ON BOARD']
    df["port"] = (df["port"].replace("None", np.nan)
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
    df.loc[df["port"].isin(list_errors), "port"] = np.nan

    unique_ports_arrivals = pd.DataFrame(df.port.unique())
    unique_ports_arrivals.columns = ['port']
    cart_prod = unique_ports_arrivals.merge(port_data, how='cross')

    res_fuz_match = check_distance_levenshtein(cart_prod, ["port_x", "port_y"])

    idx = res_fuz_match.groupby(['port_x'])['ratio1'].transform(max) == res_fuz_match['ratio1']
    res_fuz_match2 = res_fuz_match[idx]
    res_fuz_match2 = res_fuz_match2.groupby(['port_x']).first()
    res_fuz_match2["true_match"] = np.nan

    res_fuz_match2.loc[res_fuz_match2["ratio1"] > 80, "true_match"] = \
        res_fuz_match2.loc[res_fuz_match2["ratio1"] > 80, "port_y"]

    df = df.merge(
        res_fuz_match2.reset_index()[['port_x', 'true_match', 'country']], left_on='port', right_on='port_x'
    )
    df = df.drop('port_x', axis=1)
    df.loc[df['true_match'].isna(), 'country'] = np.nan

    # Imputation
    DICT_VAR = dict(df.country.value_counts())

    keys, weights = zip(*DICT_VAR.items())
    probs = np.array(weights, dtype=float) / float(sum(weights))
    sample_np = np.random.choice(keys, 1, p=probs)

    df.country = [np.random.choice(keys, 1, p=probs)[0] if val != val else val for val in df.country]

    country_list = ['Turkey', 'Bulgaria', 'Romania', 'Ukraine', 'Russia', 'Georgia']
    df = df[df.country.isin(country_list)]
    df = df.drop('true_match', axis=1)

    # Matching origin or destination
    df[var_name] = (df[var_name].replace("None", np.nan)
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
    df.loc[df[var_name].isin(list_errors), var_name] = np.nan

    port_data = port_data.rename(columns={'port': var_name, 'country': 'country_' + var_name})
    
    unique_arrivals = pd.DataFrame(df[var_name].unique())
    unique_arrivals.columns = [var_name]
    cart_prod = unique_arrivals.merge(port_data, how='cross')

    res_fuz_match = check_distance_levenshtein(cart_prod, [var_name + "_x", var_name + "_y"])

    idx = res_fuz_match.groupby([var_name + '_x'])['ratio1'].transform(max) == res_fuz_match['ratio1']
    res_fuz_match2 = res_fuz_match[idx]
    res_fuz_match2 = res_fuz_match2.groupby([var_name + '_x']).first()
    res_fuz_match2["true_match"] = np.nan

    res_fuz_match2.loc[res_fuz_match2["ratio1"] > 80, "true_match"] = \
        res_fuz_match2.loc[res_fuz_match2["ratio1"] > 80, var_name + "_y"]

    df = df.merge(
        res_fuz_match2.reset_index()[[var_name + '_x', 'true_match', 'country_' + var_name]], left_on=var_name, right_on=var_name + '_x'
    )
    df = df.drop(var_name + '_x', axis=1)
    df.loc[df['true_match'].isna(), 'country_' + var_name] = np.nan
    df = df.drop('true_match', axis=1)

    # Imputation
    DICT_VAR = dict(df['country_' + var_name].value_counts())

    keys, weights = zip(*DICT_VAR.items())
    probs = np.array(weights, dtype=float) / float(sum(weights))
    sample_np = np.random.choice(keys, 1, p=probs)

    df['country_' + var_name] = [np.random.choice(keys, 1, p=probs)[0] if val != val else val for val in df['country_' + var_name]]

    return df
