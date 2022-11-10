import pandas as pd
import utils.functions as fc


BUCKET = "projet-hackathon-un-2022/open-data/FAO/trade_matrix_fao/"

fs = fc.create_s3_fs()

def import_fao_matrix():
    """ Function that imports the FAO detailed trade matrix"""
    path2data = BUCKET + "Trade_DetailedTradeMatrix_E_All_Data_(Normalized).csv"
    with fs.open(path2data, mode="rb") as file_in:
        fao_df = pd.read_csv(file_in, error_bad_lines=False, encoding='iso-8859-1')
    return fao_df


def extract_ukraine_exports(fao_df):
    """ Function that extracts exports in cereals from Ukraine"""
    print(fao_df.shape) # 41 591 386
    fao_df2 = fao_df.loc[fao_df["Year"].isin([2019]),:] 
    print(fao_df2.shape) # 2021010
    fao_df2 = fao_df2.drop(["Year Code", "Reporter Country Code (M49)", 
                            "Partner Country Code (M49)", "Item Code (CPC)"], axis=1)
    print(fao_df2["Item"].nunique()) # 396 valeurs de produits
    # selection of cereals
    # selection des céréales
    fao_df2 = fao_df2.loc[fao_df2["Item Code"].isin([103, 104, 111, 113, 115,  126, 15, 150, 16, 17, 18, 20, 21, 212, 27, 29, 30, 31, 32, 38, 41, 44, 46, 49, 56, 57, 
                                       58, 59, 60, 61, 79, 81, 83, 85, 89, 91, 92, 94, 95, 96]), :]
    # selection exports from ukraine
    fao_df_ukr = fao_df2.loc[fao_df2["Reporter Countries"].isin(["Ukraine"]) & 
                             (fao_df2["Element"] == "Export Quantity"), :]
    fao_df_ukr = fao_df_ukr.loc[:,["Reporter Countries", "Partner Countries", "Item", "Element", "Year", "Value"]]
    fao_df_ukr.columns = [c.lower() for c in fao_df_ukr.columns]
    fao_df_ukr = fao_df_ukr.rename({"reporter countries": "report_country", 
                                "partner countries": "partner_country"}, axis=1)
    print(fao_df_ukr.shape)
    return fao_df_ukr