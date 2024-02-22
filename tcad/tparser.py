# This file is named tparser because 'parser' is already a part of the standard python library.
from io import BytesIO
import zipfile,requests
from pathlib import Path

import pandas as pd

LAYOUT_URL = 'https://traviscad.org/wp-content/largefiles/Legacy8.0.25-Export-Layouts-07242023.zip'
LAYOUT_FILE = 'Legacy8.0.25-Appraisal Export Layout07242023.xlsx'

TCAD_DIR = 'data/raw/2023_Certified_Appraisal_Export_Supp_0_07232022'

def get_layout(url=LAYOUT_URL,*,skiprows=None,nrows=None,cache_dir='cache',filename=LAYOUT_FILE):
    """ Loads or downloads the layout excel file """
    if not Path(f'{cache_dir}/{filename}').exists():
        data = requests.get(url).content
        zipfile.ZipFile(BytesIO(data)).extract(filename,path=cache_dir)
    return pd.read_excel(f'{cache_dir}/{filename}',skiprows=skiprows,nrows=nrows).drop(columns=['Unnamed: 6'])

def _to_parquet(df,export_filepath):
    """ Convenience function """
    dir, filename = export_filepath.rsplit('/',maxsplit=1) 
    Path(dir).mkdir(parents=True,exist_ok=True)
    df.to_parquet(f'{dir}/{filename}')

def load_improvement_info_layout():
    """ Loads in header info and metadata for improvement info file."""
    imp_info_layout = get_layout(skiprows=875,nrows=12)
    imp_info_layout['col_spec'] = imp_info_layout.apply(lambda row:(row['Start']-1,row['End']),axis=1)
    return imp_info_layout

def parse_improvement_info(input_file=f'{TCAD_DIR}/IMP_INFO.TXT',*,export_file=None,optimize=True):
    """ Load and parse improvement info data. """
    imp_info_layout = load_improvement_info_layout()
    imp_info_df = pd.read_fwf(input_file,names=imp_info_layout['Field Name'].tolist(),header=None,colspecs=imp_info_layout['col_spec'].tolist())
    if optimize:
        imp_info_df = optimize_memory(imp_info_df)
    if export_file:
        _to_parquet(imp_info_df,export_file)
    return imp_info_df

def parse_improvement_details(input_file=f'{TCAD_DIR}/IMP_DET.TXT',*,export_file=None):
    """ Load and parse improvement details data. """
    imp_det_df = pd.read_fwf(input_file,widths=[12,4,12,12,10,25,10,4,4,15,14],header=None,
            names=['prop_id','prop_val_yr','imprv_id','imprv_det_id','Imprv_det_type_cd',
                   'Imprv_det_type_desc','Imprv_det_class_cd','yr_built','depreciation_yr',
                   'imprv_det_area','imprv_det_val'],dtype={'prop_id':'UInt32','prop_val_yr':'UInt16','imprv_id':'UInt32','imprv_det_id':'UInt32','Imprv_det_type_cd':'category',
                   'Imprv_det_type_desc':'category','Imprv_det_class_cd':'category','yr_built':'UInt16','depreciation_yr':'UInt16',
                   'imprv_det_area':'Float32'})
    imp_det_df['imprv_det_area'] = imp_det_df['imprv_det_area'].astype("UInt32")
    imp_det_df['imprv_det_val'] = pd.to_numeric(imp_det_df['imprv_det_val'],errors='coerce').astype('UInt32')
    if export_file:
        _to_parquet(imp_det_df,export_file)
    return imp_det_df

def load_improvement_features_layout():
    imp_atr_layout = get_layout(skiprows=926,nrows=8)
    imp_atr_layout['col_spec'] = imp_atr_layout.apply(lambda row:(row['Start']-1,row['End']),axis=1)
    imp_atr_layout['dtype']=imp_atr_layout.apply(lambda row:select_type(row),axis=1)
    return imp_atr_layout

def parse_improvement_features(input_file=f'{TCAD_DIR}/IMP_ATR.TXT',*,export_file=None,optimize=True):
    imp_atr_layout = load_improvement_features_layout()
    imp_atr_df = pd.read_fwf(input_file,names=imp_atr_layout['Field Name'].tolist(),header=None,colspecs=imp_atr_layout['col_spec'].tolist())
    if optimize:
        imp_atr_df = optimize_memory(imp_atr_df)
    if export_file:
        _to_parquet(imp_atr_df,export_file)
    return imp_atr_df

def load_property_layout(filter=True):
    """Retrieve header information for property file"""
    prop_layout = get_layout(skiprows=54,nrows=436)
    prop_layout['col_spec'] = prop_layout.apply(lambda row:(row['Start']-1,row['End']),axis=1)
    prop_layout = prop_layout.loc[~prop_layout['Field Name'].isin(['filler','mineral_lease_name','mineral_lease_operator'])]
    prop_layout['dtype'] = prop_layout.apply(lambda row:select_type(row),axis=1)
    if filter:
        return prop_layout[~prop_layout['Field Name'].str.contains('sup_|flag|mineral|ag_|rendition_|timber_|_agent_|py_|jan1_|appr_|ex_|mortgage_|(?<!co|en|so|pc)_exempt|qualify_yr|_prorate',regex=True)].reset_index(drop=True)
    return prop_layout

def parse_property_details(input_file=f'{TCAD_DIR}/PROP.TXT',*,export_file=None,optimize=True,filter=True):
    property_layout = load_property_layout(filter=filter)
    prop_df = pd.read_fwf(input_file,names=property_layout['Field Name'].tolist(),
                      colspecs=property_layout['col_spec'].tolist(),header=None)
    if optimize:
        prop_df = optimize_memory(prop_df)
    if export_file:
        _to_parquet(prop_df,export_file)
        # prop_df.to_parquet(export_file)
    return prop_df

def optimize_memory(df):
    """ Downcasting numeric variables to save memory"""
    df = df.copy()

    # Fix numeric columns that have hyphens in their values
    bad_col = [col for col in df.columns if "_val" in col and df[col].dtype == "object"]
    df[bad_col] = df[bad_col].apply(
        lambda col: pd.to_numeric(col.str.replace("00-", ""))
    )

    # Convert floats to ints when possible
    float_cols = df.select_dtypes(["float32", "float64"])
    for col in float_cols:
        if (df[col] % 1 == 1).sum() == 0:
            df[col] = pd.to_numeric(df[col], downcast="integer", errors="coerce")

    # Downcast float
    float_cols = df.select_dtypes(["float32", "float64"])
    df[float_cols.columns] = float_cols.apply(
        lambda col: pd.to_numeric(col, downcast="float")
    )

    # Downcast int
    int_cols = df.select_dtypes(["int16", "int32", "int64"])
    df[int_cols.columns] = int_cols.apply(
        lambda col: pd.to_numeric(col, downcast="integer")
    )

    # Strings as category
    obj_cols = df.select_dtypes(["object"])
    df[obj_cols.columns] = obj_cols.astype("category")
    return df


def select_type(row):
    value = row["Datatype"]
    dtype = value.split("(")[0]
    length = int(value.split("(")[1].split(")")[0])
    match dtype:
        case "numeric":
            if "_yr" in row["Field Name"]:
                return "int16"
            return "float32"
        case "int":
            if length < 5:
                return "int16"
            elif length < 10:
                return "int32"
            else:
                return "int64"
        case _:
            return None
        
if __name__ == '__main__':
    pass