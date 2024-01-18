import pandas as pd

class Selector:
    def __init__(self,prop_df=None,imp_det_df=None,imp_info_df=None,imp_atr_df=None) -> None:
        if any(isinstance(var,pd.DataFrame) for var in [prop_df,imp_det_df,imp_atr_df,imp_info_df]):
            self.prop_df = prop_df 
            self.imp_det_df = imp_det_df
            self.imp_info_df = imp_info_df 
            self.imp_atr_df = imp_atr_df 
        else:
            self.prop_df = pd.read_parquet('data/processed/TCAD/PROP.parquet')
            self.imp_det_df = pd.read_parquet('data/processed/TCAD/IMP_DET.parquet')
            self.imp_info_df = pd.read_parquet('data/processed/TCAD/IMP_INFO.parquet')
            self.imp_atr_df = pd.read_parquet('data/processed/TCAD/IMP_ATR.parquet')

    def select_zip(self,zipcodes:list[int]):
        prop_df,imp_info_df,imp_atr_df,imp_det_df = self.prop_df,self.imp_info_df,self.imp_atr_df,self.imp_det_df

        prop_df = prop_df[prop_df['situs_zip'].isin(zipcodes)]
        imp_info_df = imp_info_df[imp_info_df['prop_id'].isin(prop_df['prop_id'].tolist())]
        imp_det_df = imp_det_df[imp_det_df['prop_id'].isin(prop_df['prop_id'].tolist())]
        imp_atr_df = imp_atr_df[imp_atr_df['prop_id'].isin(prop_df['prop_id'].tolist())]

        return Selector(prop_df=prop_df,imp_info_df=imp_info_df,imp_atr_df=imp_atr_df, imp_det_df=imp_det_df)                                       
        # self.prop_df,self.imp_info_df,self.imp_atr_df,self.imp_det_df = prop_df,imp_info_df,imp_atr_df,imp_det_df

    def select_bldg_type(self,bldg_type:list[str]):
        prop_df,imp_info_df,imp_atr_df,imp_det_df = self.prop_df,self.imp_info_df,self.imp_atr_df,self.imp_det_df

        imp_info_df = imp_info_df[imp_info_df['imprv_type_desc'].isin(bldg_type)]

        prop_df = prop_df[prop_df['prop_id'].isin(imp_info_df['prop_id'])]

        imp_det_df = imp_det_df[imp_det_df['prop_id'].isin(imp_info_df['prop_id'].tolist())]
        imp_det_df = imp_det_df[imp_det_df['imprv_id'].isin(imp_info_df['imprv_id'].tolist())]

        imp_atr_df = imp_atr_df[imp_atr_df['prop_id'].isin(prop_df['prop_id'].tolist())]
        imp_atr_df = imp_atr_df[imp_atr_df['imprv_id'].isin(imp_info_df['imprv_id'].tolist())]

        return Selector(prop_df=prop_df,imp_info_df=imp_info_df,imp_atr_df=imp_atr_df, imp_det_df=imp_det_df)

    def process_prop_df(self):
        prop_df = self.prop_df
        imp_det_df = self.imp_det_df

        # Drop columns with 0 or 1 unique values (where all properties have the same value like all true or all false)
        prop_df = prop_df.drop(columns=prop_df.nunique()[prop_df.nunique()<=1].index.tolist())

        # Get year built based on the oldest improvement or feature
        yr_built_dict = imp_det_df.groupby('prop_id')['yr_built'].min()
        prop_df['yr_built'] = pd.to_numeric(prop_df['prop_id'].map(yr_built_dict),downcast='integer',errors='coerce')
        prop_df['HVAC_area'] = prop_df['prop_id'].map(imp_det_df.loc[imp_det_df['Imprv_det_type_cd']=='095',['prop_id','imprv_det_area']].set_index('prop_id')['imprv_det_area'].to_dict())

        for floor_num in ['1ST','2ND','3RD','4TH','5TH']:
            prop_df[f'{floor_num}_floor_area'] = prop_df['prop_id'].map(imp_det_df.loc[imp_det_df['Imprv_det_type_cd']==floor_num,['prop_id','imprv_det_area']].set_index('prop_id')['imprv_det_area'].to_dict())
        return prop_df