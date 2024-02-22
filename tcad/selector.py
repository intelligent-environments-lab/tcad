import pandas as pd

def validate_string_list_only(var,var_name='Variable'):
    if isinstance(var,str):
        return [var]
    elif isinstance(var,list) and all(isinstance(item,str) for item in var):
        return var
    else: 
        raise ValueError(f"{var_name} must be a string or list of strings")
    
def filter_zip(df,zip_codes):
    zip_codes = validate_string_list_only(zip_codes,'zip_codes')
    df = df[df['situs_zip'].isin(zip_codes)]
    return df

def filter_bldg_type(df,bldg_types):
    bldg_types = validate_string_list_only(bldg_types,'bldg_types')
    df = df[df['imprv_type_desc'].isin(bldg_types)]
    return df

class Selector:
    def __init__(self,data_dir,*,_copying=False):
        if not _copying:
            self.prop_df = pd.read_parquet(f'{data_dir}/PROP.parquet')
            self.imp_det_df = pd.read_parquet(f'{data_dir}/IMP_DET.parquet')
            self.imp_info_df = pd.read_parquet(f'{data_dir}/IMP_INFO.parquet')
            self.imp_atr_df = pd.read_parquet(f'{data_dir}/IMP_ATR.parquet')
        else:
            pass
    
    @classmethod
    def _copy(cls,prop_df,imp_info_df,imp_det_df,imp_atr_df):
        obj = cls(None,_copying = True)
        obj.prop_df = prop_df
        obj.imp_info_df = imp_info_df
        obj.imp_det_df = imp_det_df
        obj.imp_atr_df = imp_atr_df
        return obj
    
    def copy(self):
        return Selector._copy(self.prop_df,self.imp_info_df,self.imp_det_df,self.imp_atr_df)
        
    @property
    def zip_codes(self):
        return self.prop_df['situs_zip'].unique().sort_values().tolist()
    
    @property
    def bldg_types(self):
        return self.imp_info_df['imprv_type_desc'].unique().sort_values().tolist()
    
    @property
    def detail_types(self):
        return self.imp_det_df['Imprv_det_type_desc'].unique().sort_values().tolist()

    def query(self,zip_codes=None,bldg_types=None):
        """
        Filters the stored dataframes to only keep records with the specified zip_codes or building types.
        """
        prop_df = filter_zip(self.prop_df,zip_codes) if zip_codes else self.prop_df
        imp_info_df = filter_bldg_type(self.imp_info_df,bldg_types) if bldg_types else self.imp_info_df
        imp_det_df = self.imp_det_df
        imp_atr_df = self.imp_atr_df

        # These variables are used in the query strings below (code editor may say otherwise but ignore that).
        prop_ids = list(set(prop_df['prop_id']).intersection(imp_info_df['prop_id']))
        imprv_ids = imp_info_df['imprv_id']

        query1 = "prop_id in @prop_ids"
        query2 = query1 + " and imprv_id in @imprv_ids"

        prop_df2 = prop_df.query(query1)
        imp_info_df2 = imp_info_df.query(query2)
        imp_det_df2 = imp_det_df.query(query2)
        imp_atr_df2 = imp_atr_df.query(query2)

        return Selector._copy(prop_df2,imp_info_df2,imp_det_df2,imp_atr_df2)
    
    def get_properties_table(self):
        """ 
        Returns dataframe where each record is for a property or land parcel.
        
        This data is associated with the "PROP.TXT" file in the original export.
        Notable fields of interest includes addresses, tax info, and owner info.

        Property
        """
        return self.prop_df


    def get_improvements_table(self):
        """
        Returns dataframe where each record is for an improvement(i.e. building) on the on a property.

        This data is associated with the "IMP_INFO.TXT" file in the original export.
        Notable fields of interest include building type or classification(i.e. single family).

        Property->Improvement
        """
        return self.imp_info_df
    
    def get_improvement_details_table(self):
        """
        Returns dataframe where each record is for a detail (i.e floor) for an improvement (i.e building).

        This data is associated with the "IMP_DET.TXT" file in the original export.
        Notable fields of interest include the area and year built for each floor or the HVAC.

        Property->Improvement->Improvment Detail
        """
        return self.imp_det_df

    def get_improvement_features_table(self):
        """
        Returns dataframe where each record is for information for an improvement detail.

        This data is associated with the "IMP_ATR.TXT" file in the original export.
        Notable fields of interest include foundation and roof material which is typically associated
        with the first floor (improvement detail).

        Property->Improvement->Improvment Detail->Improvment Feature
        """
        return self.imp_atr_df
    
    def unstack_improvement_details_table(self):
        """
        This function takes the improvement details table as input, and returns a table with
        [HVAC_area,1ST_floor_area-->5TH_floor_area,num_floors,main_area,yr_built] for each building.

        This only works for single family buildings due to the ways floors are handled for taller buildings in TCAD.
        Floor counts >=5 could be 5+.

        """
        imp_det_df = self.imp_det_df

        # Improvment details table: goal is to pull floor num, floor_area, and vintage into parent tables
        floor_codes = ['1ST','2ND','3RD','4TH','5TH','ADDL']
        floor_labels = {floor:floor+'_floor_area' for floor in floor_codes}

        # Only interested in details that are floors 1-5 or residential hvac (095)
        filtered_df = imp_det_df[imp_det_df['Imprv_det_type_cd'].isin(floor_codes+['095'])].copy()
        filtered_df['Imprv_det_type_cd'] = filtered_df['Imprv_det_type_cd'].cat.remove_unused_categories()

        # The details rotate to become columns, aggfun=sum because if there are 2 first floors in the same building we sum the areas.
        pivoted_df = (filtered_df.pivot_table(index=['imprv_id'],columns='Imprv_det_type_cd',values='imprv_det_area',aggfunc='sum')
                    .rename(columns={'095':'HVAC_area',**floor_labels}))
        pivoted_df.columns.name=None

        # To avoid a column not in df error (5th floor can exist but not always)
        floor_labels_in_table = [label for label in floor_labels.values() if label in pivoted_df.columns]

        # Counts number of actual floors, may act weird if there is a floor over garage.
        pivoted_df['num_floors']=(pivoted_df[floor_labels_in_table]>0).sum(axis=1)

        pivoted_df['highest_floor']=pivoted_df[floor_labels_in_table].iloc[:,::-1].replace(0,pd.NA).isnull().idxmin(axis=1).str.replace('_floor_area','')
        # Note: this only sums floors 1-5, but TCAD will also sum things like half floors for their website. TODO
        pivoted_df['main_area']=pivoted_df[floor_labels_in_table].sum(axis=1)

        # Determined by the oldest detail(floor) in each improvement(building)
        pivoted_df['yr_built']=filtered_df.groupby('imprv_id')['yr_built'].min()
        return pivoted_df
    
    def unstack_improvement_attributes_table(self):
        """
        Improvement attributes table: goal is to pull info like foundation and roof into parent tables.
        This function takes the improvement attributes table as input, and returns a table with
        [Foundation,Grade Factor,Shape Factor,Ceiling Factor,Roof Covering,Roof Style] for each building.

        This function only works for single family buildings since the floors of a commericial building are
        represented differently in TCAD. For instance, all the floor area may simply be combined into an "additional floor" entry.

        Some caveats for TCAD:
        - A building can have multiple first or other floors. When that happens, the one with the larger area is kept.
        - When an attribute like foundation or roof material is unknown, TCAD seems to add rows for all available options.
            - To handle this, all rows for that attribute are dropped for the given building.
        - Attributes tend to be assigned to the first floor of a building with the others devoid of attributes.
        """

        imp_atr_df = self.imp_atr_df
        imp_det_df = self.imp_det_df

        # Columns to remove but make sure they're in df first
        drop_cols = [col for col in ['Unique Feature', 'Location', 'Condo Floor', 'CDU','Multi Imp'] if col in imp_atr_df['imprv_attr_desc']]

        # Improvement attributes table: goal is to pull info like foundation and roof into parent tables
        pivoted_df = (imp_atr_df.drop_duplicates(subset=['prop_id','prop_val_yr','imprv_id','imprv_det_id','imprv_attr_desc'],keep=False)
                                .pivot(index='imprv_det_id',columns='imprv_attr_desc',values='imprv_attr_cd')
                                .drop(columns=drop_cols))
        pivoted_df.columns.name=None


        filtered_df = pivoted_df[pivoted_df['Floor Factor'].isin(['1ST','2ND','3RD','4TH','5TH'])]

        # Add some columns that will be useful when removing duplicates
        merged_df = filtered_df.reset_index().merge(imp_det_df[['prop_id','imprv_det_id','imprv_id','imprv_det_area']],how='left',left_on='imprv_det_id',right_on='imprv_det_id')

        # Combine attributes for all floors for a given building into one row.
        final_df = (merged_df.sort_values(['prop_id','imprv_id','Floor Factor','imprv_det_area'],ascending=[True,True,True,False])
                            .drop_duplicates(subset=['prop_id','imprv_id','Floor Factor'])
                            .groupby('imprv_id').first()
                            .drop(columns=['prop_id','imprv_det_id','Floor Factor','imprv_det_area']))

        return final_df

    def get_single_family_building_summary(self,extended_info=True,remove_nonunique_columns=False):
        """
        This function returns a summary dataframe and is suitable for single family homes only. 
        
        This function ensures this by also querying for single family buildings.

        TODO: this function assumes that there is one building per property. However, there are instances of multiple single family buildings 
        in the same parcel.
        
        """

        # Ensures that only single family buildings are included, could be removed once functionality improves
        sf = self.query(bldg_types='1 FAM DWELLING')

        # Merge all four tables into one at the building level (one row per building, can be multiple buildings per property)
        merged_df = sf.prop_df.merge(
            sf.imp_info_df.set_index('imprv_id')
            .merge(sf.unstack_improvement_details_table(),
                   how='outer',left_index=True, right_index=True)
            .merge(sf.unstack_improvement_attributes_table(),
                   how='outer',left_index=True, right_index=True)
                   .reset_index(),
            on='prop_id', how='outer')
        
        # Move improvement id column to front
        merged_df.insert(1,'imprv_id',merged_df.pop('imprv_id'))
        
        merged_df
        
        # Reduce to one building per property by keeping largest area
        merged_df = (merged_df.sort_values(['prop_id','main_area'],ascending=[True,False])
                    .drop_duplicates(subset='prop_id',keep='first',ignore_index=True))

        
        main_cols = ['prop_id', 'imprv_id', 'prop_val_yr_x', 'situs_num','situs_street_prefx', 'situs_street',
       'situs_street_suffix', 'situs_unit','situs_city', 'situs_zip', 'appraised_val','HVAC_area', 
       '1ST_floor_area', '2ND_floor_area','3RD_floor_area', '4TH_floor_area', '5TH_floor_area', 'ADDL_floor_area',
       'num_floors', 'highest_floor', 'main_area', 'yr_built', 'Foundation',
       'Grade Factor', 'Roof Covering', 'Roof Style', 'Shape Factor',
       'Ceiling Factor'] 
        
        extra_cols = ['land_acres','imprv_val','imprv_state_cd_y','abs_subdv_cd', 'hood_cd', 'block',
       'land_hstd_val', 'land_non_hstd_val', 'imprv_hstd_val',
       'imprv_non_hstd_val', 'market_value', 'ten_percent_cap',
       'assessed_val', 'imprv_homesite', 'imprv_homesite_pct','en_exempt', 'pc_exempt', 'so_exempt','eco_exempt']
        
        if extended_info:
            main_cols += extra_cols

        if remove_nonunique_columns:
            merged_df = merged_df.drop(columns=[col for col in merged_df.nunique()
                                                [merged_df.nunique()<=1].index.tolist() 
                                                if col not in ['prop_val_yr_x']])
            
        main_cols = [col for col in main_cols if col in merged_df]
        return merged_df[main_cols]