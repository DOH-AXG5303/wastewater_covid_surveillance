import pyodbc
import pandas as pd
import numpy as np
import requests
import io
import re
import redcap
from lims_login import credentials
from lims_login import redcap_tokens_prod
from lims_login import redcap_api_url

import logging


# {"LIMS Value": "REDCap Value"}
dict_lims_column_map = {
                         'SampleCollectDate': 'sample_collect_date',
                         'SampleCollectTime': 'sample_collect_time',
                         'CollectionWaterTemp': 'collection_water_temp',
                         'CollectionStorageTime': 'collection_storage_time',
                         'CollectionStorageTemp': 'collection_storage_temp',
                         'Pretreatment': 'pretreatment',
                         'PretreatmentSpecify': 'pretreatment_specify', #all null values in LIMS (05/11/2022)
                         'EquivSewageAmt': 'equiv_sewage_amt',
                         'TestResultDate': 'test_result_date',
                         'FlowRate': 'flow_rate',
                         'SARSCoV2Units': 'sars_cov2_units',
                         'SARSCoV2AvgConc': 'sars_cov2_avg_conc',
                         'SARSCoV2StdError': 'sars_cov2_std_error',
                         'SARSCoV2CI95lo': 'sars_cov2_cl_95_lo',
                         'SARSCoV2CI95up': 'sars_cov2_cl_95_up',
                         'SARSCoV2BelowLOD': 'sars_cov2_below_lod',
                         'LODSewage': 'lod_sewage',
                         'NTCAmplify': 'ntc_amplify',
                         '% Recovery Eff.': 'rec_eff_percent',
                         'InhibitionDetect': 'inhibition_detect',
                         'InhibitionAdjust': 'inhibition_adjust',
                         "InhibitionMethod": "inhibition_method",
                         'ConcentrationMethod': 'concentration_method',
                         'ExtractionMethod': 'extraction_method',
                         'PreConcStorageTime': 'pre_conc_storage_time',
                         'PreConcStorageTemp': 'pre_conc_storage_temp',
                         'PreExtStorageTime': 'pre_ext_storage_time',
                         'PreExtStorageTemp': 'pre_ext_storage_temp',
                         'TotConcVol': 'tot_conc_vol',
                         'QualityFlag': 'quality_flag',
                         'SubmitterSampleNumber': 'sample_id'}

#Columns that are numeric in LIMS, also numeric in REDCap
numeric_clms= [
             'collection_water_temp',
             'collection_storage_temp',
             'equiv_sewage_amt',
             'flow_rate',
             'sars_cov2_std_error',
             'sars_cov2_cl_95_lo',
             'sars_cov2_cl_95_up',
             'pre_ext_storage_temp',
             'collection_storage_time',
             'pre_ext_storage_time',
             'pre_conc_storage_time',
             'rec_eff_percent',
             'sars_cov2_avg_conc', #added to numeric
            ]

# special field that include both numeric and text - extract only numeric part
text_to_numeric =  [
                     'tot_conc_vol' #unique situation
                    ]

#the followeing fields must be in all lowercase for consistent import to Redcap.
#Mostly yes/no values. Lims sometimes has entires such as YES/Yes/yes, or NO/No/no 
lowercase_fields = [
                 'quality_flag',
                 'inhibition_adjust',
                 'ntc_amplify',
                 'pretreatment',
                 'inhibition_detect',
                 'sars_cov2_below_lod',
                    ]

#standard yes/no conversion from LIMS to REDCap format
yes_no_map = {'yes': 'Yes',
              'no': 'No',
              'not_tested': 'Not Tested'}

#Converting choice fields from LIMS format to REDCap format
choice_fields = {
                 'quality_flag': yes_no_map,
                 'inhibition_adjust': yes_no_map,
                 'ntc_amplify': yes_no_map,
                 'inhibition_detect': yes_no_map,
                 'sars_cov2_below_lod': yes_no_map,
                 'pretreatment': {"yes":1,
                                  "no":0},
                 'sars_cov2_units':{'Copies/L':1, #REDCAP:copies/L wastewater
                                    'Copies/g':3},  #REDCAP:copies/g wet sludge
                 'extraction_method': {'MagMAX Viral/Pathogen Nucleic Acid Isolation Kit':"magmax"}, 
                 'concentration_method': {"Skim Milk Flocculation":"skimmilk",
                                          "Ceres Nanotrap":"ceresnano"},
                }

#force the following fields to specified value in REDCap (regardless of LIMS value). 
set_field_values = {'sars_cov2_units':1}

#Force the following dtypes for select fields
fields_dtypes = {'pretreatment': "Int64",
              'sars_cov2_units': "Int64",}




def export_df_from_LIMS():
    """
    Create ODBC connection with SQL LIMS database. Return all waster water LIMS data as dataframe. 
    return: Dataframe
    
    ######## Alternate way to pull SQL data object ##########
    #cursor = cnxn.cursor()
    #cursor.execute('SELECT * FROM [vz_Epi_ELS_SARS-CoV-2 ddPCR]')
    """
    
    cnxn = pyodbc.connect(credentials) # credentials = 'DSN=LIMS_DATA;UID=xxxxxxx;PWD=xxxxxxx'
    sql_query = pd.read_sql_query('SELECT * FROM [vz_Epi_ELS_SARS-CoV-2 ddPCR]',cnxn)

    return sql_query

def isolate_relavent_data(df_lims):
    """
    identify columns relavent to redcap import, rename to redcap convention 
    convert sample_id to numeric (and drop all non numeric rows)
    Convert 'sample_id' to int64 (will become index in subsequent functions)
    """
    
    df_lims = df_lims[dict_lims_column_map.keys()].copy() #isolate columns of interest
    df_lims = df_lims.rename(columns = dict_lims_column_map) #rename columns to redcap standard
    

    df_lims['sample_id'] = pd.to_numeric(df_lims['sample_id'], errors = "coerce")
    df_lims = df_lims.dropna(subset = ['sample_id'])
    df_lims['sample_id'] = df_lims['sample_id'].astype(np.int64)
    
    df_lims = df_lims.reset_index(drop = True)
    
    return df_lims 

def convert_numeric(df):
    """
    convert numeric columns to numeric, coerce errors to NA
    """
    df = df.copy()
    df[numeric_clms] = df[numeric_clms].apply(pd.to_numeric, errors = "coerce")
    
    return df

def freetext_transform(df_lims):
    """
    Some columns are commonly filled with digits followed by numbers (e.g.,500ml) 
    Regex split values to numeric and text, replace value to the numeric portion. 
    Convert entire column to numeric, coerce errors. 
    """
    
    df_lims = df_lims.copy()

    for i in text_to_numeric:
        series = df_lims[i].str.extract(r"([0-9\.]+)([A-Za-z ]+)?")[0]
        df_lims[i] = series

    df_lims[text_to_numeric] = df_lims[text_to_numeric].apply(pd.to_numeric, errors = "coerce")

    return df_lims


def convert_choice_fields(df_lims):
    """
    Convert select fields to lowercase (yes/no) values
    Convert choice fields to matching mapped values in REDCap
    """
    
    df_lims = df_lims.copy()
    
    #setting specified fields to lowercase
    df_lims[lowercase_fields] = df_lims[lowercase_fields].apply(lambda x: x.str.lower())
    
    #convert choice_fields to mapped value
    df_lims[list(choice_fields.keys())] = df_lims[choice_fields.keys()].apply(lambda x: x.map(choice_fields[x.name]) )
    
    return df_lims


def force_values(df_lims):
    """
    """
    
    df_lims = df_lims.copy()
    
    for clm in set_field_values.keys():
        
        df_lims[clm] = set_field_values[clm]
        
    return df_lims


def set_dtypes(df_lims):
    """
    """
    
    df_lims = df_lims.copy()
    
    for clm in fields_dtypes.keys():
        df_lims[clm] = df_lims[clm].astype(fields_dtypes[clm])
        
    return df_lims