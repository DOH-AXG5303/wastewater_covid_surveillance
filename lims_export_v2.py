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
                         'CollectionWaterTemp': 'collection_water_temp',
                         'CollectionStorageTime': 'collection_storage_time',
                         'CollectionStorageTemp': 'collection_storage_temp',
                         'Pretreatment': 'pretreatment',
                         'PretreatmentSpecify': 'pretreatment_specify',
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
             'sars_cov2_avg_conc',
             'collection_storage_time',
             'pre_ext_storage_time',
             'pre_conc_storage_time',
             'rec_eff_percent',
            ]


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