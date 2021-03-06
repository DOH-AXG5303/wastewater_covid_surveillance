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
                         'PreExtStorageTime': 'pre_ext_storage_time', # #all null values in LIMS (05/11/2022)
                         'PreExtStorageTemp': 'pre_ext_storage_temp',
                         'TotConcVol': 'tot_conc_vol',
                         'QualityFlag': 'quality_flag',
                         'PCRTarget':'pcr_target',
                         'SubmitterSampleNumber': 'sample_id',}

#Columns that are numeric in LIMS, also numeric in REDCap
numeric_clms= [
             'collection_water_temp',
             'collection_storage_temp',
             'equiv_sewage_amt',
             'flow_rate',
             'sars_cov2_std_error',
             'sars_cov2_cl_95_lo',
             'sars_cov2_cl_95_up',
             'pre_ext_storage_temp', # #all null values in LIMS (05/11/2022)
             'pre_ext_storage_time', # #all null values in LIMS (05/11/2022)
             'pre_conc_storage_time',
             'rec_eff_percent',
             'sars_cov2_avg_conc', #added to numeric
             'pretreatment_specify', # #all null values in LIMS (05/11/2022)
            ]

# special field that include both numeric and text - extract only numeric part, ex: "500ml", "7 hours"
text_to_numeric =  [
                     'tot_conc_vol',
                     'collection_storage_time',
                    ]

#fields that must be in time format: "HH:MM"
time_format = ["sample_collect_time"]

#the followeing fields must be in all lowercase for consistent import to Redcap.
#Mostly yes/no values. Lims sometimes has entries such as YES/Yes/yes, or NO/No/no 
lowercase_fields = [
                 'quality_flag',
                 'inhibition_adjust',
                 'ntc_amplify',
                 'pretreatment',
                 'inhibition_detect',
                 'sars_cov2_below_lod',
                    ]

#standard yes/no conversion from LIMS to REDCap format, only the following values will remain
yes_no_map = {'yes': 'yes',
              'no': 'no',
              'not_tested': 'not_tested'}

#Converting choice fields from LIMS format to REDCap format
choice_fields = {
                 'quality_flag': yes_no_map,
                 'inhibition_adjust': yes_no_map,
                 'ntc_amplify': yes_no_map,
                 'inhibition_detect': yes_no_map,
                 'sars_cov2_below_lod': yes_no_map,
                 'pretreatment': {"yes":1,
                                  "no":0},
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

def isolate_relevant_data(df_lims):
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

def long_to_wide(df_lims):
    """
    Convert the long form: same sample ID for PCRTargets: N1 and N2, into wide form: each critical value: 'SARSCoV2AvgConc','SARSCoV2BelowLOD' 
    will have a column for N1_critical_value, and N2_critical_value. This will allow unique sample ID's in REDCap
    """
    
    df_lims = df_lims.copy()

    df_lims = df_lims.sort_values('test_result_date').drop_duplicates(subset = ["sample_id", "pcr_target"], keep = "last") #drop duplicates if both the same PCR target was tested more than once per sample iD, keep last
    df_lims = df_lims[df_lims['pcr_target'].isin(["N1","N2"])] #select only N1 and N2

    df_pivot = df_lims.pivot(index = "sample_id", columns = "pcr_target") #values = ['sars_cov2_below_lod', 'sars_cov2_avg_conc']

    #separate wide transformed dataframe intwo two parts - critical values that are dependant on PCR_target, and everything else
    df_pivot_critical = df_pivot[['sars_cov2_below_lod', 'sars_cov2_avg_conc']].copy()
    df_pivot_remaining = df_pivot[df_pivot.columns.get_level_values(0).difference(['sars_cov2_below_lod', 'sars_cov2_avg_conc'])].copy()

    #merge multi-index columns for critical fields
    new_cols = ['{1}_{0}'.format(*tup) for tup in df_pivot_critical.columns]
    df_pivot_critical.columns = [x.lower() for x in 
                                 new_cols]

    ### Drop all N2 columns
    df_pivot_remaining = df_pivot_remaining.drop("N2", axis = 1, level = 1)
    df_pivot_remaining.columns = df_pivot_remaining.columns.droplevel(1)
    
    #merge previously separated dataframes
    df_wide = df_pivot_critical.join(df_pivot_remaining)
    
    return df_wide

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

def standardize_time_fields(df_lims):
    """
    time fields must be in format HH:MM. If value do not match standard format, change to None
    """
    
    df_lims = df_lims.copy()
    
    for i in time_format:
        #mask where format does NOT match the HH:MM standard
        false_format = ~df_lims[i].str.match(r"^[0-9][0-9]\:[0-9][0-9]$", na=False)
        #set to None if format does not match
        df_lims.loc[false_format, i] = None
    
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


if __name__ == "__main__":
    
    ### Generate Log file ####
    logging.basicConfig(filename= "lims_export.log", level = logging.DEBUG,
                    format='%(asctime)s:%(levelname)s:%(message)s')
    
    ### Export LIMS and isolate relevant data ###
    df_lims = (
        export_df_from_LIMS()
        .pipe(isolate_relevant_data)
        )
    logging.debug("LIMS export complete, raw data shape: {}".format(df_lims.shape))
    
    ### Oreder independant transformations ###
    df_lims = (
        convert_numeric(df_lims)
        .pipe(freetext_transform)
        .pipe(convert_choice_fields)
        .pipe(standardize_time_fields)
        )
    logging.debug("validation transform complete, data shape: {}".format(df_lims.shape))
    
    ### Critical convert long to wide ####
    df_lims = (
        long_to_wide(df_lims)
        .pipe(force_values)
        .pipe(set_dtypes)
        )
    logging.debug("long-to-wide transform complete, data shape: {}".format(df_lims.shape))
    
    #Import to REDCap
    project = redcap.Project(redcap_api_url, redcap_tokens_prod["PID171"])
    response = project.import_records(df_lims, import_format = "df", force_auto_number=False)
    logging.debug("Import to REDCap complete: {}".format(response))
    