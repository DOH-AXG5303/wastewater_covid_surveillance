import pyodbc
import pandas as pd
import numpy as np
import requests
import io
import re
from lims_login import credentials
from lims_login import redcap_tokens_prod
from lims_login import redcap_api_url

# {"LIMS Value": "REDCap Value"}
dict_lims_column_map = {
                         'SampleCollectDate': 'sample_collect_date',
                         'SampleCollectTime': 'sample_collect_time',
                         'pH': 'ph',
                         'Conductivity': 'conductivity',
                         'CollectionWaterTemp': 'collection_water_temp',
                         'TSS': 'tss',
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
                         'RecEffSpikeConc': 'rec_eff_percent',
                         'InhibitionDetect': 'inhibition_detect',
                         'InhibitionAdjust': 'inhibition_adjust',
                         'ConcentrationMethod': 'concentration_method',
                         'ExtractionMethod': 'extraction_method',
                         'PreConcStorageTime': 'pre_conc_storage_time',
                         'PreConcStorageTemp': 'pre_conc_storage_temp',
                         'PreExtStorageTime': 'pre_ext_storage_time',
                         'PreExtStorageTemp': 'pre_ext_storage_temp',
                         'TotConcVol': 'tot_conc_vol',
                         'QualityFlag': 'quality_flag'}

#Columns that are numeric in LIMS, also numeric in REDCap
numeric_clms= [
             'ph',
             'conductivity',
             'collection_water_temp',
             'tss',
             'collection_storage_temp',
             'equiv_sewage_amt',
             'flow_rate',
             'sars_cov2_std_error',
             'sars_cov2_cl_95_lo',
             'sars_cov2_cl_95_up',
             'pre_ext_storage_temp',
             'sars_cov2_avg_conc'
            ]

#Columns that are text in LIMS, converted to numeric in REDCap 
text_to_numeric =  [
                     'pretreatment_specify',
                     'pre_conc_storage_time',
                     'pre_ext_storage_time',
                     'rec_eff_percent',
                     'collection_storage_time',
                     'tot_conc_vol'
                    ]

#LIMS columns that have drop down choice

yes_no_clms = [
                 'quality_flag',
                 'inhibition_adjust',
                 'ntc_amplify',
                 'pretreatment',
                 'inhibition_detect',
                 'sars_cov2_below_lod'
               ]

choice_clms = [
                 'sars_cov2_units'
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

def redcap_API_export(url,token):
    """
    all data API request
    """

    fields = {
        'token': token,
        'content': 'record',
        'format': 'csv',
        'type': 'flat'}

    r = requests.post(url, data=fields)
    
    df = pd.read_csv(io.StringIO(r.content.decode("utf-8")), index_col=0)

    return df

def redcap_metadata_export(url,token):
    """
    all metadata API request
    """
    
    #payload info to import metadata
    fields = {
        'token': token,
        'content': 'metadata',
        'format': 'csv',
        'returnFormat': "csv"}

    #request metadata and convert to dataframe
    r = requests.post(url, data=fields)
    df = pd.read_csv(io.StringIO(r.content.decode("utf-8")), index_col=0)

    return df


def project_dtype_summary(redcap_api_url, redcap_tokens_prod):
    """
    import all projects as a dict of dataframes from a redcap token dictionary
    
    args:
        redcap_api_url: url to redcap environment
        redcap_tokens_prod: dictionary with tokens {"PID#":token}
    return:
        dictionary {"PID#": dataframe}
    """
    
    data_dict = {key: redcap_API_export(redcap_api_url, value) for key, value in redcap_tokens_prod.items()}
    
    return data_dict


def drop_null_sample_ID(df_lims):
    """
    drop rows where 'SubmitterSampleNumber' is null
    Convert 'SubmitterSampleNumber' to int64 (instead of float)
    """
    df_lims = df_lims.copy()
    
    df_lims['SubmitterSampleNumber'] = pd.to_numeric(df_lims['SubmitterSampleNumber'], errors = "coerce")
    to_drop = df_lims[df_lims['SubmitterSampleNumber'].isnull()].index
    df_lims.drop(index = to_drop, inplace = True)
    df_lims['SubmitterSampleNumber'] = df_lims['SubmitterSampleNumber'].astype(np.int64)
    
    return df_lims 
    

def rename_lims_columns(df_lims):
    """
    Transform column names, set index, rename columns
    args:
        df_lims = raw dataframe from LIMS
    return:
        df_lims = datframe with new column names
    """
    df_lims = df_lims.copy()
    df_lims.set_index("SubmitterSampleNumber", inplace = True) #set index
    df_lims.index.name = "sample_id" #rename index
    df_lims = df_lims[list(dict_lims_column_map.keys())].copy() #keep only columns that will go to redcap
    df_lims.rename(columns = dict_lims_column_map, inplace = True) #rename the columns on the way to redcap
    
    return df_lims

def verify_time_field(df_lims):
    """
    Change time field "sample_collect_time" in prep for import to REDCap
    
    args:
        Dataframe, must contain "sample_collect_time" as field
    return:
        Dataframe
    """
    df_lims = df_lims.copy()
    collect_time = df_lims["sample_collect_time"].reset_index()

    for i in collect_time.index:

        a_1 = collect_time.loc[i,"sample_collect_time"]
        if a_1 is None:
            pass
        elif re.search(r"^[0-9][0-9]\:[0-9][0-9]$", a_1):
            pass
        else:
            collect_time.loc[i,"sample_collect_time"] = None
            
    to_return = collect_time.set_index("sample_id", drop = True).copy()
    df_lims["sample_collect_time"] = to_return["sample_collect_time"]
    
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
    A set of columns is in format of open text - commonly filled with digits followed by numbers. 
    Regex split values to numeric and text, replace value to the numeric portion. 
    Convert entire column to numeric, coerce errors. 
    """
    
    df_lims = df_lims.copy()

    for i in text_to_numeric:
        series = df_lims[i].str.extract(r"([0-9\.]+)([A-Za-z ]+)?")[0]
        df_lims[i] = series

    df_lims[text_to_numeric] = df_lims[text_to_numeric].apply(pd.to_numeric, errors = "coerce")

    return df_lims


# def convert_lims_dtypes(df_lims):
    
#     #Do I have to convert dtype to enter into redcap?
    
#     #converting columns dtypes (select columns)
#     df_lims = df_lims.copy()
#     df_lims[numeric_clms_easy] = df_lims[numeric_clms_easy].apply(pd.to_numeric)
#     df_lims[numeric_clms_challenging] = df_lims[numeric_clms_challenging].apply(pd.to_numeric, errors = "ignore") #neet to change to "coerce"
    
#     return df_lims


def accepted_redcap_fields(df):
    """
    create dictionary of accepted values in restricted fields
    
    args:
        df - metadata dataframe
    return:
        dictionary {"Field ID": {"accepted input":"redcap displayed longform value"}}
    """
    
    # dataframe Column "select_choices_or_calculations" contains metadata about accepted values
    # dataframe rows are Redcap field names (redcap columns)
    # Need to filter only the redcap columns (dataframe rows) that have value constraints

    desired_column = df["select_choices_or_calculations"] #data of interest
    empty = df["select_choices_or_calculations"].isnull() #mask for empty values
    non_blank = desired_column[~empty] # selecting all that are not empty
    mask = non_blank.str.contains(r"\|") # mask, must contain " | "
    fields_with_choices = non_blank[mask].copy()  #real field choices are separated by "|", remove rows where "|" is not present in string


    #Extracting possible choices for values from dataframe and converting into a dictionary
    fields_dict = {}
    for i in fields_with_choices.iteritems():
        string_to_process = i[1]

        list_to_process = string_to_process.split(" | ") #list of strings with key, value pairs separated by ", " within each list
        keys_values_list = [i.split(", ", 1) for i in list_to_process]
        values_dict = {t[0]:t[1] for t in keys_values_list}

        fields_dict[i[0]] =  values_dict
        
    return fields_dict


def date_time_redcap_fields(df):
    """
    create dictionary of date and time fields in a redcap project
    
    args:
       redcap_api_url - url to redcap API
       redcap_tokens_prod - token for REDCap project
    return:
        Dataframe - field names and corresponding date/time values
    
    """
    #df = redcap_metadata_export(redcap_api_url, redcap_tokens_prod)
    df = df.copy()

    df_text_type = df[["text_validation_type_or_show_slider_number"]].copy()

    date_mask = df_text_type["text_validation_type_or_show_slider_number"].str.contains("date")
    time_mask = df_text_type["text_validation_type_or_show_slider_number"].str.contains("time")

    df_date_time = df_text_type[date_mask | time_mask].copy()

    return df_date_time
