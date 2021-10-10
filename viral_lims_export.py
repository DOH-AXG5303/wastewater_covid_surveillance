import pyodbc
import pandas as pd
import requests
import io
from lims_login import credentials
from lims_login import redcap_tokens_prod
from lims_login import redcap_api_url

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

#Columns that need to be numeric: currently functional
numeric_clms_easy = [
                 'tot_conc_vol',
                 'equiv_sewage_amt',
                 'collection_storage_temp',
                 'flow_rate',
                 'collection_water_temp',
                 'rec_eff_percent',
                 'ph',
                 'pre_conc_storage_time',
                 'tss',
                 'pre_ext_storage_time',
                 'collection_storage_time']

numeric_clms_challenging = [
                             'sars_cov2_avg_conc',
                             'sars_cov2_std_error',
                             'sars_cov2_cl_95_lo',
                             'sars_cov2_cl_95_up',
                             'lod_sewage']


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

def transform_lims_dataframe(df_lims):
    """
    Transform lims dataframe in prep for redcap import.
    Transform column names, set index, rename columns, convert select columns to numeric with select conditions. 
    args:
        df_lims = raw dataframe from LIMS
    return:
        df_lims = transformed dataframe
    """
    df_lims = df_lims.copy()
    df_lims.set_index("SubmitterSampleNumber", inplace = True) #set index
    df_lims.index.name = "sample_id" #rename index
    df_lims = df_lims[list(dict_lims_column_map.keys())].copy() #keep only columns that will go to redcap
    df_lims.rename(columns = dict_lims_column_map, inplace = True) #rename the columns on the way to redcap
    
    #converting columns dtypes (select columns)
    df_lims[numeric_clms_easy] = df_lims[numeric_clms_easy].apply(pd.to_numeric)
    df_lims[numeric_clms_challenging] = df_lims[numeric_clms_challenging].apply(pd.to_numeric, errors = "coerce")
    
    return df_lims