import pyodbc
import pandas as pd
import requests
import io
from lims_login import credentials
from lims_login import redcap_tokens_prod
from lims_login import redcap_api_url


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
    
    df = pd.read_csv(io.StringIO(r.content.decode("utf-8")))

    return df