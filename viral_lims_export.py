import pyodbc
import pandas as pd
from lims_login import credentials

cnxn = pyodbc.connect(credentials) # credentials = 'DSN=LIMS_DATA;UID=xxxxxxx;PWD=xxxxxxx'
sql_query = pd.read_sql_query('SELECT * FROM [vz_Epi_ELS_SARS-CoV-2 ddPCR]',cnxn)

######## Alternate way to pull SQL data object ##########
#cursor = cnxn.cursor()
#cursor.execute('SELECT * FROM [vz_Epi_ELS_SARS-CoV-2 ddPCR]')

print(sql_query.info())