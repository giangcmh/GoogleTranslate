import os
from google.cloud import translate_v2
import pyodbc
import pandas as pd
import warnings
from CommonFunction import CommonFunction

warnings.filterwarnings('ignore')

#################################################################
# init credentials google translate api
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\PythonProject\GoogleTranslate\elaborate-art-392802-e18eac2a2238.json"

# init translate language
translate_client = translate_v2.Client()

language = {
    2: "zh-CN",
    4: "th",
    6: "zh-TW",
    5: "vi"
}

#init database list
databases = ['AQ_Accommodation', 'AQ_CarRental', 'AQ_CMS', 'AQ_Dining', 'AQ_PrivateJet', 'AQ_Tour', 'AQ_Yacht']

for database in databases:
    # init connection SQL Server
    cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                          "Server=69.172.67.3,1400;"
                          f"Database={database};"
                          "uid=translate;"
                          "pwd=456wsx765&*;",
                          autocommit=True
                          )
