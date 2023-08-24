import os
from google.cloud import translate_v2
import pyodbc
import pandas as pd
import warnings
from CommonFunction import CommonFunction

warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)  # show all columns in pandas dataframe

#################################################################
# declare database name and table name
databasename = 'AQ_Tour'
normal_tables = {
                 }
exception_tables = {
                    }

translatecode_tables = {'MusementTourTranslation': ['MusementTourID', ['Title', 'Description', 'About', 'MeetingPoint']]
                        }

translate_language_code = {
    'zh-CN': 'zh-CN',
    'th-TH': 'th',
    'vi-VN': 'vi',
    'zh-HK': 'zh-TW'
}

language = {
    2: "zh-CN",
    4: "th",
    6: "zh-TW",
    5: "vi"
}


# init connection SQL Server
cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                      "Server=69.172.67.3,1400;"
                      "Database=AQ_Tour;"
                      "uid=translate;"
                      "pwd=456wsx765&*;",
                      autocommit=True
                      )

# init credentials google translate api
os.environ[
    'GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\PythonProject\GoogleTranslate\elaborate-art-392802-e18eac2a2238.json"

# init translate language
translate_client = translate_v2.Client()

commonfunction = CommonFunction(databasename,normal_tables, exception_tables, language, cnxn, translate_client, translatecode_tables, translate_language_code)

#################################################################  Get Last Translated Date
translate_date, exception_translate_date = commonfunction.get_last_translated_date()

# print(translate_date),
# print(exception_translate_date)

################################################################# Get dataframe

# print(normal_df.shape)
# print(exception_df.shape)

############################################################## Call Function Translate
normal_df, exception_df, translatecode_df = commonfunction.call_api_translate()
# print(normal_df)
# print(exception_df)

################################################################# Insert dataframe into tables
cursor = cnxn.cursor()


musementtourtranslation = translatecode_df[translatecode_df['table_name'] == 'MusementTourTranslation']



# MusementTourTranslation
for index, row in musementtourtranslation.iterrows():
    # Insert language missing
    cursor.execute(
        '''INSERT INTO [dbo].[MusementTourTranslation]([MusementTourId]
                                                        ,[LanguageCode]
                                                        ,[Title]
                                                        ,[Description]
                                                        ,[About]
                                                        ,[MeetingPoint])
            VALUES (?,?,?,?,?,?)
        ''',
        row.MusementTourId,
        row.db_translatecode,
        row.Title,
        row.Description,
        row.About,
        row.MeetingPoint
    )

# Insert tracking log into Translate_Tracking_Log
if normal_df.isnull == 'False' or exception_df.isnull == 'False' or translatecode_df.isnull == 'False':
    commonfunction.insert_tracking_row_and_word(normal_df, exception_df, translatecode_df)

cnxn.commit()
cursor.close()
