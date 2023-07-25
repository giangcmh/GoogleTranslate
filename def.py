import os
from google.cloud import translate_v2
import pyodbc
import pandas as pd
import warnings
from CommonFunction import CommonFunction

warnings.filterwarnings('ignore')

#################################################################
# declare database name and table name
databasename = 'AQ_IdentityBookingPortal'
# 'HelpTypeDedatails': ['HelpTypeFID', ['Name']]
normal_tables = {'HelpContentsDetails': ['HelpContentFID', ['Title','FullDescriptions']]

                 }
exception_tables = {
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
                      "Database=AQ_IdentityBookingPortal;"
                      "uid=translate;"
                      "pwd=456wsx765&*;",
                      autocommit=True
                      )

# init credentials google translate api
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\PythonProject\GoogleTranslate\elaborate-art-392802-e18eac2a2238.json"

# init translate language
translate_client = translate_v2.Client()

commonfunction = CommonFunction(databasename, normal_tables, exception_tables, language, cnxn, translate_client)

#################################################################  Get Last Translated Date
translate_date, exception_translate_date = commonfunction.get_last_translated_date()

# print(translate_date),
# print(exception_translate_date)

################################################################# Get dataframe

# print(normal_df.shape)
# print(exception_df.shape)

############################################################## Call Function Translate
normal_df, exception_df = commonfunction.call_api_translate()
# print(normal_df)
# print(exception_df)

################################################################# Insert dataframe into tables

cursor = cnxn.cursor()

helpcontentsdetails = normal_df[normal_df['table_name'] == 'HelpContentsDetails']
helptypededatails = normal_df[normal_df['table_name'] == 'HelpTypeDedatails']

# HelpContentsDetails
for index, row in helpcontentsdetails.iterrows():
    if row.is_insert == 1:
        cursor.execute(
            '''INSERT INTO [dbo].[HelpContentsDetails]([LanguageFID],
                                                        [HelpContentFID],
                                                        [Title],
                                                        [ShortDescriptions],
                                                        [FullDescriptions],
                                                        [Deleted],
                                                        [IsActived],
                                                        [CreatedBy],
                                                        [CreatedDate],
                                                        [LastModifiedBy],
                                                        [LastModifiedDate]) 
                VALUES (?,?,?,?,?,?,?,?,getdate(),?,getdate()-1)
            ''',
            row.translatefid,
            row.HelpContentFID,
            row.Title,
            row.ShortDescriptions,
            row.FullDescriptions,
            row.Deleted,
            row.IsActived,
            row.CreatedBy,
            row.LastModifiedBy
            )
    else:
        cursor.execute(
            ''' UPDATE [dbo].[HelpContentsDetails] 
                SET	    Title = ?, 
                        FullDescriptions = ?, 
                        LastModifiedDate = getdate()
                WHERE   HelpContentFID = ? and LanguageFID = ?
            ''',
            row.Title,
            row.FullDescriptions,
            row.HelpContentFID,
            row.translatefid
        )
        # update language en-US = getdate
        cursor.execute(
            ''' UPDATE [dbo].[HelpContentsDetails] 
                SET	    LastModifiedDate = getdate()
                WHERE   HelpContentFID = ? 
                and     LanguageFID = ?
            ''',
            row.HelpContentFID,
            1  # language en-US
        )

# HelpTypeDedatails
for index, row in helptypededatails.iterrows():
    if row.is_insert == 1:
        cursor.execute(
            '''INSERT INTO [dbo].[HelpTypeDedatails]([LanguageFID],
                                                    [HelpTypeFID],
                                                    [Name],
                                                    [Remarks],
                                                    [Deleted],
                                                    [IsActived],
                                                    [CreatedBy],
                                                    [CreatedDate],
                                                    [LastModifiedBy],
                                                    [LastModifiedDate]) 
                VALUES (?,?,?,?,?,?,?,getdate(),?,getdate()-1)
            ''',
            row.translatefid,
            row.HelpTypeFID,
            row.Name,
            row.Remarks,
            row.Deleted,
            row.IsActived,
            row.CreatedBy,
            row.LastModifiedBy
            )
    else:
        cursor.execute(
            ''' UPDATE [dbo].[HelpTypeDedatails] 
                SET	    Name = ?, 
                        LastModifiedDate = getdate()
                WHERE   HelpTypeFID = ? 
                and     LanguageFID = ?
            ''',
            row.Name,
            row.HelpTypeFID,
            row.translatefid
        )
        # update language en-US = getdate
        cursor.execute(
            ''' UPDATE [dbo].[HelpTypeDedatails] 
                SET	    LastModifiedDate = getdate()
                WHERE   HelpTypeFID = ? 
                and     LanguageFID = ?
            ''',
            row.HelpTypeFID,
            1  # language en-US
        )

# Insert tracking log into Translate_Tracking_Log
commonfunction.insert_tracking_log()
if normal_df.isnull == 'False' or exception_df.isnull == 'False':
    commonfunction.insert_tracking_row_and_word(normal_df, exception_df)

cnxn.commit()
cursor.close()


















