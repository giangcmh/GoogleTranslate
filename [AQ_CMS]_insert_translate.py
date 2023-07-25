import os
from google.cloud import translate_v2
import pyodbc
import pandas as pd
import warnings
from CommonFunction import CommonFunction

warnings.filterwarnings('ignore')

#################################################################
# declare database name and table name
databasename = 'AQ_CMS'
normal_tables = {'PostCategoryDetails': ['PostCategoryFID',['Name']]}
exception_tables = {'PostDetails': ['PostFId',['Title','Body','ShortDescription']]}
language = {
    2: "zh-CN",
    4: "th",
    6: "zh-TW",
    5: "vi"
}

# init connection SQL Server
try:
    cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                             "Server=69.172.67.3,1400;"
                             "Database=AQ_CMS;"
                             "uid=translate;"
                             "pwd=456wsx765&*;",
                             autocommit=True
                             )
except pyodbc.Error as ex:
    sqlstate = ex.args[1]
    print(sqlstate)
else:
    try:
        # init credentials google translate api
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\PythonProject\GoogleTranslate\elaborate-art-392802-e18eac2a2238.json"

        # init translate language
        translate_client = translate_v2.Client()

        commonfunction = CommonFunction(databasename,normal_tables, exception_tables, language, cnxn, translate_client)

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

        postcategorydetails = normal_df[normal_df['table_name'] == 'PostCategoryDetails']
        postdetails = exception_df[exception_df['table_name'] == 'PostDetails']

        # PostCategoryDetails
        for index, row in postcategorydetails.iterrows():
            if row.is_insert == 1:
                cursor.execute(
                    '''INSERT INTO [dbo].[PostCategoryDetails]([PostCategoryFID],
                                                                [Name],
                                                                [LanguageFID],
                                                                [Deleted],
                                                                [IsActivated],
                                                                [CreatedBy],
                                                                [CreatedDate],
                                                                [LastModifiedBy],
                                                                [LastModifiedDate],
                                                                [FriendlyUrl])
                        VALUES (?,?,?,?,?,?,getdate(),?,getdate()-1,?)
                    ''',
                    row.PostCategoryFID,
                    row.Description_Translated,
                    row.translatefid,
                    row.Deleted,
                    row.IsActivated,
                    row.CreatedBy,
                    row.LastModifiedBy,
                    row.FriendlyUrl
                    )
            else:
                cursor.execute(
                    ''' UPDATE [dbo].[PostCategoryDetails]
                        SET	    Name = ?,
                                LastModifiedDate = getdate()
                        WHERE   PostCategoryFID = ?
                        and     LanguageFID = ?
                    ''',
                    row.Description_Translated,
                    row.PostCategoryFID,
                    row.translatefid
                )
                # update language en-US = getdate
                cursor.execute(
                    ''' UPDATE [dbo].[PostCategoryDetails]
                        SET	    LastModifiedDate = getdate()
                        WHERE   PostCategoryFID = ?
                        and     LanguageFID = ?
                    ''',
                    row.PostCategoryFID,
                    1  # language en-US
                )

        # PostDetails
        for index, row in postdetails.iterrows():
            if row.is_insert == 1:
                cursor.execute(
                    '''INSERT INTO [dbo].[PostDetails]([UniqueId],
                                                        [PostFId],
                                                        [LanguageFID],
                                                        [FileTypeFID],
                                                        [FileStreamFID],
                                                        [Title],
                                                        [MetaDescription],
                                                        [Body],
                                                        [KeyWord],
                                                        [Deleted],
                                                        [IsActivated],
                                                        [ActivatedDate],
                                                        [ActivatedBy],
                                                        [LastModifiedBy],
                                                        [LastModifiedDate],
                                                        [ShortDescription],
                                                        [FriendlyUrl]) 
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,getdate(),?,?,getdate()-1,?,?)
                    ''',
                    row.UniqueId,
                    row.PostFId,
                    row.translatefid,
                    row.FileTypeFID,
                    row.FileStreamFID,
                    row.Title_Translated,
                    row.MetaDescription_Translated,
                    row.Body_Translated,
                    row.KeyWord,
                    row.Deleted,
                    row.IsActivated,
                    row.ActivatedBy,
                    row.LastModifiedBy,
                    row.ShortDescription_Translated,
                    row.FriendlyUrl
                    )
            else:
                cursor.execute(
                    ''' UPDATE [dbo].[PostDetails] 
                        SET	    Title = ?, 
                                MetaDescription = ?, 
                                Body = ?, 
                                ShortDescription = ?, 
                                LastModifiedDate = getdate()  
                        WHERE   PostFId = ? 
                        and     LanguageFID = ?
                    ''',
                    row.Title_Translated,
                    row.MetaDescription_Translated,
                    row.Body_Translated,
                    row.ShortDescription_Translated,
                    row.PostFId,
                    row.translatefid
                )
                # update language en-US = getdate
                cursor.execute(
                    ''' UPDATE [dbo].[PostDetails] 
                        SET	    LastModifiedDate = getdate()
                        WHERE   PostFId = ? 
                        and     LanguageFID = ?
                    ''',
                    row.PostFId,
                    1  # language en-US
                )

        # Insert tracking log into Translate_Tracking_Log
        commonfunction.insert_tracking_log()
        commonfunction.insert_tracking_row_and_word()

        cnxn.commit()
        cursor.close()
    except:
        cursor = cnxn.cursor()
        cursor.execute(
            f'''INSERT INTO [AQ_Configurations].[dbo].[Translate_Tracking_RowAndWord_Log]([Database_Name],
                                                                                                    [Status],
                                                                                                    [CreatedDate])
                                    VALUES (?,?,getdate())
                                ''',
            databasename,
            'Fail'
        )
        cnxn.commit()
        cursor.close()
    else:
        cursor = cnxn.cursor()
        cursor.execute(
            f'''INSERT INTO [AQ_Configurations].[dbo].[Translate_Tracking_RowAndWord_Log]([Database_Name],
                                                                                                        [Status],
                                                                                                        [CreatedDate])
                                        VALUES (?,?,getdate())
                                    ''',
            databasename,
            'Success'
        )
        cnxn.commit()
        cursor.close()