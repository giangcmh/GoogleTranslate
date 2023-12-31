import os
from google.cloud import translate_v2
import pyodbc
import pandas as pd
import warnings
from CommonFunction import CommonFunction

warnings.filterwarnings('ignore')

#################################################################
# declare database name and table name
databasename = 'AQ_Dining'
normal_tables = {'RatingAttributeDetails': ['RatingFID', ['Name']],
                 'RestaurantCancelPoliMutilLang': ['CancellationPoliciFID', ['Name','TermAndPolicies']]
                 }
exception_tables = {'RestaurantInformationDetails': ['InformationFID', ['Title', 'FullDescriptions', 'ShortDescriptions']],
                    'RestaurantOtherInformations': ['RestaurantFID', ['Title','Descriptions']]
                    }
language = {
    2: "zh-CN",
    4: "th",
    6: "zh-TW",
    5: "vi"
}

try:
    # init connection SQL Server
    cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                          "Server=69.172.67.3,1400;"
                          "Database=AQ_Dining;"
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
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\PythonProject\GoogleTranslate\aqtranslate_key_new.json"



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

        ##################################################### Insert dataframe into tables
        cursor = cnxn.cursor()

        ratingattributedetails = normal_df[normal_df['table_name'] == 'RatingAttributeDetails']
        restaurantcancelpolimutillang = normal_df[normal_df['table_name'] == 'RestaurantCancelPoliMutilLang']
        restaurantinformationdetails = exception_df[exception_df['table_name'] == 'RestaurantInformationDetails']
        restaurantotherinformations = exception_df[exception_df['table_name'] == 'RestaurantOtherInformations']

        # RatingAttributeDetails
        for index, row in ratingattributedetails.iterrows():
            if row.is_insert == 1:
                cursor.execute(
                    '''INSERT INTO [dbo].[RatingAttributeDetails]([RatingFID],
                                                                    [LanguageFID],
                                                                    [Name],
                                                                    [Remark],
                                                                    [IsActive],
                                                                    [Deleted],
                                                                    [CreatedBy],
                                                                    [CreatedDate],
                                                                    [LastModifiedBy],
                                                                    [LastModifiedDate]) 
                        VALUES (?,?,?,?,?,?,?,getdate(),?,getdate()-1)
                    ''',
                    row.RatingFID,
                    row.language_translate,
                    row.Name,
                    row.Remark,
                    row.IsActive,
                    row.Deleted,
                    row.CreatedBy,
                    row.LastModifiedBy
                )
            else:
                cursor.execute(
                    ''' UPDATE [dbo].[RatingAttributeDetails] 
                        SET	    Name = ?, 
                                LastModifiedDate = getdate() 
                        WHERE   RatingFID = ? 
                        and LanguageFID = ?
                    ''',
                    row.Name,
                    row.RatingFID,
                    row.language_translate
                )
                # update language en-US = getdate
                cursor.execute(
                    ''' UPDATE [dbo].[RatingAttributeDetails]
                        SET	    LastModifiedDate = getdate()
                        WHERE   RatingFID = ?
                        and     LanguageFID = ?
                    ''',
                    row.RatingFID,
                    1  # language en-US
                )

        # RestaurantCancelPoliMutilLang
        for index, row in restaurantcancelpolimutillang.iterrows():
            if row.is_insert == 1:
                cursor.execute(
                    '''INSERT INTO [dbo].[RestaurantCancelPoliMutilLang]([CancellationPoliciFID],
                                                                        [LanguageFID],
                                                                        [Name],
                                                                        [TermAndPolicies],
                                                                        [IsActive],
                                                                        [Deleted],
                                                                        [CreatedBy],
                                                                        [CreatedDate],
                                                                        [LastModifiedBy],
                                                                        [LastModifiedDate]) 
                        VALUES (?,?,?,?,?,?,?,getdate(),?,getdate()-1)
                    ''',
                    row.CancellationPoliciFID,
                    row.language_translate,
                    row.Name,
                    row.TermAndPolicies,
                    row.IsActive,
                    row.Deleted,
                    row.CreatedBy,
                    row.LastModifiedBy
                )
            else:
                cursor.execute(
                    ''' UPDATE [dbo].[RestaurantCancelPoliMutilLang] 
                        SET	    Name = ?, 
                                TermAndPolicies = ?, 
                                LastModifiedDate = getdate()
                        WHERE   CancellationPoliciFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.Name,
                    row.TermAndPolicies,
                    row.CancellationPoliciFID,
                    row.language_translate
                )
                # update language en-US = getdate
                cursor.execute(
                    ''' UPDATE [dbo].[RestaurantCancelPoliMutilLang]
                        SET	    LastModifiedDate = getdate()
                        WHERE   CancellationPoliciFID = ?
                        and     LanguageFID = ?
                    ''',
                    row.CancellationPoliciFID,
                    1  # language en-US
                )

        # RestaurantInformationDetails
        for index, row in restaurantinformationdetails.iterrows():
            if row.is_insert == 1:
                cursor.execute(
                    '''INSERT INTO [dbo].[RestaurantInformationDetails]([UniqueID],
                                                                        [InformationFID],
                                                                        [LanguageFID],
                                                                        [LanguageResKey],
                                                                        [FileTypeFID],
                                                                        [FileStreamFID],
                                                                        [Title],
                                                                        [ShortDescriptions],
                                                                        [FullDescriptions],
                                                                        [Deleted],
                                                                        [IsActivated],
                                                                        [ActivatedDate],
                                                                        [ActivatedBy],
                                                                        [LastModifiedBy],
                                                                        [LastModifiedDate]) 
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,getdate(),?,?,getdate()-1)
                    ''',
                    row.UniqueID,
                    row.InformationFID,
                    row.language_translate,
                    row.LanguageResKey,
                    row.FileTypeFID,
                    row.FileStreamFID,
                    row.Title,
                    row.ShortDescriptions,
                    row.FullDescriptions,
                    row.Deleted,
                    row.IsActivated,
                    row.ActivatedBy,
                    row.LastModifiedBy
                )
            else:
                cursor.execute(
                    ''' UPDATE [dbo].[RestaurantInformationDetails] 
                        SET     Title = ?, 
                                ShortDescriptions = ?, 
                                FullDescriptions = ?, 
                                LastModifiedDate = getdate()
                        WHERE   InformationFID = ? 
                        and LanguageFID = ?
                    ''',
                    row.Title,
                    row.ShortDescriptions,
                    row.FullDescriptions,
                    row.InformationFID,
                    row.language_translate
                )
                # update language en-US = getdate
                cursor.execute(
                    ''' UPDATE [dbo].[RestaurantInformationDetails]
                        SET	    LastModifiedDate = getdate()
                        WHERE   InformationFID = ?
                        and     LanguageFID = ?
                    ''',
                    row.InformationFID,
                    1  # language en-US
                )

        # RestaurantOtherInformations
        for index, row in restaurantotherinformations.iterrows():
            if row.is_insert == 1:
                cursor.execute(
                    '''INSERT INTO [dbo].[RestaurantOtherInformations]([RestaurantFID],
                                                                        [UniqueID],
                                                                        [InfoTypeFID],
                                                                        [LanguageFID],
                                                                        [LanguageResKey],
                                                                        [FileTypeFID],
                                                                        [FileStreamFID],
                                                                        [Title],
                                                                        [Descriptions],
                                                                        [Deleted],
                                                                        [IsActivated],
                                                                        [ActivatedDate],
                                                                        [ActivatedBy],
                                                                        [CreatedBy],
                                                                        [CreatedDate],
                                                                        [LastModifiedBy],
                                                                        [LastModifiedDate]) 
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,getdate(),?,?,getdate(),?,getdate()-1)
                    ''',
                    row.RestaurantFID,
                    row.UniqueID,
                    row.InfoTypeFID,
                    row.language_translate,
                    row.LanguageResKey,
                    row.FileTypeFID,
                    row.FileStreamFID,
                    row.Title,
                    row.Descriptions,
                    row.Deleted,
                    row.IsActivated,
                    row.ActivatedBy,
                    row.CreatedBy,
                    row.LastModifiedBy
                )
            else:
                cursor.execute(
                    ''' UPDATE [dbo].[RestaurantOtherInformations] 
                        SET     Title = ?, 
                                FullDescriptions = ?, 
                                LastModifiedDate = getdate() 
                        WHERE   RestaurantFID = ? 
                        and LanguageFID = ?
                    ''',
                    row.Title,
                    row.Descriptions,
                    row.RestaurantFID,
                    row.language_translate
                )
                # update language en-US = getdate
                cursor.execute(
                    ''' UPDATE [dbo].[RestaurantOtherInformations]
                        SET	    LastModifiedDate = getdate()
                        WHERE   RestaurantFID = ?
                        and     LanguageFID = ?
                    ''',
                    row.RestaurantFID,
                    1  # language en-US
                )

        # Insert tracking log into Translate_Tracking_Log
        if normal_df.isnull == 'False' or exception_df.isnull == 'False':
            commonfunction.insert_tracking_row_and_word(normal_df, exception_df)

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