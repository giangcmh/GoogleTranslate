import os
from google.cloud import translate_v2
import pyodbc
import pandas as pd
import warnings
from CommonFunction import CommonFunction

warnings.filterwarnings('ignore')

#################################################################
# declare database name and table name
databasename = 'AQ_CarRental'
normal_tables = {'CarAdditionalServiceDetails': ['AdditionalServiceFID', ['Name']],
                 'CarCancelPoliMutilLang': ['CancellationPoliciFID', ['Name','TermAndPolicies']],
                 'CarPaymentPoliMutilLang': ['PaymentPoliciFID', ['Name','TermAndPolicies']],
                 'RatingAttributeDetails': ['RatingFID', ['Name']]
                 }
exception_tables = {'CarInformationDetails': ['InformationFID', ['Title', 'FullDescriptions', 'ShortDescriptions']],
                    }
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
                             "Database=AQ_CarRental;"
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

        caradditionalservicedetails = normal_df[normal_df['table_name'] == 'CarAdditionalServiceDetails']
        carcancelpolimutillang = normal_df[normal_df['table_name'] == 'CarCancelPoliMutilLang']
        carpaymentpolimutillang = normal_df[normal_df['table_name'] == 'CarPaymentPoliMutilLang']
        ratingattributedetails = normal_df[normal_df['table_name'] == 'RatingAttributeDetails']
        carinformationdetails = exception_df[exception_df['table_name'] == 'CarInformationDetails']

        # CarAdditionalServiceDetails
        for index, row in caradditionalservicedetails.iterrows():
            if row.is_insert == 1:
                cursor.execute(
                    '''INSERT INTO [dbo].[CarAdditionalServiceDetails]([AdditionalServiceFID],
                                                                        [LanguageFID],
                                                                        [Name],
                                                                        [Remark],
                                                                        [Deleted],
                                                                        [CreatedBy],
                                                                        [CreatedDate],
                                                                        [LastModifiedBy],
                                                                        [LastModifiedDate]) 
                        VALUES (?,?,?,?,?,?,getdate(),?,getdate()-1)
                    ''',
                    row.AdditionalServiceFID,
                    row.translatefid,
                    row.Name,
                    row.Remark,
                    row.Deleted,
                    row.CreatedBy,
                    row.LastModifiedBy
                    )
            else:
                cursor.execute(
                    ''' UPDATE  [dbo].[CarAdditionalServiceDetails] 
                        SET	    Name = ?, 
                                LastModifiedDate = getdate()  
                        WHERE AdditionalServiceFID = ? and LanguageFID = ?
                    ''',
                    row.Name,
                    row.AdditionalServiceFID,
                    row.translatefid
                )
                # update language en-US = getdate
                cursor.execute(
                    ''' UPDATE [dbo].[CarAdditionalServiceDetails] 
                        SET	    LastModifiedDate = getdate()
                        WHERE   AdditionalServiceFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.AdditionalServiceFID,
                    1  # language en-US
                )


        # CarCancelPoliMutilLang
        for index, row in carcancelpolimutillang.iterrows():
            if row.is_insert == 1:
                cursor.execute(
                    '''INSERT INTO [dbo].[CarCancelPoliMutilLang]([CancellationPoliciFID],
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
                    row.translatefid,
                    row.Name,
                    row.TermAndPolicies,
                    row.IsActive,
                    row.Deleted,
                    row.CreatedBy,
                    row.LastModifiedBy
                    )
            else:
                cursor.execute(
                    ''' UPDATE [dbo].[CarCancelPoliMutilLang] 
                        SET	    Name = ?, 
                                TermAndPolicies = ?, 
                                LastModifiedDate = getdate()
                        WHERE   CancellationPoliciFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.Name,
                    row.TermAndPolicies,
                    row.CancellationPoliciFID,
                    row.translatefid
                )
                # update language en-US = getdate
                cursor.execute(
                    ''' UPDATE [dbo].[CarCancelPoliMutilLang] 
                        SET	    LastModifiedDate = getdate()
                        WHERE   CancellationPoliciFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.CancellationPoliciFID,
                    1  # language en-US
                )


        # CarPaymentPoliMutilLang
        for index, row in carpaymentpolimutillang.iterrows():
            if row.is_insert == 1:
                cursor.execute(
                    '''INSERT INTO [dbo].[CarPaymentPoliMutilLang]([PaymentPoliciFID],
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
                    row.PaymentPoliciFID,
                    row.translatefid,
                    row.Name,
                    row.TermAndPolicies,
                    row.IsActive,
                    row.Deleted,
                    row.CreatedBy,
                    row.LastModifiedBy
                    )
            else:
                cursor.execute(
                    ''' UPDATE [dbo].[CarPaymentPoliMutilLang] 
                        SET	    Name = ?, 
                                TermAndPolicies = ?, 
                                LastModifiedDate = getdate() 
                        WHERE   PaymentPoliciFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.Description_Translated,
                    row.TermAndPolicies_Translated,
                    row.PaymentPoliciFID,
                    row.translatefid
                )
                # update language en-US = getdate
                cursor.execute(
                    ''' UPDATE [dbo].[CarPaymentPoliMutilLang] 
                        SET	    LastModifiedDate = getdate()
                        WHERE   PaymentPoliciFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.PaymentPoliciFID,
                    1  # language en-US
                )


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
                    row.translatefid,
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
                        and     LanguageFID = ?
                    ''',
                    row.Description_Translated,
                    row.RatingFID,
                    row.translatefid
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



        # CarInformationDetails
        for index, row in carinformationdetails.iterrows():
            if row.is_insert == 1:
                cursor.execute(
                    '''INSERT INTO [dbo].[CarInformationDetails]([UniqueID],
                                                                [InformationFID],
                                                                [LanguageFID],
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
                        VALUES (?,?,?,?,?,?,?,?,?,?,getdate(),?,?,getdate()-1)
                    ''',
                    row.UniqueID,
                    row.InformationFID,
                    row.translatefid,
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
                    ''' UPDATE [dbo].[CarInformationDetails] 
                        SET     Title = ?, 
                                ShortDescriptions = ?, 
                                FullDescriptions = ?, 
                                LastModifiedDate = getdate() 
                        WHERE   InformationFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.Title,
                    row.ShortDescriptions,
                    row.FullDescriptions,
                    row.InformationFID,
                    row.translatefid
                )
                # update language en-US = getdate
                cursor.execute(
                    ''' UPDATE [dbo].[CarInformationDetails] 
                        SET	    LastModifiedDate = getdate()
                        WHERE   InformationFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.InformationFID,
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