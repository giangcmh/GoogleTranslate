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

# init connection SQL Server
cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                         "Server=69.172.67.3,1400;"
                         "Database=AQ_PrivateJet;"
                         "uid=translate;"
                         "pwd=456wsx765&*;",
                         autocommit=True
                         )

# init translate language
translate_client = translate_v2.Client()

language = {
    2: "zh-CN",
    4: "th",
    6: "zh-TW",
    5: "vi"
}

# declare database name and table name
databasename = 'AQ_PrivateJet'
normal_tables = {'FlightCancelPoliMutilLang': ['CancellationPoliciFID', ['Name', 'TermAndPolicies']],
                 'FlightPaymentPoliMutilLang': ['PaymentPoliciFID', ['Name', 'TermAndPolicies']]
                 }
exception_tables = {'FlightInformationDetails': ['InformationFID', ['Title', 'ShortDescriptions', 'FullDescriptions']]
                    }
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

flightcancelpolimutillang = normal_df[normal_df['table_name'] == 'FlightCancelPoliMutilLang']
flightpaymentpolimutillang = normal_df[normal_df['table_name'] == 'FlightPaymentPoliMutilLang']
flightinformationdetails = exception_df[exception_df['table_name'] == 'FlightInformationDetails']

# FlightCancelPoliMutilLang
for index, row in flightcancelpolimutillang.iterrows():
    if row.is_insert == 1:
        cursor.execute(
            '''INSERT INTO [dbo].[FlightCancelPoliMutilLang]([CancellationPoliciFID],
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
            row.Name_Translated,
            row.Term_Translated,
            row.IsActive,
            row.Deleted,
            row.CreatedBy,
            row.LastModifiedBy
            )
    else:
        cursor.execute(
            ''' UPDATE [dbo].[FlightCancelPoliMutilLang] 
                SET	    Name = ?, 
                        TermAndPolicies = ?, 
                        LastModifiedDate = getdate()  
                WHERE   CancellationPoliciFID = ? 
                and     LanguageFID = ?
            ''',
            row.Name_Translated,
            row.Term_Translated,
            row.CancellationPoliciFID,
            row.translatefid
        )
        # update language en-US = getdate
        cursor.execute(
            ''' UPDATE [dbo].[FlightCancelPoliMutilLang] 
                SET	    LastModifiedDate = getdate()
                WHERE   CancellationPoliciFID = ? 
                and     LanguageFID = ?
            ''',
            row.CancellationPoliciFID,
            1  # language en-US
        )


# FlightPaymentPoliMutilLang
for index, row in flightpaymentpolimutillang.iterrows():
    if row.is_insert == 1:
        cursor.execute(
            '''INSERT INTO [dbo].[FlightPaymentPoliMutilLang]([PaymentPoliciFID],
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
            row.Name_Translated,
            row.Term_Translated,
            row.IsActive,
            row.Deleted,
            row.CreatedBy,
            row.LastModifiedBy
            )
    else:
        cursor.execute(
            ''' UPDATE [dbo].[FlightPaymentPoliMutilLang] 
                SET	    Name = ?, 
                        TermAndPolicies = ?, 
                        LastModifiedDate = getdate() 
                WHERE   PaymentPoliciFID = ? 
                and     LanguageFID = ?
            ''',
            row.Name_Translated,
            row.Term_Translated,
            row.PaymentPoliciFID,
            row.translatefid
        )
        # update language en-US = getdate
        cursor.execute(
            ''' UPDATE [dbo].[FlightPaymentPoliMutilLang] 
                SET	    LastModifiedDate = getdate()
                WHERE   PaymentPoliciFID = ? 
                and     LanguageFID = ?
            ''',
            row.PaymentPoliciFID,
            1  # language en-US
        )


# FlightInformationDetails
for index, row in flightinformationdetails.iterrows():
    if row.is_insert == 1:
        cursor.execute(
            '''INSERT INTO [dbo].[FlightInformationDetails]([UniqueID],
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
            row.Title_Translated,
            row.ShortDescriptions_Translated,
            row.FullDescriptions_Translated,
            row.Deleted,
            row.IsActivated,
            row.ActivatedBy,
            row.LastModifiedBy
            )
    else:
        cursor.execute(
            ''' UPDATE [dbo].[FlightInformationDetails] 
                SET	    Title = ?, 
                        ShortDescriptions = ?, 
                        FullDescriptions = ?, 
                        LastModifiedDate = getdate()
                WHERE   InformationFID = ? and LanguageFID = ?
            ''',
            row.Title_Translated,
            row.ShortDescriptions_Translated,
            row.FullDescriptions_Translated,
            row.InformationFID,
            row.translatefid
        )
        # update language en-US = getdate
        cursor.execute(
            ''' UPDATE [dbo].[FlightInformationDetails] 
                SET	    LastModifiedDate = getdate()
                WHERE   InformationFID = ? 
                and     LanguageFID = ?
            ''',
            row.InformationFID,
            1  # language en-US
        )

# Insert tracking log into Translate_Tracking_Log
commonfunction.insert_tracking_log()

cnxn.commit()
cursor.close()
