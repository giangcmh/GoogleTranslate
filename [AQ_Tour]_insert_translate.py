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

try:
    # init connection SQL Server
    cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                          "Server=69.172.67.3,1400;"
                          "Database=AQ_Tour;"
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

        ratingattributedetails = normal_df[normal_df['table_name'] == 'RatingAttributeDetails']
        tourcancelpolimutillang = normal_df[normal_df['table_name'] == 'TourCancelPoliMutilLang']
        tourpaymentpolimutillang = normal_df[normal_df['table_name'] == 'TourPaymentPoliMutilLang']
        tourpricingdetails = normal_df[normal_df['table_name'] == 'TourPricingDetails']
        tourtripplandetail = normal_df[normal_df['table_name'] == 'TourTripPlanDetail']
        tourtypedetail = normal_df[normal_df['table_name'] == 'TourTypeDetail']
        tourinformationdetails = exception_df[exception_df['table_name'] == 'TourInformationDetails']
        musementtourtranslation = translatecode_df[translatecode_df['table_name'] == 'MusementTourTranslation']

        # RatingAttributeDetails
        for index, row in ratingattributedetails.iterrows():
            # Insert language missing
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
                # update language missing
                cursor.execute(
                    ''' UPDATE [dbo].[RatingAttributeDetails] 
                        SET	    Name = ?, 
                                LastModifiedDate = getdate()
                        WHERE   RatingFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.Name,
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
                    1 # language en-US
                )


        # TourCancelPoliMutilLang
        for index, row in tourcancelpolimutillang.iterrows():
            if row.is_insert == 1:
                cursor.execute(
                    '''INSERT INTO [dbo].[TourCancelPoliMutilLang]([CancellationPoliciFID],
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
                    ''' UPDATE [dbo].[TourCancelPoliMutilLang] 
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
                    ''' UPDATE [dbo].[TourCancelPoliMutilLang] 
                        SET	    LastModifiedDate = getdate()
                        WHERE   CancellationPoliciFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.CancellationPoliciFID,
                    1  # language en-US
                )


        # TourPaymentPoliMutilLang
        for index, row in tourpaymentpolimutillang.iterrows():
            if row.is_insert == 1:
                cursor.execute(
                    '''INSERT INTO [dbo].[TourPaymentPoliMutilLang]([PaymentPoliciFID],
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
                    ''' UPDATE [dbo].[TourPaymentPoliMutilLang] 
                        SET	    Name = ?, 
                                TermAndPolicies = ?, 
                                LastModifiedDate = getdate()
                        WHERE   PaymentPoliciFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.Name,
                    row.TermAndPolicies,
                    row.PaymentPoliciFID,
                    row.translatefid
                )
                # update language en-US = getdate
                cursor.execute(
                    ''' UPDATE [dbo].[TourPaymentPoliMutilLang] 
                        SET	    LastModifiedDate = getdate()
                        WHERE   PaymentPoliciFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.PaymentPoliciFID,
                    1  # language en-US
                )


        # TourPricingDetails
        for index, row in tourpricingdetails.iterrows():
            if row.is_insert == 1:
                cursor.execute(
                    '''INSERT INTO [dbo].[TourPricingDetails]([PricingFID],
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
                    row.PricingFID,
                    row.translatefid,
                    row.Name,
                    row.Remark,
                    row.Deleted,
                    row.CreatedBy,
                    row.LastModifiedBy
                    )
            else:
                cursor.execute(
                    ''' UPDATE [dbo].[TourPricingDetails] 
                        SET	    Name = ?, 
                                Remark = ?, 
                                LastModifiedDate = getdate()
                        WHERE   PricingFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.Name,
                    row.Remark,
                    row.PricingFID,
                    row.translatefid
                )
                # update language en-US = getdate
                cursor.execute(
                    ''' UPDATE [dbo].[TourPricingDetails] 
                        SET	    LastModifiedDate = getdate()
                        WHERE   PricingFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.PricingFID,
                    1  # language en-US
                )


        # TourTripPlanDetail
        for index, row in tourtripplandetail.iterrows():
            if row.is_insert == 1:
                cursor.execute(
                    '''INSERT INTO [dbo].[TourTripPlanDetail]([TripPlanFID],
                                                                [ItemName],
                                                                [ItemDescription],
                                                                [LanguageFID],
                                                                [Deleted],
                                                                [CreatedBy],
                                                                [CreatedDate],
                                                                [LastModifiedBy],
                                                                [LastModifiedDate],
                                                                [IsActivated]) 
                        VALUES (?,?,?,?,?,?,getdate(),?,getdate()-1,?)
                    ''',
                    row.TripPlanFID,
                    row.ItemName,
                    row.ItemDescription,
                    row.translatefid,
                    row.Deleted,
                    row.CreatedBy,
                    row.LastModifiedBy,
                    row.IsActivated
                    )
            else:
                cursor.execute(
                    ''' UPDATE [dbo].[TourTripPlanDetail] 
                        SET	    ItemName = ?, 
                                ItemDescription = ?, 
                                LastModifiedDate = getdate()
                        WHERE   TripPlanFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.ItemName,
                    row.ItemDescription,
                    row.TripPlanFID,
                    row.translatefid
                )
                # update language en-US = getdate
                cursor.execute(
                    ''' UPDATE [dbo].[TourTripPlanDetail] 
                        SET	    LastModifiedDate = getdate()
                        WHERE   TripPlanFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.TripPlanFID,
                    1  # language en-US
                )


        # TourTypeDetail
        for index, row in tourtypedetail.iterrows():
            if row.is_insert == 1:
                cursor.execute(
                    '''INSERT INTO [dbo].[TourTypeDetail]([TourTypeFID],
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
                    row.TourTypeFID,
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
                    ''' UPDATE [dbo].[TourTypeDetail] 
                        SET	    Name = ?, 
                                LastModifiedDate = getdate()
                        WHERE   TourTypeFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.Name,
                    row.TourTypeFID,
                    row.translatefid
                )
                # update language en-US = getdate
                cursor.execute(
                    ''' UPDATE [dbo].[TourTypeDetail] 
                        SET	    LastModifiedDate = getdate()
                        WHERE   TourTypeFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.TourTypeFID,
                    1  # language en-US
                )


        #exception table
        # TourInformationDetails
        for index, row in tourinformationdetails.iterrows():
            if row.is_insert == 1:
                cursor.execute(
                    '''INSERT INTO [dbo].[TourInformationDetails]([UniqueID],
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
                    ''' UPDATE [dbo].[TourInformationDetails] 
                        SET	    Title = ?, 
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
                    ''' UPDATE [dbo].[TourInformationDetails] 
                        SET	    LastModifiedDate = getdate()
                        WHERE   InformationFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.InformationFID,
                    1  # language en-US
                )

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
    except:
        cursor = cnxn.cursor()
        cursor.execute(
            f'''INSERT INTO [AQ_Configurations].[dbo].[Translate_Tracking_Status_Log]([Database_Name],
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
            f'''INSERT INTO [AQ_Configurations].[dbo].[Translate_Tracking_Status_Log]([Database_Name],
                                                                                                        [Status],
                                                                                                        [CreatedDate])
                                        VALUES (?,?,getdate())
                                    ''',
            databasename,
            'Success'
        )
        cnxn.commit()
        cursor.close()