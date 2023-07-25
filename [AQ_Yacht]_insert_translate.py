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
databasename = 'AQ_Yacht'
normal_tables = {'RatingAttributeDetails': ['RatingFID', ['Name']],
                 'YachtAdditionalServiceDetails': ['AdditionalServiceFID', ['Name']],
                 'YachtCancelPoliMutilLang': ['CancellationPoliciFID', ['Name']],
                 'YachtPaymentPoliMutilLang': ['PaymentPoliciFID', ['Name']],
                 'YachtTourCategoryInfomations': ['TourCategoryFID', ['Name']]
                 }
exception_tables = {'YachtInformationDetails': ['InformationFID', ['Name']],
                    'YachtMerchantInformationDetails': ['InformationFID', ['Name']],
                    'YachtTourInformationDetails': ['InformationFID', ['Name']]
                    }
#exception table
yachtroutemultilanguages = pd.DataFrame()

try:
    # init connection SQL Server
    cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                          "Server=69.172.67.3,1400;"
                          "Database=AQ_Yacht;"
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

        language = {
            2: "zh-CN",
            4: "th",
            6: "zh-TW",
            5: "vi"
        }





        commonfunction = CommonFunction(databasename,normal_tables, exception_tables, language, cnxn, translate_client)

        #################################################################  Get Last Translated Date
        translate_date, exception_translate_date = commonfunction.get_last_translated_date()

        # print(translate_date),
        # print(exception_translate_date)

        # YachtRouteMultiLanguages # no fields LastModifiedDate so can not update
        for key, value in language.items():
            ###Insert / 1 as is_insert
            yachtroutemultilanguages_insert = pd.read_sql(
                f'''select	1 as is_insert,
                            {key} as translatefid,
                            '{value}' as language_translate,
                            * 
                    from	[dbo].[YachtRouteMultiLanguages] A 
                    where	LanguageFID = 1 
                    and     not exists (select 1 
                                        from [dbo].[YachtRouteMultiLanguages] B 
                                        where A.YachtRouteFID = B.YachtRouteFID 
                                        and B.LanguageFID = {key}) 
        
                '''
                , cnxn, index_col='ID')
            yachtroutemultilanguages = pd.concat([yachtroutemultilanguages, yachtroutemultilanguages_insert], ignore_index=True)

        ################################################################# Call Function Translate
        normal_df, exception_df = commonfunction.call_api_translate()
        # print(normal_df)
        # print(exception_df)

        # YachtRouteMultiLanguages
        yachtroutemultilanguages_RouteName_Translated = []

        for index, row in yachtroutemultilanguages.iterrows():
            language_translate = row['language_translate']
            text_routename = row['RouteName']

            # process NULL data in column
            if text_routename == None:
                yachtroutemultilanguages_RouteName_Translated.append(text_routename)
            else:
                output_description = translate_client.translate(text_routename, target_language=language_translate)
                yachtroutemultilanguages_RouteName_Translated.append(output_description['translatedText'])

        # add 1 new columns translated
        yachtroutemultilanguages['RouteName_Translated'] = yachtroutemultilanguages_RouteName_Translated



        ################################################################# Insert dataframe into tables
        cursor = cnxn.cursor()
        ratingattributedetails = normal_df[normal_df['table_name'] == 'RatingAttributeDetails']
        yachtadditionalservicedetails = normal_df[normal_df['table_name'] == 'YachtAdditionalServiceDetails']
        yachtcancelpolimutillang = normal_df[normal_df['table_name'] == 'YachtCancelPoliMutilLang']
        yachtpaymentpolimutillang = normal_df[normal_df['table_name'] == 'YachtPaymentPoliMutilLang']
        yachttourcategoryinfomations = normal_df[normal_df['table_name'] == 'YachtTourCategoryInfomations']

        #exception table
        yachtinformationdetails = exception_df[exception_df['table_name'] == 'YachtInformationDetails']
        yachtmerchantinformationdetails = exception_df[exception_df['table_name'] == 'YachtMerchantInformationDetails']
        yachttourinformationdetails = exception_df[exception_df['table_name'] == 'YachtTourInformationDetails']

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
                    row.Name_Translated,
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
                    row.Name_Translated,
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



        # YachtAdditionalServiceDetails
        for index, row in yachtadditionalservicedetails.iterrows():
            if row.is_insert == 1:
                cursor.execute(
                    '''INSERT INTO [dbo].[YachtAdditionalServiceDetails]([AdditionalServiceFID],
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
                    row.Name_Translated,
                    row.Remark,
                    row.Deleted,
                    row.CreatedBy,
                    row.LastModifiedBy
                    )
            else:
                cursor.execute(
                    ''' UPDATE [dbo].[YachtAdditionalServiceDetails] 
                        SET	    Name = ?, 
                                LastModifiedDate = getdate()    
                        WHERE   AdditionalServiceFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.Name_Translated,
                    row.AdditionalServiceFID,
                    row.translatefid
                )
                # update language en-US = getdate
                cursor.execute(
                    ''' UPDATE [dbo].[YachtAdditionalServiceDetails] 
                        SET	    LastModifiedDate = getdate()
                        WHERE   AdditionalServiceFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.AdditionalServiceFID,
                    1  # language en-US
                )


        # YachtCancelPoliMutilLang
        for index, row in yachtcancelpolimutillang.iterrows():
            if row.is_insert == 1:
                cursor.execute(
                    '''INSERT INTO [dbo].[YachtCancelPoliMutilLang]([CancellationPoliciFID],
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
                    ''' UPDATE [dbo].[YachtCancelPoliMutilLang] 
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
                    ''' UPDATE [dbo].[YachtCancelPoliMutilLang] 
                        SET	    LastModifiedDate = getdate()
                        WHERE   CancellationPoliciFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.CancellationPoliciFID,
                    1  # language en-US
                )


        # YachtPaymentPoliMutilLang
        for index, row in yachtpaymentpolimutillang.iterrows():
            if row.is_insert == 1:
                cursor.execute(
                    '''INSERT INTO [dbo].[YachtPaymentPoliMutilLang]([PaymentPoliciFID],
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
                    ''' UPDATE [dbo].[YachtPaymentPoliMutilLang] 
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
                    ''' UPDATE [dbo].[YachtPaymentPoliMutilLang] 
                        SET	    LastModifiedDate = getdate()
                        WHERE   PaymentPoliciFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.PaymentPoliciFID,
                    1  # language en-US
                )


        # YachtTourCategoryInfomations
        for index, row in yachttourcategoryinfomations.iterrows():
            if row.is_insert == 1:
                cursor.execute(
                    '''INSERT INTO [dbo].[YachtTourCategoryInfomations]([TourCategoryFID],
                                                                        [TourCategoryResourceKey],
                                                                        [LanguageFID],
                                                                        [ShortDescription],
                                                                        [FullDescription],
                                                                        [UsingSpecialImage],
                                                                        [FileTypeFID],
                                                                        [FileStreamFID],
                                                                        [EffectiveDate],
                                                                        [Deleted],
                                                                        [CreatedBy],
                                                                        [CreatedDate],
                                                                        [LastModifiedBy],
                                                                        [LastModifiedDate]) 
                        VALUES (?,?,?,?,?,?,?,?,getdate(),?,?,getdate(),?,getdate()-1)
                    ''',
                    row.TourCategoryFID,
                    row.TourCategoryResourceKey,
                    row.translatefid,
                    row.ShortDescriptions_Translated,
                    row.FullDescription,
                    row.UsingSpecialImage,
                    row.FileTypeFID,
                    row.FileStreamFID,
                    row.Deleted,
                    row.CreatedBy,
                    row.LastModifiedBy
                    )
            else:
                cursor.execute(
                    ''' UPDATE [dbo].[YachtTourCategoryInfomations] 
                        SET	    ShortDescription = ?, 
                                LastModifiedDate = getdate() 
                        WHERE   TourCategoryFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.ShortDescriptions_Translated,
                    row.TourCategoryFID,
                    row.translatefid
                )
                # update language en-US = getdate
                cursor.execute(
                    ''' UPDATE [dbo].[YachtTourCategoryInfomations] 
                        SET	    LastModifiedDate = getdate()
                        WHERE   TourCategoryFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.TourCategoryFID,
                    1  # language en-US
                )


        # YachtInformationDetails
        for index, row in yachtinformationdetails.iterrows():
            if row.is_insert == 1:
                cursor.execute(
                    '''INSERT INTO [dbo].[YachtInformationDetails]([UniqueID],  
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
                    ''' UPDATE [dbo].[YachtInformationDetails] 
                        SET	    Title = ?, 
                                ShortDescriptions = ?, 
                                FullDescriptions = ?, 
                                LastModifiedDate = getdate() 
                        WHERE   InformationFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.Title_Translated,
                    row.ShortDescriptions_Translated,
                    row.FullDescriptions_Translated,
                    row.InformationFID,
                    row.translatefid
                )
                # update language en-US = getdate
                cursor.execute(
                    ''' UPDATE [dbo].[YachtInformationDetails] 
                        SET	    LastModifiedDate = getdate()
                        WHERE   InformationFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.InformationFID,
                    1  # language en-US
                )


        # YachtMerchantInformationDetails
        for index, row in yachtmerchantinformationdetails.iterrows():
            if row.is_insert == 1:
                cursor.execute(
                    '''INSERT INTO [dbo].[YachtMerchantInformationDetails]([UniqueID],
                                                                            [InformationFID],
                                                                            [HaveFileStream],
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
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,getdate(),?,?,getdate()-1)
                    ''',
                    row.UniqueID,
                    row.InformationFID,
                    row.HaveFileStream,
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
                    ''' UPDATE [dbo].[YachtMerchantInformationDetails] 
                        SET	    Title = ?, 
                                ShortDescriptions = ?, 
                                FullDescriptions = ?, 
                                LastModifiedDate = getdate() 
                        WHERE   InformationFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.Title_Translated,
                    row.ShortDescriptions_Translated,
                    row.FullDescriptions_Translated,
                    row.InformationFID,
                    row.translatefid
                )
                # update language en-US = getdate
                cursor.execute(
                    ''' UPDATE [dbo].[YachtMerchantInformationDetails] 
                        SET	    LastModifiedDate = getdate()
                        WHERE   InformationFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.InformationFID,
                    1  # language en-US
                )


        # YachtTourInformationDetails
        for index, row in yachttourinformationdetails.iterrows():
            if row.is_insert == 1:
                cursor.execute(
                    '''INSERT INTO [dbo].[YachtTourInformationDetails]([UniqueID],
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
                    ''' UPDATE [dbo].[YachtTourInformationDetails] 
                        SET	    Title = ?, 
                                ShortDescriptions = ?, 
                                FullDescriptions = ?, 
                                LastModifiedDate = getdate() 
                        WHERE InformationFID = ? and LanguageFID = ?
                    ''',
                    row.Title_Translated,
                    row.ShortDescriptions_Translated,
                    row.FullDescriptions_Translated,
                    row.InformationFID,
                    row.translatefid
                )
                # update language en-US = getdate
                cursor.execute(
                    ''' UPDATE [dbo].[YachtTourInformationDetails] 
                        SET	    LastModifiedDate = getdate()
                        WHERE   InformationFID = ? 
                        and     LanguageFID = ?
                    ''',
                    row.InformationFID,
                    1  # language en-US
                )


        # YachtRouteMultiLanguages
        # only insert
        for index, row in yachtroutemultilanguages.iterrows():
            cursor.execute(
                '''INSERT INTO [dbo].[YachtRouteMultiLanguages]([YachtRouteFID],
                                                                [LanguageFID],
                                                                [RouteName],
                                                                [Remark]) 
                    VALUES(?,?,?,?)
                ''',
                row.YachtRouteFID,
                row.translatefid,
                row.RouteName_Translated,
                row.Remark
                )

        # Insert tracking log into Translate_Tracking_Log
        commonfunction.insert_tracking_log()
        commonfunction.insert_tracking_row_and_word()

        #For yachtroutemultilanguages
        if yachtroutemultilanguages.empty == False:
            cursor.execute(
                f'''INSERT INTO [AQ_Configurations].[dbo].[Translate_Tracking_Log]([Database_Name],
                                                                                            [Table_Name],
                                                                                            [LastTranlastedDate]) 
                            VALUES (?,?,getdate())
                        ''',
                databasename,
                "YachtRouteMultiLanguages"
            )

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