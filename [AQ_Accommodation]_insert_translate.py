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
                         "Database=AQ_Accommodation;"
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
databasename = 'AQ_Accommodation'
normal_tables = {'RatingAttributeDetails': ['PostCategoryFID', ['Name']],
                 'RoomAmenityAttributeDetails': ['PostCategoryFID', ['Name']],
                 'HotelSandboxMutilLang': ['PostCategoryFID', ['Name']],
                 'HotelAmenityAttributeDetails': ['PostCategoryFID', ['Name']],
                 'HotelAdditionalServiceDetails': ['PostCategoryFID', ['Name']]
                 }
exception_tables = {'HotelInformationDetails': ['PostCategoryFID', ['Name']]
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

ratingattributedetails = normal_df[normal_df['table_name'] == 'RatingAttributeDetails']
roomamenityattributedetails = normal_df[normal_df['table_name'] == 'RoomAmenityAttributeDetails']
hotelsandboxmutillang = normal_df[normal_df['table_name'] == 'HotelSandboxMutilLang']
hotelamenityattributedetails = normal_df[normal_df['table_name'] == 'HotelAmenityAttributeDetails']
hoteladditionalservicedetails = normal_df[normal_df['table_name'] == 'HotelAdditionalServiceDetails']
hotelinformationdetails = exception_df[exception_df['table_name'] == 'HotelInformationDetails']

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
            row.Description_Translated,
            row.Remark,
            row.IsActive,
            row.Deleted,
            row.CreatedBy,
            row.LastModifiedBy
            )
    else:
        cursor.execute(
            ''' UPDATE  [dbo].[RatingAttributeDetails] 
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



# RoomAmenityAttributeDetails
for index, row in roomamenityattributedetails.iterrows():
    if row.is_insert == 1:
        cursor.execute(
            '''INSERT INTO [dbo].[RoomAmenityAttributeDetails]([RoomAmenitiFID],
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
            row.RoomAmenitiFID,
            row.translatefid,
            row.Description_Translated,
            row.Remark,
            row.IsActive,
            row.Deleted,
            row.CreatedBy,
            row.LastModifiedBy
            )
    else:
        cursor.execute(
            ''' UPDATE  [dbo].[RoomAmenityAttributeDetails] 
                SET	    Name = ?, 
                        LastModifiedDate = getdate() 
                WHERE   RoomAmenitiFID = ? 
                and     LanguageFID = ?
            ''',
            row.Description_Translated,
            row.RoomAmenitiFID,
            row.translatefid
        )
        # update language en-US = getdate
        cursor.execute(
            ''' UPDATE [dbo].[RoomAmenityAttributeDetails] 
                SET	    LastModifiedDate = getdate()
                WHERE   RoomAmenitiFID = ? 
                and     LanguageFID = ?
            ''',
            row.RoomAmenitiFID,
            1  # language en-US
        )



# HotelSandboxMutilLang
for index, row in hotelsandboxmutillang.iterrows():
    if row.is_insert == 1:
        cursor.execute(
            '''INSERT INTO [dbo].[HotelSandboxMutilLang]([SanboxFID],
                                                            [LanguageFID],
                                                            [SanboxName],
                                                            [SanboxDescription],
                                                            [IsActivated],
                                                            [Deleted],
                                                            [CreatedBy],
                                                            [CreatedDate],
                                                            [LastModifiedBy],
                                                            [LastModifiedDate],
                                                            [SandboxPolicies],
                                                            [SanboxShortDescription]) 
                VALUES (?,?,?,?,?,?,?,getdate(),?,getdate()-1,?,?)
            ''',
            row.SanboxFID,
            row.translatefid,
            row.SanboxName,
            row.Description_Translated,
            row.IsActivated,
            row.Deleted,
            row.CreatedBy,
            row.LastModifiedBy,
            row.Policies_Translated,
            row.ShortDescription_Translated
            )
    else:
        cursor.execute(
            ''' UPDATE  [dbo].[HotelSandboxMutilLang] 
                SET     SanboxDescription = ?, 
                        SandboxPolicies = ? ,
                        SanboxShortDescription = ?, 
                        LastModifiedDate = getdate()   
                WHERE SanboxFID = ? 
                and LanguageFID = ?
            ''',
            row.Description_Translated,
            row.Policies_Translated,
            row.ShortDescription_Translated,
            row.SanboxFID,
            row.translatefid
        )
        # update language en-US = getdate
        cursor.execute(
            ''' UPDATE [dbo].[HotelSandboxMutilLang] 
                SET	    LastModifiedDate = getdate()
                WHERE   SanboxFID = ? 
                and     LanguageFID = ?
            ''',
            row.SanboxFID,
            1  # language en-US
        )



# HotelAmenityAttributeDetails
for index, row in hotelamenityattributedetails.iterrows():
    if row.is_insert == 1:
        cursor.execute(
            '''INSERT INTO [dbo].[HotelAmenityAttributeDetails]([HotelAmenitiFID],
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
            row.HotelAmenitiFID,
            row.translatefid,
            row.Description_Translated,
            row.Remark,
            row.IsActive,
            row.Deleted,
            row.CreatedBy,
            row.LastModifiedBy
            )
    else:
        cursor.execute(
            ''' UPDATE [dbo].[HotelAmenityAttributeDetails] 
                SET	    Name = ?, 
                        LastModifiedDate = getdate()
                WHERE   HotelAmenitiFID = ? 
                and LanguageFID = ?
            ''',
            row.Description_Translated,
            row.HotelAmenitiFID,
            row.translatefid
        )
        # update language en-US = getdate
        cursor.execute(
            ''' UPDATE [dbo].[HotelAmenityAttributeDetails] 
                SET	    LastModifiedDate = getdate()
                WHERE   HotelAmenitiFID = ? 
                and     LanguageFID = ?
            ''',
            row.HotelAmenitiFID,
            1  # language en-US
        )



# HotelAdditionalServiceDetails
for index, row in hoteladditionalservicedetails.iterrows():
    if row.is_insert == 1:
        cursor.execute(
            '''INSERT INTO [dbo].[HotelAdditionalServiceDetails]([AdditionalServiceFID],
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
            row.Description_Translated,
            row.Remark,
            row.Deleted,
            row.CreatedBy,
            row.LastModifiedBy
            )
    else:
        cursor.execute(
            ''' UPDATE  [dbo].[HotelAdditionalServiceDetails] 
                SET     Name = ?, 
                        LastModifiedDate = getdate()
                WHERE   AdditionalServiceFID = ? 
                and     LanguageFID = ?
            ''',
            row.Description_Translated,
            row.AdditionalServiceFID,
            row.translatefid
        )
        # update language en-US = getdate
        cursor.execute(
            ''' UPDATE [dbo].[HotelAdditionalServiceDetails] 
                SET	    LastModifiedDate = getdate()
                WHERE   AdditionalServiceFID = ? 
                and     LanguageFID = ?
            ''',
            row.AdditionalServiceFID,
            1  # language en-US
        )


# hotelinformationdetails
for index, row in hotelinformationdetails.iterrows():
    if row.is_insert == 1:
        cursor.execute(
            '''INSERT INTO [dbo].[HotelInformationDetails]([UniqueID],
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
            row.ShortDescription_Translated,
            row.Description_Translated,
            row.Deleted,
            row.IsActivated,
            row.ActivatedBy,
            row.LastModifiedBy
            )
    else:
        cursor.execute(
            ''' UPDATE [dbo].[HotelInformationDetails] 
                SET	    Title = ?, 
                        ShortDescriptions = ?, 
                        FullDescriptions = ?,
                        LastModifiedDate = getdate()
                WHERE   InformationFID = ? 
                and     LanguageFID = ?
            ''',
            row.Title_Translated,
            row.ShortDescription_Translated,
            row.Description_Translated,
            row.InformationFID,
            row.translatefid
        )
        # update language en-US = getdate
        cursor.execute(
            ''' UPDATE [dbo].[HotelInformationDetails] 
                SET	    LastModifiedDate = getdate()
                WHERE   InformationFID = ? 
                and     LanguageFID = ?
            ''',
            row.InformationFID,
            1  # language en-US
        )

# Insert tracking log into Translate_Tracking_Log
commonfunction.insert_tracking_log()
commonfunction.insert_tracking_row_and_word()

cnxn.commit()
cursor.close()
