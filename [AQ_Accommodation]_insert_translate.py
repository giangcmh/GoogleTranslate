import os
from google.cloud import translate_v2
import pyodbc
import pandas as pd
import warnings
from CommonFunction import CommonFunction

warnings.filterwarnings('ignore')

#################################################################
# declare database name and table name
databasename = 'AQ_Accommodation'


normal_tables = {'RatingAttributeDetails': ['RatingFID', ['Name']],
                 'RoomAmenityAttributeDetails': ['RoomAmenitiFID', ['Name']],
                 'HotelSandboxMutilLang': ['SanboxFID', ['SanboxDescription', 'SandboxPolicies', 'SanboxShortDescription']],
                 'HotelAmenityAttributeDetails': ['HotelAmenitiFID', ['Name']],
                 'HotelAdditionalServiceDetails': ['AdditionalServiceFID', ['Name']],
                 'HotelInventoryDescriptions': ['InventoryId', ['Description']]
                 }
exception_tables = {'HotelInformationDetails': ['InformationFID', ['Title','ShortDescriptions','FullDescriptions']]
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
                                 "Database=AQ_Accommodation;"
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

        ratingattributedetails = normal_df[normal_df['table_name'] == 'RatingAttributeDetails']
        roomamenityattributedetails = normal_df[normal_df['table_name'] == 'RoomAmenityAttributeDetails']
        hotelsandboxmutillang = normal_df[normal_df['table_name'] == 'HotelSandboxMutilLang']
        hotelamenityattributedetails = normal_df[normal_df['table_name'] == 'HotelAmenityAttributeDetails']
        hoteladditionalservicedetails = normal_df[normal_df['table_name'] == 'HotelAdditionalServiceDetails']
        hotelinformationdetails = exception_df[exception_df['table_name'] == 'HotelInformationDetails']
        hotelinventorydescriptions = normal_df[normal_df['table_name'] == 'HotelInventoryDescriptions']

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
                    ''' UPDATE  [dbo].[RatingAttributeDetails] 
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
                    row.Name,
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
                    row.Name,
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
                    row.SanboxDescription,
                    row.IsActivated,
                    row.Deleted,
                    row.CreatedBy,
                    row.LastModifiedBy,
                    row.SandboxPolicies,
                    row.SanboxShortDescription
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
                    row.SanboxDescription,
                    row.SandboxPolicies,
                    row.SanboxShortDescription,
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
                    row.Name,
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
                    row.Name,
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
                    row.Name,
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
                    row.Name,
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

        #HotelInventoryDescriptions
        for index, row in hotelinventorydescriptions.iterrows():
            if row.is_insert == 1:
                cursor.execute(
                    '''INSERT INTO [dbo].[HotelInventoryDescriptions]([InventoryId],
                                                                [LanguageFID],
                                                                [Name],
                                                                [Description],
                                                                [Deleted],
                                                                [CreatedDate],
                                                                [CreatedBy],
                                                                [LastModifiedDate],
                                                                [LastModifiedBy])
                        VALUES (?,?,?,?,?,getdate(),?,getdate()-1,?)
                    ''',
                    row.InventoryId,
                    row.translatefid,
                    row.Name,
                    row.Description,
                    row.Deleted,
                    row.CreatedBy,
                    row.LastModifiedBy
                    )
            else:
                cursor.execute(
                    ''' UPDATE  [dbo].[HotelInventoryDescriptions] 
                        SET     Description = ?, 
                                LastModifiedDate = getdate()
                        WHERE   InventoryId = ? 
                        and     LanguageFID = ?
                    ''',
                    row.Description,
                    row.InventoryId,
                    row.translatefid
                )
                # update language en-US = getdate
                cursor.execute(
                    ''' UPDATE [dbo].[HotelInventoryDescriptions] 
                        SET	    LastModifiedDate = getdate()
                        WHERE   InventoryId = ? 
                        and     LanguageFID = ?
                    ''',
                    row.InventoryId,
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
                    ''' UPDATE [dbo].[HotelInformationDetails] 
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
                    ''' UPDATE [dbo].[HotelInformationDetails] 
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