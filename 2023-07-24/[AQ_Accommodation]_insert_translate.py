import os
from google.cloud import translate_v2
import pyodbc
import pandas as pd
import warnings

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

# declare dataframe
ratingattributedetails = pd.DataFrame()
roomamenityattributedetails = pd.DataFrame()
hotelsandboxmutillang = pd.DataFrame()
hotelamenityattributedetails = pd.DataFrame()
hoteladditionalservicedetails = pd.DataFrame()

#exception table
hotelinformationdetails = pd.DataFrame()

# declare database name and table name
databasename = 'AQ_Accommodation'
tables = ['RatingAttributeDetails', 'RoomAmenityAttributeDetails', 'HotelSandboxMutilLang',
          'HotelAmenityAttributeDetails', 'HotelAdditionalServiceDetails']
exception_tables = ['HotelInformationDetails']
translate_date = []
exception_translate_date = []
insert_track = []
exception_insert_track = []

#################################################################  Get Last Translated Date

for table in tables:
    df_translate_date = pd.read_sql(
            f'''with cte_get_10_date_asc as (
                    select	distinct top 10 COALESCE(LastModifiedDate, CreatedDate) as LastModifiedDate
                    from	[dbo].[{table}]
                    where	LanguageFID = 1
                    order by LastModifiedDate
                    ),
                    cte_get_all as (
                    select	LastModifiedDate
                    from	cte_get_10_date_asc
                    UNION ALL
                    select	max([LastTranlastedDate])
                    from	[AQ_Configurations].[dbo].[Translate_Tracking_Log]
                    where	[Database_Name] = '{databasename}'
                    and		[Table_Name] = '{table}'
                    )
                    select convert(nvarchar(15), max(LastModifiedDate),102) as LastTranlastedDate from cte_get_all
            ''', cnxn)
    translate_date.append(df_translate_date['LastTranlastedDate'][0])
# print(ratingattributedetails_translate_date['LastTranlastedDate'])

# exception table (exclude YachtRouteMultiLanguages)
for table in exception_tables:
    df_exception_translate_date = pd.read_sql(
            f'''with cte_get_10_date_asc as (
                    select	distinct top 10 COALESCE(LastModifiedDate, ActivatedDate) as LastModifiedDate
                    from	[dbo].[{table}]
                    where	LanguageFID = 1
                    order by LastModifiedDate
                    ),
                    cte_get_all as (
                    select	LastModifiedDate
                    from	cte_get_10_date_asc
                    UNION ALL
                    select	max([LastTranlastedDate])
                    from	[AQ_Configurations].[dbo].[Translate_Tracking_Log]
                    where	[Database_Name] = '{databasename}'
                    and		[Table_Name] = '{table}'
                    )
                    select convert(nvarchar(15), max(LastModifiedDate),102) as LastTranlastedDate from cte_get_all
            ''', cnxn)
    exception_translate_date.append(df_exception_translate_date['LastTranlastedDate'][0])

################################################################# Get dataframe

# RatingAttributeDetails
for key, value in language.items():
    ###Insert / 1 as is_insert
    ratingattributedetails_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from [dbo].[RatingAttributeDetails] A 
            where LanguageFID = 1 
            and not exists (select 1 
                            from [dbo].[RatingAttributeDetails] B 
                            where A.RatingFID = B.RatingFID 
                            and B.LanguageFID = {key}) 
            and Deleted = 0
            and RatingFID = 1
        '''
        , cnxn, index_col='ID')
    ratingattributedetails = pd.concat([ratingattributedetails, ratingattributedetails_insert], ignore_index=True)

    ###Update / 0 as is_insert
    ratingattributedetails_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[RatingAttributeDetails] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, CreatedDate),102)
                <= '{translate_date[0]}'
            and RatingFID = 1
        '''
        , cnxn, index_col='ID')
    ratingattributedetails = pd.concat([ratingattributedetails, ratingattributedetails_update], ignore_index=True)

# check insert into track log
if ratingattributedetails.empty:
    insert_track.append(0)
else:
    insert_track.append(1)

# RoomAmenityAttributeDetails
for key, value in language.items():
    ###Insert / 1 as is_insert
    roomamenityattributedetails_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from    [dbo].[RoomAmenityAttributeDetails] A 
            where	LanguageFID = 1 
            and not exists (select 1 
                            from [dbo].[RoomAmenityAttributeDetails] B 
                            where A.RoomAmenitiFID = B.RoomAmenitiFID 
                            and B.LanguageFID = {key}) 
            and     Deleted = 0
            and RoomAmenitiFID = 1 
        '''
        , cnxn, index_col='ID')
    roomamenityattributedetails = pd.concat([roomamenityattributedetails, roomamenityattributedetails_insert], ignore_index=True)

    ###Update / 0 as is_insert
    roomamenityattributedetails_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[RoomAmenityAttributeDetails] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, CreatedDate),102)
                <= '{translate_date[1]}'
            and RoomAmenitiFID = 1 
        '''
        , cnxn, index_col='ID')
    roomamenityattributedetails = pd.concat([roomamenityattributedetails, roomamenityattributedetails_update], ignore_index=True)

# check insert into track log
if roomamenityattributedetails.empty:
    insert_track.append(0)
else:
    insert_track.append(1)

# HotelSandboxMutilLang
for key, value in language.items():
    ###Insert / 1 as is_insert
    hotelsandboxmutillang_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from    [dbo].[HotelSandboxMutilLang] A 
            where	LanguageFID = 1 
            and not exists (select 1 
                            from [dbo].[HotelSandboxMutilLang] B 
                            where A.SanboxFID = B.SanboxFID 
                            and B.LanguageFID = {key}) 
            and Deleted = 0
            and SanboxFID = 10
        '''
        , cnxn, index_col='ID')
    hotelsandboxmutillang = pd.concat([hotelsandboxmutillang, hotelsandboxmutillang_insert], ignore_index=True)

    ###Update / 0 as is_insert
    hotelsandboxmutillang_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[HotelSandboxMutilLang] A 
            where	LanguageFID = 1 
            and Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, CreatedDate),102)
                <= '{translate_date[2]}'
            and SanboxFID = 10
        '''
        , cnxn, index_col='ID')
    hotelsandboxmutillang = pd.concat([hotelsandboxmutillang, hotelsandboxmutillang_update], ignore_index=True)

# check insert into track log
if hotelsandboxmutillang.empty:
    insert_track.append(0)
else:
    insert_track.append(1)

# HotelAmenityAttributeDetails
for key, value in language.items():
    ###Insert / 1 as is_insert
    hotelamenityattributedetails_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from    [dbo].[HotelAmenityAttributeDetails] A 
            where	LanguageFID = 1 
            and not exists (select 1 
                            from [dbo].[HotelAmenityAttributeDetails] B 
                            where A.HotelAmenitiFID = B.HotelAmenitiFID 
                            and B.LanguageFID = {key}) 
            and Deleted = 0
            and HotelAmenitiFID = 1
        '''
        , cnxn, index_col='ID')
    hotelamenityattributedetails = pd.concat([hotelamenityattributedetails, hotelamenityattributedetails_insert], ignore_index=True)

    ###Update / 0 as is_insert
    hotelamenityattributedetails_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[HotelAmenityAttributeDetails] A 
            where	LanguageFID = 1 
            and Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, CreatedDate),102)
                <= '{translate_date[3]}'
            and HotelAmenitiFID = 1
        '''
        , cnxn, index_col='ID')
    hotelamenityattributedetails = pd.concat([hotelamenityattributedetails, hotelamenityattributedetails_update], ignore_index=True)

# check insert into track log
if hotelsandboxmutillang.empty:
    insert_track.append(0)
else:
    insert_track.append(1)

# HotelAdditionalServiceDetails
for key, value in language.items():
    ###Insert / 1 as is_insert
    hoteladditionalservicedetails_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from    [dbo].[HotelAdditionalServiceDetails] A 
            where	LanguageFID = 1 
            and not exists (select 1 
                            from [dbo].[HotelAdditionalServiceDetails] B 
                            where A.AdditionalServiceFID = B.AdditionalServiceFID 
                            and B.LanguageFID = {key}) 
            and Deleted = 0
            and AdditionalServiceFID = 8
        '''
        , cnxn, index_col='ID')
    hoteladditionalservicedetails = pd.concat([hoteladditionalservicedetails, hoteladditionalservicedetails_insert], ignore_index=True)

    ###Update / 0 as is_insert
    hoteladditionalservicedetails_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[HotelAdditionalServiceDetails] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, CreatedDate),102)
                <= '{translate_date[4]}'
            and AdditionalServiceFID = 8
        '''
        , cnxn, index_col='ID')
    hoteladditionalservicedetails = pd.concat([hoteladditionalservicedetails, hoteladditionalservicedetails_update], ignore_index=True)

# check insert into track log
if hotelsandboxmutillang.empty:
    insert_track.append(0)
else:
    insert_track.append(1)

# hotelinformationdetails
for key, value in language.items():
    ###Insert / 1 as is_insert
    hotelinformationdetails_insert = pd.read_sql(
        f'''    select	1 as is_insert,
                        {key} as translatefid,
                        '{value}' as language_translate,
                        *
                from	[dbo].[HotelInformationDetails] A
                where	LanguageFID = 1 
                and     not exists (select 1 
                        from [dbo].[HotelInformationDetails] B 
                        where A.InformationFID = B.InformationFID 
                        and B.LanguageFID = {key}) 
                and Deleted = 0
                and InformationFID = 9
                '''
        , cnxn, index_col='ID')
    hotelinformationdetails = pd.concat([hotelinformationdetails, hotelinformationdetails_insert], ignore_index=True)

    ###Update / 0 as is_insert
    hotelinformationdetails_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[HotelInformationDetails] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, ActivatedDate),102)
                <= '{exception_translate_date[0]}'
            and InformationFID = 9
        '''
        , cnxn, index_col='ID')
    hotelinformationdetails = pd.concat([hotelinformationdetails, hotelinformationdetails_update], ignore_index=True)

# check insert into track log
if hotelinformationdetails.empty:
    exception_insert_track.append(0)
else:
    exception_insert_track.append(1)

############################################################## Call Function Translate


#################################################################

# RatingAttributeDetails
ratingattributedetails_Description_Translated = []
for index, row in ratingattributedetails.iterrows():
    language_translate = row['language_translate']
    text_description = row['Name']

    # process NULL data in FullDescriptions column
    if text_description == None:
        ratingattributedetails_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=language_translate)
        ratingattributedetails_Description_Translated.append(output_description['translatedText'])
# add 1 new columns translated
ratingattributedetails['Description_Translated'] = ratingattributedetails_Description_Translated

#################################################################

# roomamenityattributedetails
roomamenityattributedetails_Description_Translated = []
for index, row in roomamenityattributedetails.iterrows():
    language_translate = row['language_translate']
    text_description = row['Name']
    # process NULL data in Title column

    # process NULL data in FullDescriptions column
    if text_description == None:
        roomamenityattributedetails_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=language_translate)
        roomamenityattributedetails_Description_Translated.append(output_description['translatedText'])
# add 1 new columns translated
roomamenityattributedetails['Description_Translated'] = roomamenityattributedetails_Description_Translated

#################################################################

# hotelsandboxmutillang
hotelsandboxmutillang_Description_Translated = []
hotelsandboxmutillang_Policies_Translated = []
hotelsandboxmutillang_ShortDescription_Translated = []
for index, row in hotelsandboxmutillang.iterrows():
    language_translate = row['language_translate']
    text_description = row['SanboxDescription']
    text_policies = row['SandboxPolicies']
    text_shortdescription = row['SanboxShortDescription']

    # process NULL data in FullDescriptions column
    if text_description == None:
        hotelsandboxmutillang_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=language_translate)
        hotelsandboxmutillang_Description_Translated.append(output_description['translatedText'])

    # process NULL data in policies column
    if text_policies == None:
        hotelsandboxmutillang_Policies_Translated.append(text_policies)
    else:
        output_policies = translate_client.translate(text_policies, target_language=language_translate)
        hotelsandboxmutillang_Policies_Translated.append(output_policies['translatedText'])

    # process NULL data in ShortDescription column
    if text_shortdescription == None:
        hotelsandboxmutillang_ShortDescription_Translated.append(text_shortdescription)
    else:
        output_shortdescription = translate_client.translate(text_shortdescription, target_language=language_translate)
        hotelsandboxmutillang_ShortDescription_Translated.append(output_shortdescription['translatedText'])

# add 3 new columns translated
hotelsandboxmutillang['Description_Translated'] = hotelsandboxmutillang_Description_Translated
hotelsandboxmutillang['Policies_Translated'] = hotelsandboxmutillang_Policies_Translated
hotelsandboxmutillang['ShortDescription_Translated'] = hotelsandboxmutillang_ShortDescription_Translated

#################################################################

# hotelamenityattributedetails
hotelamenityattributedetails_Description_Translated = []
for index, row in hotelamenityattributedetails.iterrows():
    language_translate = row['language_translate']
    text_description = row['Name']

    # process NULL data in FullDescriptions column
    if text_description == None:
        hotelamenityattributedetails_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=language_translate)
        hotelamenityattributedetails_Description_Translated.append(output_description['translatedText'])
# add 1 new columns translated
hotelamenityattributedetails['Description_Translated'] = hotelamenityattributedetails_Description_Translated

#################################################################

# hoteladditionalservicedetails
hoteladditionalservicedetails_Description_Translated = []
for index, row in hoteladditionalservicedetails.iterrows():
    language_translate = row['language_translate']
    text_description = row['Name']

    # process NULL data in FullDescriptions column
    if text_description == None:
        hoteladditionalservicedetails_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=language_translate)
        hoteladditionalservicedetails_Description_Translated.append(output_description['translatedText'])
# add 2 new columns translated
hoteladditionalservicedetails['Description_Translated'] = hoteladditionalservicedetails_Description_Translated

# hotelinformationdetails
hotelinformationdetails_Title_Translated = []
hotelinformationdetails_Description_Translated = []
hotelinformationdetails_ShortDescription_Translated = []
for index, row in hotelinformationdetails.iterrows():
    language_translate = row['language_translate']
    text_title = row['Title']
    text_description = row['FullDescriptions']
    text_shortdescription = row['ShortDescriptions']
    # process NULL data in Title column
    if text_title == None:
        hotelinformationdetails_Title_Translated.append(text_title)
    else:
        output_title = translate_client.translate(text_title, target_language=language_translate)
        hotelinformationdetails_Title_Translated.append(output_title['translatedText'])

    # process NULL data in FullDescriptions column
    if text_description == None:
        hotelinformationdetails_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=language_translate)
        hotelinformationdetails_Description_Translated.append(output_description['translatedText'])

    # process NULL data in ShortDescriptions column
    if text_shortdescription == None:
        hotelinformationdetails_ShortDescription_Translated.append(text_shortdescription)
    else:
        output_shortdescription = translate_client.translate(text_shortdescription, target_language=language_translate)
        hotelinformationdetails_ShortDescription_Translated.append(output_shortdescription['translatedText'])
# add 3 new columns translated
hotelinformationdetails['Title_Translated'] = hotelinformationdetails_Title_Translated
hotelinformationdetails['Description_Translated'] = hotelinformationdetails_Description_Translated
hotelinformationdetails['ShortDescription_Translated'] = hotelinformationdetails_ShortDescription_Translated


################################################################# Insert dataframe into tables
cursor = cnxn.cursor()

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
for i in range(0,len(insert_track)):
    if insert_track[i] == 1:
        cursor.execute(
            f'''INSERT INTO [AQ_Configurations].[dbo].[Translate_Tracking_Log]([Database_Name],
                                                                                [Table_Name],
                                                                                [LastTranlastedDate]) 
                VALUES (?,?,?)
            ''',
            databasename,
            tables[i],
            translate_date[i]
        )

# Insert exception table tracking log into Translate_Tracking_Log
for i in range(0,len(exception_insert_track)):
    if exception_insert_track[i] == 1:
        cursor.execute(
            f'''INSERT INTO [AQ_Configurations].[dbo].[Translate_Tracking_Log]([Database_Name],
                                                                                [Table_Name],
                                                                                [LastTranlastedDate]) 
                VALUES (?,?,?)
            ''',
            databasename,
            exception_tables[i],
            exception_translate_date[i]
        )


cnxn.commit()
cursor.close()
