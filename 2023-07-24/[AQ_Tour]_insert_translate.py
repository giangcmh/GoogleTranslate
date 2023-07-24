import os
from google.cloud import translate_v2
import pyodbc
import pandas as pd
import warnings

warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)  # show all columns in pandas dataframe

#################################################################
# init credentials google translate api
os.environ[
    'GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\PythonProject\GoogleTranslate\elaborate-art-392802-e18eac2a2238.json"

# init connection SQL Server
cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                      "Server=69.172.67.3,1400;"
                      "Database=AQ_Tour;"
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
tourcancelpolimutillang = pd.DataFrame()
tourinformationdetails = pd.DataFrame()
tourpaymentpolimutillang = pd.DataFrame()
tourpricingdetails = pd.DataFrame()
tourtripplandetail = pd.DataFrame()
tourtypedetail = pd.DataFrame()

# declare database name and table name
databasename = 'AQ_Tour'
tables = ['RatingAttributeDetails','TourCancelPoliMutilLang', 'TourPaymentPoliMutilLang',
              'TourPricingDetails', 'TourTripPlanDetail', 'TourTypeDetail' ]
translate_date = []
insert_track = []

# exception table because it has ActivatedDate column instead of CreatedDate
tourinformationdetails_table = 'TourInformationDetails'

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

# exception table
tourinformationdetails_translate_date = pd.read_sql(
        f'''with cte_get_10_date_asc as (
                select	distinct top 10 COALESCE(LastModifiedDate, ActivatedDate) as LastModifiedDate
                from	[dbo].[TourInformationDetails]
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
                and		[Table_Name] = '{tourinformationdetails_table}'
                )
                select convert(nvarchar(15), max(LastModifiedDate),102) as LastTranlastedDate from cte_get_all
        ''', cnxn)

#################################################################  Get dataframe

# RatingAttributeDetails
for key, value in language.items():
    ###Insert / 1 as is_insert
    ratingattributedetails_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[RatingAttributeDetails] A 
            where	LanguageFID = 1 
            and     not exists (select 1 
                                from [dbo].[RatingAttributeDetails] B 
                                where A.RatingFID = B.RatingFID 
                                and B.LanguageFID = {key}) 
            and Deleted = 0
        '''
        , cnxn, index_col='ID')
    ratingattributedetails = pd.concat([ratingattributedetails, ratingattributedetails_insert], ignore_index=True)

    ###Update / 0 as is_insert
    ratingattributedetails_update = pd.read_sql(
        f'''    select	0 as is_insert,
                        {key} as translatefid,
                        '{value}' as language_translate,
                        * 
                from	[dbo].[RatingAttributeDetails] A 
                where	LanguageFID = 1 
                and     Deleted = 0 
                and     convert(nvarchar(15), COALESCE(LastModifiedDate, CreatedDate),102)
                <= '{translate_date[0]}'
        '''
        , cnxn, index_col='ID')
    ratingattributedetails = pd.concat([ratingattributedetails, ratingattributedetails_update], ignore_index=True)

# check insert into track log
if ratingattributedetails.empty:
    insert_track.append(0)
else:
    insert_track.append(1)

# print(ratingattributedetails[['is_insert','language_translate', 'RatingFID', 'LanguageFID', 'LastModifiedDate']])

# TourCancelPoliMutilLang
for key, value in language.items():
    ###Insert / 1 as is_insert
    tourcancelpolimutillang_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[TourCancelPoliMutilLang] A 
            where	LanguageFID = 1 
            and     not exists (select 1 
                                from [dbo].[TourCancelPoliMutilLang] B 
                                where A.CancellationPoliciFID = B.CancellationPoliciFID 
                                and B.LanguageFID = {key}) 
            and Deleted = 0
        '''
        , cnxn, index_col='ID')
    tourcancelpolimutillang = pd.concat([tourcancelpolimutillang, tourcancelpolimutillang_insert], ignore_index=True)

    ###Update / 0 as is_insert
    tourcancelpolimutillang_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[TourCancelPoliMutilLang] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, CreatedDate),102)
                <= '{translate_date[1]}'
        '''
        , cnxn, index_col='ID')
    tourcancelpolimutillang = pd.concat([tourcancelpolimutillang, tourcancelpolimutillang_update], ignore_index=True)


# check insert into track log
if tourcancelpolimutillang.empty:
    insert_track.append(0)
else:
    insert_track.append(1)
# print(translate_date[1])
# print(tourcancelpolimutillang[['is_insert','language_translate', 'CancellationPoliciFID', 'LanguageFID']])

# TourPaymentPoliMutilLang
for key, value in language.items():
    ###Insert / 1 as is_insert
    tourpaymentpolimutillang_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[TourPaymentPoliMutilLang] A 
            where	LanguageFID = 1 
            and     not exists (select 1 
                                from [dbo].[TourPaymentPoliMutilLang] B 
                                where A.PaymentPoliciFID = B.PaymentPoliciFID 
                                and B.LanguageFID = {key}) 
            and Deleted = 0
        '''
        , cnxn, index_col='ID')
    tourpaymentpolimutillang = pd.concat([tourpaymentpolimutillang, tourpaymentpolimutillang_insert], ignore_index=True)

    ###Update / 0 as is_insert
    tourpaymentpolimutillang_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[TourPaymentPoliMutilLang] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, CreatedDate),102)
                <= '{translate_date[2]}'
        '''
        , cnxn, index_col='ID')
    tourpaymentpolimutillang = pd.concat([tourpaymentpolimutillang, tourpaymentpolimutillang_update], ignore_index=True)

# check insert into track log
if tourpaymentpolimutillang.empty:
    insert_track.append(0)
else:
    insert_track.append(1)
# print(tourpaymentpolimutillang[['is_insert','language_translate', 'PaymentPoliciFID', 'LanguageFID']])

# TourPricingDetails
for key, value in language.items():
    ###Insert / 1 as is_insert
    tourpricingdetails_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[TourPricingDetails] A 
            where	LanguageFID = 1 
            and     not exists (select 1 
                                from [dbo].[TourPricingDetails] B 
                                where A.PricingFID = B.PricingFID 
                                and B.LanguageFID = {key}) 
            and Deleted = 0
        '''
        , cnxn, index_col='ID')
    tourpricingdetails = pd.concat([tourpricingdetails, tourpricingdetails_insert], ignore_index=True)

    ###Update / 0 as is_insert
    tourpricingdetails_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[TourPricingDetails] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, CreatedDate),102)
                <= '{translate_date[3]}'
        '''
        , cnxn, index_col='ID')
    tourpricingdetails = pd.concat([tourpricingdetails, tourpricingdetails_update], ignore_index=True)

# check insert into track log
if tourpricingdetails.empty:
    insert_track.append(0)
else:
    insert_track.append(1)
# print(tourpricingdetails[['is_insert','language_translate', 'PricingFID', 'LanguageFID']])

# TourTripPlanDetail
for key, value in language.items():
    ###Insert / 1 as is_insert
    tourtripplandetail_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[TourTripPlanDetail] A 
            where	LanguageFID = 1 
            and     not exists (select 1 
                                from [dbo].[TourTripPlanDetail] B 
                                where A.TripPlanFID = B.TripPlanFID 
                                and B.LanguageFID = {key}) 
            and Deleted = 0
        '''
        , cnxn, index_col='ID')
    tourtripplandetail = pd.concat([tourtripplandetail, tourtripplandetail_insert], ignore_index=True)

    ###Update / 0 as is_insert
    tourtripplandetail_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[TourTripPlanDetail] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, CreatedDate),102)
                <= '{translate_date[4]}'
        '''
        , cnxn, index_col='ID')
    tourtripplandetail = pd.concat([tourtripplandetail, tourtripplandetail_update], ignore_index=True)

# check insert into track log
if tourtripplandetail.empty:
    insert_track.append(0)
else:
    insert_track.append(1)
# print(tourtripplandetail[['is_insert','language_translate', 'TripPlanFID', 'LanguageFID']])

# TourTypeDetail
for key, value in language.items():
    ###Insert / 1 as is_insert
    tourtypedetail_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[TourTypeDetail] A 
            where	LanguageFID = 1 
            and     not exists (select 1 
                                from [dbo].[TourTypeDetail] B 
                                where A.TourTypeFID = B.TourTypeFID 
                                and B.LanguageFID = {key}) 
            and Deleted = 0
        '''
        , cnxn, index_col='ID')
    tourtypedetail = pd.concat([tourtypedetail, tourtypedetail_insert], ignore_index=True)

    ###Update / 0 as is_insert
    tourtypedetail_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[TourTypeDetail] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, CreatedDate),102)
                <= '{translate_date[5]}'
                
        '''
        , cnxn, index_col='ID')
    tourtypedetail = pd.concat([tourtypedetail, tourtypedetail_update], ignore_index=True)

# check insert into track log
if tourtypedetail.empty:
    insert_track.append(0)
else:
    insert_track.append(1)
# print(tourtypedetail[['is_insert','language_translate', 'TourTypeFID', 'LanguageFID']])

# exception table
# TourInformationDetails
for key, value in language.items():
    ###Insert / 1 as is_insert
    tourinformationdetails_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[TourInformationDetails] A 
            where	LanguageFID = 1 
            and     not exists (select 1 
                                from [dbo].[TourInformationDetails] B 
                                where A.InformationFID = B.InformationFID 
                                and B.LanguageFID = {key}) 
            and Deleted = 0
        '''
        , cnxn, index_col='ID')
    tourinformationdetails = pd.concat([tourinformationdetails, tourinformationdetails_insert], ignore_index=True)

    ###Update / 0 as is_insert
    tourinformationdetails_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[TourInformationDetails] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, ActivatedDate),102)
                <= '{tourinformationdetails_translate_date['LastTranlastedDate'][0]}'
        '''
        , cnxn, index_col='ID')
    tourinformationdetails = pd.concat([tourinformationdetails, tourinformationdetails_update], ignore_index=True)

# print(tourinformationdetails[['is_insert','language_translate', 'InformationFID', 'LanguageFID']])

################################################################# Call Function Translate

# RatingAttributeDetails
ratingattributedetails_Name_Translated = []

for index, row in ratingattributedetails.iterrows():
    language_translate = row['language_translate']
    text_name = row['Name']

    # process NULL data in column
    if text_name == None:
        ratingattributedetails_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=language_translate)
        ratingattributedetails_Name_Translated.append(output_description['translatedText'])

# add 1 new columns translated
ratingattributedetails['Name_Translated'] = ratingattributedetails_Name_Translated

# print(ratingattributedetails[['language_translate', 'RatingFID', 'LanguageFID','Name','Name_Translated' ]])

# TourCancelPoliMutilLang
tourcancelpolimutillang_Name_Translated = []
tourcancelpolimutillang_Term_Translated = []

for index, row in tourcancelpolimutillang.iterrows():
    language_translate = row['language_translate']
    text_name = row['Name']
    text_term = row['TermAndPolicies']

    # process NULL data in column
    if text_name == None:
        tourcancelpolimutillang_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=language_translate)
        tourcancelpolimutillang_Name_Translated.append(output_description['translatedText'])
    if text_term == None:
        tourcancelpolimutillang_Term_Translated.append(text_term)
    else:
        output_description = translate_client.translate(text_term, target_language=language_translate)
        tourcancelpolimutillang_Term_Translated.append(output_description['translatedText'])

# add 1 new columns translated
tourcancelpolimutillang['Name_Translated'] = tourcancelpolimutillang_Name_Translated
tourcancelpolimutillang['Term_Translated'] = tourcancelpolimutillang_Term_Translated

# TourInformationDetails
tourinformationdetails_Title_Translated = []
tourinformationdetails_ShortDescriptions_Translated = []
tourinformationdetails_FullDescriptions_Translated = []

for index, row in tourinformationdetails.iterrows():
    language_translate = row['language_translate']
    text_title = row['Title']
    text_shortdescription = row['ShortDescriptions']
    text_fulldescription = row['FullDescriptions']

    # process NULL data in column
    if text_title == None:
        tourinformationdetails_Title_Translated.append(text_title)
    else:
        output_description = translate_client.translate(text_title, target_language=language_translate)
        tourinformationdetails_Title_Translated.append(output_description['translatedText'])

    if text_shortdescription == None:
        tourinformationdetails_ShortDescriptions_Translated.append(text_shortdescription)
    else:
        output_description = translate_client.translate(text_shortdescription, target_language=language_translate)
        tourinformationdetails_ShortDescriptions_Translated.append(output_description['translatedText'])

    if text_fulldescription == None:
        tourinformationdetails_FullDescriptions_Translated.append(text_fulldescription)
    else:
        output_description = translate_client.translate(text_fulldescription, target_language=language_translate)
        tourinformationdetails_FullDescriptions_Translated.append(output_description['translatedText'])

# add 1 new columns translated
tourinformationdetails['Title_Translated'] = tourinformationdetails_Title_Translated
tourinformationdetails['ShortDescriptions_Translated'] = tourinformationdetails_ShortDescriptions_Translated
tourinformationdetails['FullDescriptions_Translated'] = tourinformationdetails_FullDescriptions_Translated

# TourPaymentPoliMutilLang
tourpaymentpolimutillang_Name_Translated = []
tourpaymentpolimutillang_Term_Translated = []

for index, row in tourpaymentpolimutillang.iterrows():
    language_translate = row['language_translate']
    text_name = row['Name']
    text_term = row['TermAndPolicies']

    # process NULL data in column
    if text_name == None:
        tourpaymentpolimutillang_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=language_translate)
        tourpaymentpolimutillang_Name_Translated.append(output_description['translatedText'])
    if text_term == None:
        tourpaymentpolimutillang_Term_Translated.append(text_term)
    else:
        output_description = translate_client.translate(text_term, target_language=language_translate)
        tourpaymentpolimutillang_Term_Translated.append(output_description['translatedText'])

# add 1 new columns translated
tourpaymentpolimutillang['Name_Translated'] = tourpaymentpolimutillang_Name_Translated
tourpaymentpolimutillang['Term_Translated'] = tourpaymentpolimutillang_Term_Translated

# TourPricingDetails
tourpricingdetails_Name_Translated = []
tourpricingdetails_Remark_Translated = []

for index, row in tourpricingdetails.iterrows():
    language_translate = row['language_translate']
    text_name = row['Name']
    text_remark = row['Remark']

    # process NULL data in column
    if text_name == None:
        tourpricingdetails_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=language_translate)
        tourpricingdetails_Name_Translated.append(output_description['translatedText'])
    if text_remark == None:
        tourpricingdetails_Remark_Translated.append(text_remark)
    else:
        output_description = translate_client.translate(text_remark, target_language=language_translate)
        tourpricingdetails_Remark_Translated.append(output_description['translatedText'])

# add 1 new columns translated
tourpricingdetails['Name_Translated'] = tourpricingdetails_Name_Translated
tourpricingdetails['Remark_Translated'] = tourpricingdetails_Remark_Translated

# TourTripPlanDetail
tourtripplandetail_ItemName_Translated = []
tourtripplandetail_ItemDescription_Translated = []

for index, row in tourtripplandetail.iterrows():
    language_translate = row['language_translate']
    text_itemname = row['ItemName']
    text_itemdescription = row['ItemDescription']

    # process NULL data in column
    if text_itemname == None:
        tourtripplandetail_ItemName_Translated.append(text_itemname)
    else:
        output_description = translate_client.translate(text_itemname, target_language=language_translate)
        tourtripplandetail_ItemName_Translated.append(output_description['translatedText'])

    if text_itemdescription == None:
        tourtripplandetail_ItemDescription_Translated.append(text_itemdescription)
    else:
        output_description = translate_client.translate(text_itemdescription, target_language=language_translate)
        tourtripplandetail_ItemDescription_Translated.append(output_description['translatedText'])

# add 1 new columns translated
tourtripplandetail['ItemName_Translated'] = tourtripplandetail_ItemName_Translated
tourtripplandetail['ItemDescription_Translated'] = tourtripplandetail_ItemDescription_Translated

# TourTypeDetail
tourtypedetail_Name_Translated = []

for index, row in tourtypedetail.iterrows():
    language_translate = row['language_translate']
    text_name = row['Name']

    # process NULL data in column
    if text_name == None:
        tourtypedetail_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=language_translate)
        tourtypedetail_Name_Translated.append(output_description['translatedText'])

# add 1 new columns translated
tourtypedetail['Name_Translated'] = tourtypedetail_Name_Translated

################################################################# Insert dataframe into tables
cursor = cnxn.cursor()

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
            row.Name_Translated,
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
            row.Name_Translated,
            row.Term_Translated,
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
            row.Name_Translated,
            row.Term_Translated,
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
            row.Name_Translated,
            row.Term_Translated,
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
            row.Name_Translated,
            row.Term_Translated,
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
            row.Name_Translated,
            row.Remark_Translated,
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
            row.Name_Translated,
            row.Remark_Translated,
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
            row.ItemName_Translated,
            row.ItemDescription_Translated,
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
            row.ItemName_Translated,
            row.ItemDescription_Translated,
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
            row.Name_Translated,
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
            row.Name_Translated,
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
            ''' UPDATE [dbo].[TourInformationDetails] 
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
            ''' UPDATE [dbo].[TourInformationDetails] 
                SET	    LastModifiedDate = getdate()
                WHERE   InformationFID = ? 
                and     LanguageFID = ?
            ''',
            row.InformationFID,
            1  # language en-US
        )

# Insert tracking log into Translate_Tracking_Log
if tourinformationdetails.empty == False:
    cursor.execute(
        f'''INSERT INTO [AQ_Configurations].[dbo].[Translate_Tracking_Log]([Database_Name],
                                                                            [Table_Name],
                                                                            [LastTranlastedDate]) 
            VALUES (?,?,?)
        ''',
        databasename,
        tourinformationdetails_table,
        tourinformationdetails_translate_date['LastTranlastedDate'][0]
    )


cnxn.commit()
cursor.close()
