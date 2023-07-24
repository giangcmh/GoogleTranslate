import os
from google.cloud import translate_v2
import pyodbc
import pandas as pd
import warnings

warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)  # show all columns in pandas dataframe

#################################################################
# init credentials google translate api
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\PythonProject\GoogleTranslate\elaborate-art-392802-e18eac2a2238.json"

# init connection SQL Server
cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                      "Server=69.172.67.3,1400;"
                      "Database=AQ_Yacht;"
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
yachtadditionalservicedetails = pd.DataFrame()
yachtcancelpolimutillang = pd.DataFrame()
yachtpaymentpolimutillang = pd.DataFrame()
yachttourcategoryinfomations = pd.DataFrame()

#exception table
yachtinformationdetails = pd.DataFrame()
yachtmerchantinformationdetails = pd.DataFrame()
yachttourinformationdetails = pd.DataFrame()
yachtroutemultilanguages = pd.DataFrame()

# declare database name and table name
databasename = 'AQ_Yacht'
tables = ['RatingAttributeDetails','YachtAdditionalServiceDetails', 'YachtCancelPoliMutilLang',
              'YachtPaymentPoliMutilLang', 'YachtTourCategoryInfomations' ]
exception_tables = ['YachtInformationDetails','YachtMerchantInformationDetails','YachtTourInformationDetails',
                    'YachtRouteMultiLanguages']
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
    if table != 'YachtRouteMultiLanguages':
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
        f'''select	0 as is_insert,
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

# YachtAdditionalServiceDetails
for key, value in language.items():
    ###Insert / 1 as is_insert
    yachtadditionalservicedetails_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[YachtAdditionalServiceDetails] A 
            where	LanguageFID = 1 
            and     not exists (select 1 
                                from [dbo].[YachtAdditionalServiceDetails] B 
                                where A.AdditionalServiceFID = B.AdditionalServiceFID and B.LanguageFID = {key}) 
            and Deleted = 0

        '''
        , cnxn, index_col='ID')
    yachtadditionalservicedetails = pd.concat([yachtadditionalservicedetails, yachtadditionalservicedetails_insert], ignore_index=True)

    ###Update / 0 as is_insert
    yachtadditionalservicedetails_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[YachtAdditionalServiceDetails] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, CreatedDate),102)
                <= '{translate_date[1]}'

        '''
        , cnxn, index_col='ID')
    yachtadditionalservicedetails = pd.concat([yachtadditionalservicedetails, yachtadditionalservicedetails_update], ignore_index=True)

# check insert into track log
if yachtadditionalservicedetails.empty:
    insert_track.append(0)
else:
    insert_track.append(1)

# YachtCancelPoliMutilLang
for key, value in language.items():
    ###Insert / 1 as is_insert
    yachtcancelpolimutillang_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[YachtCancelPoliMutilLang] A 
            where	LanguageFID = 1 
            and     not exists (select 1 
                                from [dbo].[YachtCancelPoliMutilLang] B 
                                where A.CancellationPoliciFID = B.CancellationPoliciFID 
                                and B.LanguageFID = {key}) 
            and Deleted = 0

        '''
        , cnxn, index_col='ID')
    yachtcancelpolimutillang = pd.concat([yachtcancelpolimutillang, yachtcancelpolimutillang_insert], ignore_index=True)

    ###Update / 0 as is_insert
    yachtcancelpolimutillang_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[YachtCancelPoliMutilLang] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, CreatedDate),102)
                <= '{translate_date[2]}'

        '''
        , cnxn, index_col='ID')
    yachtcancelpolimutillang = pd.concat([yachtcancelpolimutillang, yachtcancelpolimutillang_update], ignore_index=True)

# check insert into track log
if yachtcancelpolimutillang.empty:
    insert_track.append(0)
else:
    insert_track.append(1)

# YachtPaymentPoliMutilLang
for key, value in language.items():
    ###Insert / 1 as is_insert
    yachtpaymentpolimutillang_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[YachtPaymentPoliMutilLang] A 
            where	LanguageFID = 1 
            and     not exists (select 1 
                                from [dbo].[YachtPaymentPoliMutilLang] B 
                                where A.PaymentPoliciFID = B.PaymentPoliciFID 
                                and B.LanguageFID = {key}) 
            and Deleted = 0

        '''
        , cnxn, index_col='ID')
    yachtpaymentpolimutillang = pd.concat([yachtpaymentpolimutillang, yachtpaymentpolimutillang_insert], ignore_index=True)

    ###Update / 0 as is_insert
    yachtpaymentpolimutillang_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[YachtPaymentPoliMutilLang] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, CreatedDate),102)
                <= '{translate_date[3]}'

        '''
        , cnxn, index_col='ID')
    yachtpaymentpolimutillang = pd.concat([yachtpaymentpolimutillang, yachtpaymentpolimutillang_update], ignore_index=True)

# check insert into track log
if yachtpaymentpolimutillang.empty:
    insert_track.append(0)
else:
    insert_track.append(1)

# YachtTourCategoryInfomations
for key, value in language.items():
    ###Insert / 1 as is_insert
    yachttourcategoryinfomations_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[YachtTourCategoryInfomations] A 
            where	LanguageFID = 1 
            and     not exists (select 1 
                                from [dbo].[YachtTourCategoryInfomations] B 
                                where A.TourCategoryFID = B.TourCategoryFID 
                                and B.LanguageFID = {key}) 
            and Deleted = 0

        '''
        , cnxn, index_col='ID')
    yachttourcategoryinfomations = pd.concat([yachttourcategoryinfomations, yachttourcategoryinfomations_insert], ignore_index=True)

    ###Update / 0 as is_insert
    yachttourcategoryinfomations_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[YachtTourCategoryInfomations] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, CreatedDate),102)
                <= '{translate_date[4]}'

        '''
        , cnxn, index_col='ID')
    yachttourcategoryinfomations = pd.concat([yachttourcategoryinfomations, yachttourcategoryinfomations_update], ignore_index=True)

# check insert into track log
if yachttourcategoryinfomations.empty:
    insert_track.append(0)
else:
    insert_track.append(1)

#########exception table
# YachtInformationDetails
for key, value in language.items():
    ###Insert / 1 as is_insert
    yachtinformationdetails_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[YachtInformationDetails] A 
            where	LanguageFID = 1 
            and     not exists (select 1 
                                from [dbo].[YachtInformationDetails] B 
                                where A.InformationFID = B.InformationFID 
                                and B.LanguageFID = {key})  
            and Deleted = 0

        '''
        , cnxn, index_col='ID')
    yachtinformationdetails = pd.concat([yachtinformationdetails, yachtinformationdetails_insert], ignore_index=True)

    ###Update / 0 as is_insert
    yachtinformationdetails_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[YachtInformationDetails] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, ActivatedDate),102)
                <= '{exception_translate_date[0]}'

        '''
        , cnxn, index_col='ID')
    yachtinformationdetails = pd.concat([yachtinformationdetails, yachtinformationdetails_update], ignore_index=True)

# check insert into track log
if yachtinformationdetails.empty:
    exception_insert_track.append(0)
else:
    exception_insert_track.append(1)

# YachtMerchantInformationDetails
for key, value in language.items():
    ###Insert / 1 as is_insert
    yachtmerchantinformationdetails_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[YachtMerchantInformationDetails] A 
            where	LanguageFID = 1 
            and     not exists (select 1 
                                from [dbo].[YachtMerchantInformationDetails] B 
                                where A.InformationFID = B.InformationFID 
                                and B.LanguageFID = {key}) 
            and Deleted = 0

        '''
        , cnxn, index_col='ID')
    yachtmerchantinformationdetails = pd.concat([yachtmerchantinformationdetails, yachtmerchantinformationdetails_insert], ignore_index=True)

    ###Update / 0 as is_insert
    yachtmerchantinformationdetails_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[YachtMerchantInformationDetails] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, ActivatedDate),102)
                <= '{exception_translate_date[1]}'

        '''
        , cnxn, index_col='ID')
    yachtmerchantinformationdetails = pd.concat([yachtmerchantinformationdetails, yachtmerchantinformationdetails_update], ignore_index=True)

# check insert into track log
if yachtmerchantinformationdetails.empty:
    exception_insert_track.append(0)
else:
    exception_insert_track.append(1)

# YachtTourInformationDetails
for key, value in language.items():
    ###Insert / 1 as is_insert
    yachttourinformationdetails_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[YachtTourInformationDetails] A 
            where	LanguageFID = 1 
            and     not exists (select 1 
                                from [dbo].[YachtTourInformationDetails] B 
                                where A.InformationFID = B.InformationFID 
                                and B.LanguageFID = {key}) 
            and Deleted = 0

        '''
        , cnxn, index_col='ID')
    yachttourinformationdetails = pd.concat([yachttourinformationdetails, yachttourinformationdetails_insert], ignore_index=True)

    ###Update / 0 as is_insert
    yachttourinformationdetails_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[YachtTourInformationDetails] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, ActivatedDate),102)
                <= '{exception_translate_date[2]}'

        '''
        , cnxn, index_col='ID')
    yachttourinformationdetails = pd.concat([yachttourinformationdetails, yachttourinformationdetails_update], ignore_index=True)

# check insert into track log
if yachttourinformationdetails.empty:
    exception_insert_track.append(0)
else:
    exception_insert_track.append(1)

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

# RatingAttributeDetails
ratingattributedetails_Name_Translated = []

for index, row in ratingattributedetails.iterrows():
    language_translate = row['language_translate']
    text_name = row['Name']

    # process NULL data in FullDescriptions column
    if text_name == None:
        ratingattributedetails_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=language_translate)
        ratingattributedetails_Name_Translated.append(output_description['translatedText'])

# add 1 new columns translated
ratingattributedetails['Name_Translated'] = ratingattributedetails_Name_Translated

#################################################################

# YachtAdditionalServiceDetails
yachtadditionalservicedetails_Name_Translated = []

for index, row in yachtadditionalservicedetails.iterrows():
    language_translate = row['language_translate']
    text_name = row['Name']

    # process NULL data in FullDescriptions column
    if text_name == None:
        yachtadditionalservicedetails_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=language_translate)
        yachtadditionalservicedetails_Name_Translated.append(output_description['translatedText'])

# add 1 new columns translated
yachtadditionalservicedetails['Name_Translated'] = yachtadditionalservicedetails_Name_Translated

#################################################################

# YachtCancelPoliMutilLang
yachtcancelpolimutillang_Name_Translated = []
yachtcancelpolimutillang_Term_Translated = []
for index, row in yachtcancelpolimutillang.iterrows():
    language_translate = row['language_translate']
    text_name = row['Name']
    text_term = row['TermAndPolicies']

    # process NULL data in FullDescriptions column
    if text_name == None:
        yachtcancelpolimutillang_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=language_translate)
        yachtcancelpolimutillang_Name_Translated.append(output_description['translatedText'])

    if text_term == None:
        yachtcancelpolimutillang_Term_Translated.append(text_term)
    else:
        output_description = translate_client.translate(text_term, target_language=language_translate)
        yachtcancelpolimutillang_Term_Translated.append(output_description['translatedText'])
# add 1 new columns translated
yachtcancelpolimutillang['Name_Translated'] = yachtcancelpolimutillang_Name_Translated
yachtcancelpolimutillang['Term_Translated'] = yachtcancelpolimutillang_Term_Translated


#################################################################

# YachtPaymentPoliMutilLang
yachtpaymentpolimutillang_Name_Translated = []
yachtpaymentpolimutillang_Term_Translated = []
for index, row in yachtpaymentpolimutillang.iterrows():
    language_translate = row['language_translate']
    text_name = row['Name']
    text_term = row['TermAndPolicies']

    # process NULL data in FullDescriptions column
    if text_name == None:
        yachtpaymentpolimutillang_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=language_translate)
        yachtpaymentpolimutillang_Name_Translated.append(output_description['translatedText'])

    if text_term == None:
        yachtpaymentpolimutillang_Term_Translated.append(text_term)
    else:
        output_description = translate_client.translate(text_term, target_language=language_translate)
        yachtpaymentpolimutillang_Term_Translated.append(output_description['translatedText'])
# add 1 new columns translated
yachtpaymentpolimutillang['Name_Translated'] = yachtpaymentpolimutillang_Name_Translated
yachtpaymentpolimutillang['Term_Translated'] = yachtpaymentpolimutillang_Term_Translated

#################################################################

# YachtTourCategoryInfomations
yachttourcategoryinfomations_ShortDescriptions_Translated = []
for index, row in yachttourcategoryinfomations.iterrows():
    language_translate = row['language_translate']
    text_shortdescription = row['ShortDescription']

    # process NULL data in FullDescriptions column
    if text_shortdescription == None:
        yachttourcategoryinfomations_ShortDescriptions_Translated.append(text_shortdescription)
    else:
        output_description = translate_client.translate(text_shortdescription, target_language=language_translate)
        yachttourcategoryinfomations_ShortDescriptions_Translated.append(output_description['translatedText'])

# add 1 new columns translated
yachttourcategoryinfomations['ShortDescriptions_Translated'] = yachttourcategoryinfomations_ShortDescriptions_Translated

#################################################################

# YachtInformationDetails
yachtinformationdetails_Title_Translated = []
yachtinformationdetails_ShortDescriptions_Translated = []
yachtinformationdetails_FullDescriptions_Translated = []
for index, row in yachtinformationdetails.iterrows():
    language_translate = row['language_translate']
    text_title = row['Title']
    text_shortdescription = row['ShortDescriptions']
    text_fulldescription = row['FullDescriptions']

    # process NULL data in FullDescriptions column
    if text_title == None:
        yachtinformationdetails_Title_Translated.append(text_title)
    else:
        output_description = translate_client.translate(text_title, target_language=language_translate)
        yachtinformationdetails_Title_Translated.append(output_description['translatedText'])

    if text_shortdescription == None:
        yachtinformationdetails_ShortDescriptions_Translated.append(text_shortdescription)
    else:
        output_description = translate_client.translate(text_shortdescription, target_language=language_translate)
        yachtinformationdetails_ShortDescriptions_Translated.append(output_description['translatedText'])

    if text_fulldescription == None:
        yachtinformationdetails_FullDescriptions_Translated.append(text_fulldescription)
    else:
        output_description = translate_client.translate(text_fulldescription, target_language=language_translate)
        yachtinformationdetails_FullDescriptions_Translated.append(output_description['translatedText'])
# add 1 new columns translated
yachtinformationdetails['Title_Translated'] = yachtinformationdetails_Title_Translated
yachtinformationdetails['ShortDescriptions_Translated'] = yachtinformationdetails_ShortDescriptions_Translated
yachtinformationdetails['FullDescriptions_Translated'] = yachtinformationdetails_FullDescriptions_Translated

#################################################################

# YachtMerchantInformationDetails
yachtmerchantinformationdetails_Title_Translated = []
yachtmerchantinformationdetails_ShortDescriptions_Translated = []
yachtmerchantinformationdetails_FullDescriptions_Translated = []
for index, row in yachtmerchantinformationdetails.iterrows():
    language_translate = row['language_translate']
    text_title = row['Title']
    text_shortdescription = row['ShortDescriptions']
    text_fulldescription = row['FullDescriptions']

    # process NULL data in FullDescriptions column
    if text_title == None:
        yachtmerchantinformationdetails_Title_Translated.append(text_title)
    else:
        output_description = translate_client.translate(text_title, target_language=language_translate)
        yachtmerchantinformationdetails_Title_Translated.append(output_description['translatedText'])

    if text_shortdescription == None:
        yachtmerchantinformationdetails_ShortDescriptions_Translated.append(text_shortdescription)
    else:
        output_description = translate_client.translate(text_shortdescription, target_language=language_translate)
        yachtmerchantinformationdetails_ShortDescriptions_Translated.append(output_description['translatedText'])

    if text_fulldescription == None:
        yachtmerchantinformationdetails_FullDescriptions_Translated.append(text_fulldescription)
    else:
        output_description = translate_client.translate(text_fulldescription, target_language=language_translate)
        yachtmerchantinformationdetails_FullDescriptions_Translated.append(output_description['translatedText'])
# add 1 new columns translated
yachtmerchantinformationdetails['Title_Translated'] = yachtmerchantinformationdetails_Title_Translated
yachtmerchantinformationdetails['ShortDescriptions_Translated'] = yachtmerchantinformationdetails_ShortDescriptions_Translated
yachtmerchantinformationdetails['FullDescriptions_Translated'] = yachtmerchantinformationdetails_FullDescriptions_Translated

#################################################################

# YachtTourInformationDetails
yachttourinformationdetails_Title_Translated = []
yachttourinformationdetails_ShortDescriptions_Translated = []
yachttourinformationdetails_FullDescriptions_Translated = []
for index, row in yachttourinformationdetails.iterrows():
    language_translate = row['language_translate']
    text_title = row['Title']
    text_shortdescription = row['ShortDescriptions']
    text_fulldescription = row['FullDescriptions']

    # process NULL data in FullDescriptions column
    if text_title == None:
        yachttourinformationdetails_Title_Translated.append(text_title)
    else:
        output_description = translate_client.translate(text_title, target_language=language_translate)
        yachttourinformationdetails_Title_Translated.append(output_description['translatedText'])

    if text_shortdescription == None:
        yachttourinformationdetails_ShortDescriptions_Translated.append(text_shortdescription)
    else:
        output_description = translate_client.translate(text_shortdescription, target_language=language_translate)
        yachttourinformationdetails_ShortDescriptions_Translated.append(output_description['translatedText'])

    if text_fulldescription == None:
        yachttourinformationdetails_FullDescriptions_Translated.append(text_fulldescription)
    else:
        output_description = translate_client.translate(text_fulldescription, target_language=language_translate)
        yachttourinformationdetails_FullDescriptions_Translated.append(output_description['translatedText'])
# add 1 new columns translated
yachttourinformationdetails['Title_Translated'] = yachttourinformationdetails_Title_Translated
yachttourinformationdetails['ShortDescriptions_Translated'] = yachttourinformationdetails_ShortDescriptions_Translated
yachttourinformationdetails['FullDescriptions_Translated'] = yachttourinformationdetails_FullDescriptions_Translated

#################################################################

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

if yachtroutemultilanguages.empty == False:
    cursor.execute(
        f'''INSERT INTO [AQ_Configurations].[dbo].[Translate_Tracking_Log]([Database_Name],
                                                                                    [Table_Name],
                                                                                    [LastTranlastedDate]) 
                    VALUES (?,?,getdate())
                ''',
        databasename,
        exception_tables[-1]
    )

cnxn.commit()
cursor.close()
