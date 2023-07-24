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
                         "Database=AQ_CarRental;"
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
caradditionalservicedetails = pd.DataFrame()
carcancelpolimutillang = pd.DataFrame()
carpaymentpolimutillang = pd.DataFrame()
ratingattributedetails = pd.DataFrame()

#exception table
carinformationdetails = pd.DataFrame()

# declare database name and table name
databasename = 'AQ_CarRental'
tables = ['CarAdditionalServiceDetails','CarCancelPoliMutilLang', 'CarPaymentPoliMutilLang', 'RatingAttributeDetails']
exception_tables = ['CarInformationDetails']
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

# CarAdditionalServiceDetails
for key, value in language.items():
    ###Insert / 1 as is_insert
    caradditionalservicedetails_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[CarAdditionalServiceDetails] A 
            where	LanguageFID = 1 
            and     not exists (select 1 
                                from [dbo].[CarAdditionalServiceDetails] B 
                                where A.AdditionalServiceFID = B.AdditionalServiceFID 
                                and B.LanguageFID = {key}) 
            and Deleted = 0

        '''
        , cnxn, index_col='ID')
    caradditionalservicedetails = pd.concat([caradditionalservicedetails, caradditionalservicedetails_insert], ignore_index=True)

    ###Update / 0 as is_insert
    caradditionalservicedetails_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[CarAdditionalServiceDetails] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, CreatedDate),102)
                <= '{translate_date[0]}'

        '''
        , cnxn, index_col='ID')
    caradditionalservicedetails = pd.concat([caradditionalservicedetails, caradditionalservicedetails_update], ignore_index=True)

# check insert into track log
if caradditionalservicedetails.empty:
    insert_track.append(0)
else:
    insert_track.append(1)

# CarCancelPoliMutilLang
for key, value in language.items():
    ###Insert / 1 as is_insert
    carcancelpolimutillang_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[CarCancelPoliMutilLang] A 
            where	LanguageFID = 1 
            and     not exists (select 1 
                                from [dbo].[CarCancelPoliMutilLang] B 
                                where A.CancellationPoliciFID = B.CancellationPoliciFID 
                                and B.LanguageFID = {key}) 
            and Deleted = 0

        '''
        , cnxn, index_col='ID')
    carcancelpolimutillang = pd.concat([carcancelpolimutillang, carcancelpolimutillang_insert], ignore_index=True)

    ###Update / 0 as is_insert
    carcancelpolimutillang_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[CarCancelPoliMutilLang] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, CreatedDate),102)
                <= '{translate_date[1]}'

        '''
        , cnxn, index_col='ID')
    carcancelpolimutillang = pd.concat([carcancelpolimutillang, carcancelpolimutillang_update], ignore_index=True)

# check insert into track log
if carcancelpolimutillang.empty:
    insert_track.append(0)
else:
    insert_track.append(1)

# CarPaymentPoliMutilLang
for key, value in language.items():
    ###Insert / 1 as is_insert
    carpaymentpolimutillang_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[CarPaymentPoliMutilLang] A 
            where	LanguageFID = 1 
            and     not exists (select 1 
                                from [dbo].[CarPaymentPoliMutilLang] B 
                                where A.PaymentPoliciFID = B.PaymentPoliciFID 
                                and B.LanguageFID = {key}) 
            and Deleted = 0

        '''
        , cnxn, index_col='ID')
    carpaymentpolimutillang = pd.concat([carpaymentpolimutillang, carpaymentpolimutillang_insert], ignore_index=True)

    ###Update / 0 as is_insert
    carpaymentpolimutillang_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[CarPaymentPoliMutilLang] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, CreatedDate),102)
                <= '{translate_date[2]}'

        '''
        , cnxn, index_col='ID')
    carpaymentpolimutillang = pd.concat([carpaymentpolimutillang, carpaymentpolimutillang_update], ignore_index=True)

# check insert into track log
if carpaymentpolimutillang.empty:
    insert_track.append(0)
else:
    insert_track.append(1)

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
                <= '{translate_date[3]}'

        '''
        , cnxn, index_col='ID')
    ratingattributedetails = pd.concat([ratingattributedetails, ratingattributedetails_update], ignore_index=True)

# check insert into track log
if ratingattributedetails.empty:
    insert_track.append(0)
else:
    insert_track.append(1)

# CarInformationDetails
for key, value in language.items():
    ###Insert / 1 as is_insert
    carinformationdetails_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,    
                    * 
            from	[dbo].[CarInformationDetails] A 
            where	LanguageFID = 1 and not exists (select 1 
                                                    from [dbo].[CarInformationDetails] B 
                                                    where A.InformationFID = B.InformationFID 
                                                    and B.LanguageFID = {key}) 
            and Deleted = 0

        '''
        , cnxn, index_col='ID')
    carinformationdetails = pd.concat([carinformationdetails, carinformationdetails_insert], ignore_index=True)

    ###Update / 0 as is_insert
    carinformationdetails_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[CarInformationDetails] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, ActivatedDate),102)
                <= '{exception_translate_date[0]}'

        '''
        , cnxn, index_col='ID')
    carinformationdetails = pd.concat([carinformationdetails, carinformationdetails_update], ignore_index=True)

# check insert into track log
if carinformationdetails.empty:
    exception_insert_track.append(0)
else:
    exception_insert_track.append(1)

##############################################################  Call Function Translate

# CarAdditionalServiceDetails
caradditionalservicedetails_Description_Translated = []
for index, row in caradditionalservicedetails.iterrows():
    language_translate = row['language_translate']
    text_description = row['Name']

    # process NULL data in FullDescriptions column
    if text_description == None:
        caradditionalservicedetails_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=language_translate)
        caradditionalservicedetails_Description_Translated.append(output_description['translatedText'])
# add 1 new columns translated
caradditionalservicedetails['Description_Translated'] = caradditionalservicedetails_Description_Translated

#################################################################

# CarCancelPoliMutilLang
carcancelpolimutillang_Description_Translated = []
carcancelpolimutillang_termandpolicies_Translated = []
for index, row in carcancelpolimutillang.iterrows():
    language_translate = row['language_translate']
    text_description = row['Name']
    text_termandpolicies = row['TermAndPolicies']

    # process NULL data in FullDescriptions column
    if text_description == None:
        carcancelpolimutillang_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=language_translate)
        carcancelpolimutillang_Description_Translated.append(output_description['translatedText'])

    # process NULL data in FullDescriptions column
    if text_termandpolicies == None:
        carcancelpolimutillang_termandpolicies_Translated.append(text_termandpolicies)
    else:
        output_description = translate_client.translate(text_termandpolicies, target_language=language_translate)
        carcancelpolimutillang_termandpolicies_Translated.append(output_description['translatedText'])
# add 1 new columns translated
carcancelpolimutillang['Description_Translated'] = carcancelpolimutillang_Description_Translated
carcancelpolimutillang['TermAndPolicies_Translated'] = carcancelpolimutillang_termandpolicies_Translated

#################################################################

# CarInformationDetails
carinformationdetails_Title_Translated = []
carinformationdetails_Description_Translated = []
carinformationdetails_ShortDescription_Translated = []
for index, row in carinformationdetails.iterrows():
    language_translate = row['language_translate']
    text_title = row['Title']
    text_description = row['FullDescriptions']
    text_shortdescription = row['ShortDescriptions']
    # process NULL data in Title column
    if text_title == None:
        carinformationdetails_Title_Translated.append(text_title)
    else:
        output_title = translate_client.translate(text_title, target_language=language_translate)
        carinformationdetails_Title_Translated.append(output_title['translatedText'])

    # process NULL data in FullDescriptions column
    if text_description == None:
        carinformationdetails_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=language_translate)
        carinformationdetails_Description_Translated.append(output_description['translatedText'])

    # process NULL data in ShortDescriptions column
    if text_shortdescription == None:
        carinformationdetails_ShortDescription_Translated.append(text_shortdescription)
    else:
        output_shortdescription = translate_client.translate(text_shortdescription, target_language=language_translate)
        carinformationdetails_ShortDescription_Translated.append(output_shortdescription['translatedText'])
# add 3 new columns translated
carinformationdetails['Title_Translated'] = carinformationdetails_Title_Translated
carinformationdetails['Description_Translated'] = carinformationdetails_Description_Translated
carinformationdetails['ShortDescription_Translated'] = carinformationdetails_ShortDescription_Translated

#################################################################

# CarPaymentPoliMutilLang
carpaymentpolimutillang_Description_Translated = []
carpaymentpolimutillang_termandpolicies_Translated = []
for index, row in carpaymentpolimutillang.iterrows():
    language_translate = row['language_translate']
    text_description = row['Name']
    text_termandpolicies = row['TermAndPolicies']

    # process NULL data in FullDescriptions column
    if text_description == None:
        carpaymentpolimutillang_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=language_translate)
        carpaymentpolimutillang_Description_Translated.append(output_description['translatedText'])

    # process NULL data in FullDescriptions column
    if text_termandpolicies == None:
        carpaymentpolimutillang_termandpolicies_Translated.append(text_termandpolicies)
    else:
        output_description = translate_client.translate(text_termandpolicies, target_language=language_translate)
        carpaymentpolimutillang_termandpolicies_Translated.append(output_description['translatedText'])
# add 1 new columns translated
carpaymentpolimutillang['Description_Translated'] = carpaymentpolimutillang_Description_Translated
carpaymentpolimutillang['TermAndPolicies_Translated'] = carpaymentpolimutillang_termandpolicies_Translated

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



################################################################# Insert dataframe into tables
cursor = cnxn.cursor()

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
            row.Description_Translated,
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
            row.Description_Translated,
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
            row.Description_Translated,
            row.TermAndPolicies_Translated,
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
            row.Description_Translated,
            row.TermAndPolicies_Translated,
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
            row.Description_Translated,
            row.TermAndPolicies_Translated,
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
            row.Description_Translated,
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
            ''' UPDATE [dbo].[CarInformationDetails] 
                SET     Title = ?, 
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
            ''' UPDATE [dbo].[CarInformationDetails] 
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
