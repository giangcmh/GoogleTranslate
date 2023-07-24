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

# declare dataframe
flightcancelpolimutillang = pd.DataFrame()
flightpaymentpolimutillang = pd.DataFrame()

#exception table
flightinformationdetails = pd.DataFrame()

# declare database name and table name
databasename = 'AQ_PrivateJet'
tables = ['FlightCancelPoliMutilLang','FlightPaymentPoliMutilLang']
exception_tables = ['FlightInformationDetails']
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

# FlightCancelPoliMutilLang
for key, value in language.items():
    ###Insert / 1 as is_insert
    flightcancelpolimutillang_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[FlightCancelPoliMutilLang] A 
            where	LanguageFID = 1 
            and     not exists (select 1 
                                from [dbo].[FlightCancelPoliMutilLang] B 
                                where A.CancellationPoliciFID = B.CancellationPoliciFID 
                                and B.LanguageFID = {key}) 
            and Deleted = 0

        '''
        , cnxn, index_col='ID')
    flightcancelpolimutillang = pd.concat([flightcancelpolimutillang, flightcancelpolimutillang_insert], ignore_index=True)

    ###Update / 0 as is_insert
    flightcancelpolimutillang_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[FlightCancelPoliMutilLang] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, CreatedDate),102)
                <= '{translate_date[0]}'

        '''
        , cnxn, index_col='ID')
    flightcancelpolimutillang = pd.concat([flightcancelpolimutillang, flightcancelpolimutillang_update], ignore_index=True)

# check insert into track log
if flightcancelpolimutillang.empty:
    insert_track.append(0)
else:
    insert_track.append(1)

# FlightPaymentPoliMutilLang
for key, value in language.items():
    ###Insert / 1 as is_insert
    flightpaymentpolimutillang_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[FlightPaymentPoliMutilLang] A 
            where	LanguageFID = 1 
            and     not exists (select 1 
                                from [dbo].[FlightPaymentPoliMutilLang] B 
                                where A.PaymentPoliciFID = B.PaymentPoliciFID 
                                and B.LanguageFID = {key}) 
            and Deleted = 0

        '''
        , cnxn, index_col='ID')
    flightpaymentpolimutillang = pd.concat([flightpaymentpolimutillang, flightpaymentpolimutillang_insert], ignore_index=True)

    ###Update / 0 as is_insert
    flightpaymentpolimutillang_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[FlightPaymentPoliMutilLang] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, CreatedDate),102)
                <= '{translate_date[1]}'

        '''
        , cnxn, index_col='ID')
    flightpaymentpolimutillang = pd.concat([flightpaymentpolimutillang, flightpaymentpolimutillang_update], ignore_index=True)

#exception table
# FlightInformationDetails
for key, value in language.items():
    ###Insert / 1 as is_insert
    flightinformationdetails_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from    [dbo].[FlightInformationDetails] A 
            where	LanguageFID = 1 
            and     not exists (select 1 
                                from [dbo].[FlightInformationDetails] B 
                                where A.InformationFID = B.InformationFID 
                                and B.LanguageFID = {key}) 
            and Deleted = 0

        '''
        , cnxn, index_col='ID')
    flightinformationdetails = pd.concat([flightinformationdetails, flightinformationdetails_insert], ignore_index=True)

    ###Update / 0 as is_insert
    flightinformationdetails_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[FlightInformationDetails] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, ActivatedDate),102)
                <= '{exception_translate_date[0]}'

        '''
        , cnxn, index_col='ID')
    flightinformationdetails = pd.concat([flightinformationdetails, flightinformationdetails_update], ignore_index=True)

# check insert into track log
if flightinformationdetails.empty:
    exception_insert_track.append(0)
else:
    exception_insert_track.append(1)

################################################################# Call Function Translate

# FlightCancelPoliMutilLang
flightcancelpolimutillang_Name_Translated = []
flightcancelpolimutillang_Term_Translated = []
for index, row in flightcancelpolimutillang.iterrows():
    language_translate = row['language_translate']
    text_name = row['Name']
    text_term = row['TermAndPolicies']

    # process NULL data in FullDescriptions column
    if text_name == None:
        flightcancelpolimutillang_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=language_translate)
        flightcancelpolimutillang_Name_Translated.append(output_description['translatedText'])

    if text_term == None:
        flightcancelpolimutillang_Term_Translated.append(text_term)
    else:
        output_description = translate_client.translate(text_term, target_language=language_translate)
        flightcancelpolimutillang_Term_Translated.append(output_description['translatedText'])
# add 1 new columns translated
flightcancelpolimutillang['Name_Translated'] = flightcancelpolimutillang_Name_Translated
flightcancelpolimutillang['Term_Translated'] = flightcancelpolimutillang_Term_Translated

#################################################################

# FlightInformationDetails
flightinformationdetails_Title_Translated = []
flightinformationdetails_ShortDescriptions_Translated = []
flightinformationdetails_FullDescriptions_Translated = []
for index, row in flightinformationdetails.iterrows():
    language_translate = row['language_translate']
    text_title = row['Title']
    text_shortdescription = row['ShortDescriptions']
    text_fulldescription = row['FullDescriptions']

    # process NULL data in FullDescriptions column
    if text_title == None:
        flightinformationdetails_Title_Translated.append(text_title)
    else:
        output_description = translate_client.translate(text_title, target_language=language_translate)
        flightinformationdetails_Title_Translated.append(output_description['translatedText'])

    if text_shortdescription == None:
        flightinformationdetails_ShortDescriptions_Translated.append(text_shortdescription)
    else:
        output_description = translate_client.translate(text_shortdescription, target_language=language_translate)
        flightinformationdetails_ShortDescriptions_Translated.append(output_description['translatedText'])

    if text_fulldescription == None:
        flightinformationdetails_FullDescriptions_Translated.append(text_fulldescription)
    else:
        output_description = translate_client.translate(text_fulldescription, target_language=language_translate)
        flightinformationdetails_FullDescriptions_Translated.append(output_description['translatedText'])
# add 1 new columns translated
flightinformationdetails['Title_Translated'] = flightinformationdetails_Title_Translated
flightinformationdetails['ShortDescriptions_Translated'] = flightinformationdetails_ShortDescriptions_Translated
flightinformationdetails['FullDescriptions_Translated'] = flightinformationdetails_FullDescriptions_Translated

#################################################################

# FlightPaymentPoliMutilLang
flightpaymentpolimutillang_Name_Translated = []
flightpaymentpolimutillang_Term_Translated = []
for index, row in flightpaymentpolimutillang.iterrows():
    language_translate = row['language_translate']
    text_name = row['Name']
    text_term = row['TermAndPolicies']

    # process NULL data in FullDescriptions column
    if text_name == None:
        flightpaymentpolimutillang_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=language_translate)
        flightpaymentpolimutillang_Name_Translated.append(output_description['translatedText'])

    if text_term == None:
        flightpaymentpolimutillang_Term_Translated.append(text_term)
    else:
        output_description = translate_client.translate(text_term, target_language=language_translate)
        flightpaymentpolimutillang_Term_Translated.append(output_description['translatedText'])
# add 1 new columns translated
flightpaymentpolimutillang['Name_Translated'] = flightpaymentpolimutillang_Name_Translated
flightpaymentpolimutillang['Term_Translated'] = flightpaymentpolimutillang_Term_Translated



################################################################# Insert dataframe into tables
cursor = cnxn.cursor()

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
