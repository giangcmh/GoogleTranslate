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
                         "Database=AQ_CMS;"
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
postcategorydetails = pd.DataFrame()

#exception table
postdetails = pd.DataFrame()

# declare database name and table name
databasename = 'AQ_CMS'
tables = ['PostCategoryDetails']
exception_tables = ['PostDetails']
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

# exception table
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

# PostCategoryDetails
for key, value in language.items():
    ###Insert / 1 as is_insert
    postcategorydetails_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[PostCategoryDetails] A 
            where	LanguageFID = 1 
            and     not exists (select 1 
                                from [dbo].[PostCategoryDetails] B 
                                where A.PostCategoryFID = B.PostCategoryFID 
                                and B.LanguageFID = {key}) 
            and     Deleted = 0

        '''
        , cnxn, index_col='ID')
    postcategorydetails = pd.concat([postcategorydetails, postcategorydetails_insert], ignore_index=True)

    ###Update / 0 as is_insert
    postcategorydetails_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[PostCategoryDetails] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, CreatedDate),102)
                <= '{translate_date[0]}'

        '''
        , cnxn, index_col='ID')
    postcategorydetails = pd.concat([postcategorydetails, postcategorydetails_update], ignore_index=True)

# check insert into track log
if postcategorydetails.empty:
    insert_track.append(0)
else:
    insert_track.append(1)

# PostDetails
for key, value in language.items():
    ###Insert / 1 as is_insert
    postdetails_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[PostDetails] A 
            where	LanguageFID = 1 
            and not exists (select 1 
                            from [dbo].[PostDetails] B 
                            where A.PostFId = B.PostFId 
                            and B.LanguageFID = {key}) 
            and     Deleted = 0

        '''
        , cnxn, index_col='ID')
    postdetails = pd.concat([postdetails, postdetails_insert], ignore_index=True)

    ###Update / 0 as is_insert
    postdetails_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[PostDetails] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), COALESCE(LastModifiedDate, ActivatedDate),102)
                <= '{exception_translate_date[0]}'

        '''
        , cnxn, index_col='ID')
    postdetails = pd.concat([postdetails, postdetails_update], ignore_index=True)

# check insert into track log
if postdetails.empty:
    exception_insert_track.append(0)
else:
    exception_insert_track.append(1)

############################################################## Call Function Translate

# PostCategoryDetails
postcategorydetails_Description_Translated = []
for index, row in postcategorydetails.iterrows():
    language_translate = row['language_translate']
    text_description = row['Name']

    # process NULL data in FullDescriptions column
    if text_description == None:
        postcategorydetails_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=language_translate)
        postcategorydetails_Description_Translated.append(output_description['translatedText'])
# add 1 new columns translated
postcategorydetails['Description_Translated'] = postcategorydetails_Description_Translated

# PostDetails
postdetails_Title_Translated = []
postdetails_MetaDescription_Translated = []
postdetails_Body_Translated = []
postdetails_ShortDescription_Translated = []
for index, row in postdetails.iterrows():
    language_translate = row['language_translate']
    text_title = row['Title']
    text_metadescription = row['MetaDescription']
    text_body = row['Body']
    text_shortdescription = row['ShortDescription']

    # process NULL data
    if text_title == None:
        postdetails_Title_Translated.append(text_title)
    else:
        output_description = translate_client.translate(text_title, target_language=language_translate)
        postdetails_Title_Translated.append(output_description['translatedText'])

    # process NULL data
    if text_metadescription == None:
        postdetails_MetaDescription_Translated.append(text_metadescription)
    else:
        output_description = translate_client.translate(text_metadescription, target_language=language_translate)
        postdetails_MetaDescription_Translated.append(output_description['translatedText'])

    # process NULL data
    if text_body == None:
        postdetails_Body_Translated.append(text_body)
    else:
        output_description = translate_client.translate(text_body, target_language=language_translate)
        postdetails_Body_Translated.append(output_description['translatedText'])

    # process NULL data
    if text_shortdescription == None:
        postdetails_ShortDescription_Translated.append(text_shortdescription)
    else:
        output_description = translate_client.translate(text_shortdescription, target_language=language_translate)
        postdetails_ShortDescription_Translated.append(output_description['translatedText'])
# add 1 new columns translated
postdetails['Title_Translated'] = postdetails_Title_Translated
postdetails['MetaDescription_Translated'] = postdetails_MetaDescription_Translated
postdetails['Body_Translated'] = postdetails_Body_Translated
postdetails['ShortDescription_Translated'] = postdetails_ShortDescription_Translated


################################################################# Insert dataframe into tables
cursor = cnxn.cursor()

# postcategorydetails
for index, row in postcategorydetails.iterrows():
    if row.is_insert == 1:
        cursor.execute(
            '''INSERT INTO [dbo].[PostCategoryDetails]([PostCategoryFID],
                                                        [Name],
                                                        [LanguageFID],
                                                        [Deleted],
                                                        [IsActivated],
                                                        [CreatedBy],
                                                        [CreatedDate],
                                                        [LastModifiedBy],
                                                        [LastModifiedDate],
                                                        [FriendlyUrl]) 
                VALUES (?,?,?,?,?,?,getdate(),?,getdate()-1,?)
            ''',
            row.PostCategoryFID,
            row.Description_Translated,
            row.translatefid,
            row.Deleted,
            row.IsActivated,
            row.CreatedBy,
            row.LastModifiedBy,
            row.FriendlyUrl
            )
    else:
        cursor.execute(
            ''' UPDATE [dbo].[PostCategoryDetails] 
                SET	    Name = ?, 
                        LastModifiedDate = getdate() 
                WHERE   PostCategoryFID = ? 
                and     LanguageFID = ?
            ''',
            row.Description_Translated,
            row.PostCategoryFID,
            row.translatefid
        )
        # update language en-US = getdate
        cursor.execute(
            ''' UPDATE [dbo].[PostCategoryDetails] 
                SET	    LastModifiedDate = getdate()
                WHERE   PostCategoryFID = ? 
                and     LanguageFID = ?
            ''',
            row.PostCategoryFID,
            1  # language en-US
        )


# postdetails
for index, row in postdetails.iterrows():
    if row.is_insert == 1:
        cursor.execute(
            '''INSERT INTO [dbo].[PostDetails]([UniqueId],
                                                [PostFId],
                                                [LanguageFID],
                                                [FileTypeFID],
                                                [FileStreamFID],
                                                [Title],
                                                [MetaDescription],
                                                [Body],
                                                [KeyWord],
                                                [Deleted],
                                                [IsActivated],
                                                [ActivatedDate],
                                                [ActivatedBy],
                                                [LastModifiedBy],
                                                [LastModifiedDate],
                                                [ShortDescription],
                                                [FriendlyUrl]) 
                VALUES (?,?,?,?,?,?,?,?,?,?,?,getdate(),?,?,getdate()-1,?,?)
            ''',
            row.UniqueId,
            row.PostFId,
            row.translatefid,
            row.FileTypeFID,
            row.FileStreamFID,
            row.Title_Translated,
            row.MetaDescription_Translated,
            row.Body_Translated,
            row.KeyWord,
            row.Deleted,
            row.IsActivated,
            row.ActivatedBy,
            row.LastModifiedBy,
            row.ShortDescription_Translated,
            row.FriendlyUrl
            )
    else:
        cursor.execute(
            ''' UPDATE [dbo].[PostDetails] 
                SET	    Title = ?, 
                        MetaDescription = ?, 
                        Body = ?, 
                        ShortDescription = ?, 
                        LastModifiedDate = getdate()  
                WHERE   PostFId = ? 
                and     LanguageFID = ?
            ''',
            row.Title_Translated,
            row.MetaDescription_Translated,
            row.Body_Translated,
            row.ShortDescription_Translated,
            row.PostFId,
            row.translatefid
        )
        # update language en-US = getdate
        cursor.execute(
            ''' UPDATE [dbo].[PostDetails] 
                SET	    LastModifiedDate = getdate()
                WHERE   PostFId = ? 
                and     LanguageFID = ?
            ''',
            row.PostFId,
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
