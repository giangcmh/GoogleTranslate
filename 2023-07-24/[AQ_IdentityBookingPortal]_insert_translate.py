import os
from google.cloud import translate_v2
import pyodbc
import pandas as pd
import warnings

warnings.filterwarnings('ignore')

#################################################################
# init credentials google translate api
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\PythonProject\GoogleTranslate\aqtranslate_key.json"

# init connection SQL Server
cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                         "Server=69.172.67.3,1400;"
                         "Database=AQ_IdentityBookingPortal;"
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

helpcontentsdetails = pd.DataFrame()
helptypededatails = pd.DataFrame()

################################################################# Get dataframe

# HelpContentsDetails
for key, value in language.items():
    ###Insert / 1 as is_insert
    caradditionalservicedetails_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[HelpContentsDetails] A 
            where	LanguageFID = 1 
            and     not exists (select 1 
                                from [dbo].[HelpContentsDetails] B 
                                where A.HelpContentFID = B.HelpContentFID 
                                and B.LanguageFID = {key}) 
            and Deleted = 0
        '''
        , cnxn, index_col='ID')
    helpcontentsdetails = pd.concat([helpcontentsdetails, caradditionalservicedetails_insert], ignore_index=True)

    ###Update / 0 as is_insert
    caradditionalservicedetails_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[HelpContentsDetails] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)
        '''
        , cnxn, index_col='ID')
    helpcontentsdetails = pd.concat([helpcontentsdetails, caradditionalservicedetails_update], ignore_index=True)


# HelpTypeDedatails
for key, value in language.items():
    ###Insert / 1 as is_insert
    caradditionalservicedetails_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[HelpTypeDedatails] A 
            where	LanguageFID = 1 
            and     not exists (select 1 
                                from [dbo].[HelpTypeDedatails] B 
                                where A.HelpTypeFID = B.HelpTypeFID 
                                and B.LanguageFID = {key}) 
            and Deleted = 0
        '''
        , cnxn, index_col='ID')
    helptypededatails = pd.concat([helptypededatails, caradditionalservicedetails_insert], ignore_index=True)

    ###Update / 0 as is_insert
    caradditionalservicedetails_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[HelpTypeDedatails] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)
        '''
        , cnxn, index_col='ID')
    helptypededatails = pd.concat([helptypededatails, caradditionalservicedetails_update], ignore_index=True)



################################################################# Call Function Translate

# HelpContentsDetails
helpcontentsdetails_Title_Translated = []
helpcontentsdetails_FullDescriptions_Translated = []
for index, row in helpcontentsdetails.iterrows():
    language_translate = row['language_translate']
    text_title = row['Title']
    text_fulldescription = row['FullDescriptions']

    # process NULL data in FullDescriptions column
    if text_title == None:
        helpcontentsdetails_Title_Translated.append(text_title)
    else:
        output_description = translate_client.translate(text_title, target_language=language_translate)
        helpcontentsdetails_Title_Translated.append(output_description['translatedText'])

    if text_fulldescription == None:
        helpcontentsdetails_FullDescriptions_Translated.append(text_fulldescription)
    else:
        output_description = translate_client.translate(text_fulldescription, target_language=language_translate)
        helpcontentsdetails_FullDescriptions_Translated.append(output_description['translatedText'])
# add 1 new columns translated

helpcontentsdetails['Title_Translated'] = helpcontentsdetails_Title_Translated
helpcontentsdetails['FullDescriptions_Translated'] = helpcontentsdetails_FullDescriptions_Translated


#################################################################

# HelpTypeDedatails
helptypededatails_Description_Translated = []
for index, row in helptypededatails.iterrows():
    language_translate = row['language_translate']
    text_description = row['Name']

    # process NULL data in FullDescriptions column
    if text_description == None:
        helptypededatails_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=language_translate)
        helptypededatails_Description_Translated.append(output_description['translatedText'])
# add 1 new columns translated

helptypededatails['Description_Translated'] = helptypededatails_Description_Translated

################################################################# Insert dataframe into tables

cursor = cnxn.cursor()

# HelpContentsDetails
for index, row in helpcontentsdetails.iterrows():
    if row.is_insert == 1:
        cursor.execute(
            '''INSERT INTO [dbo].[HelpContentsDetails]([LanguageFID],
                                                        [HelpContentFID],
                                                        [Title],
                                                        [ShortDescriptions],
                                                        [FullDescriptions],
                                                        [Deleted],
                                                        [IsActived],
                                                        [CreatedBy],
                                                        [CreatedDate],
                                                        [LastModifiedBy],
                                                        [LastModifiedDate]) 
                VALUES (?,?,?,?,?,?,?,?,getdate(),?,getdate()-1)
            ''',
            row.language_translate,
            row.HelpContentFID,
            row.Title_Translated,
            row.ShortDescriptions,
            row.FullDescriptions_Translated,
            row.Deleted,
            row.IsActived,
            row.CreatedBy,
            row.LastModifiedBy
            )
    else:
        cursor.execute(
            ''' UPDATE [dbo].[HelpContentsDetails] 
                SET	    Title = ?, 
                        FullDescriptions = ?, 
                        LastModifiedDate = ? 
                WHERE   HelpContentFID = ? and LanguageFID = ?
            ''',
            row.Title_Translated,
            row.FullDescriptions_Translated,
            row.LastModifiedDate,
            row.HelpContentFID,
            row.language_translate
        )


# HelpTypeDedatails
for index, row in helpcontentsdetails.iterrows():
    if row.is_insert == 1:
        cursor.execute(
            '''INSERT INTO [dbo].[HelpTypeDedatails]([LanguageFID],
                                                    [HelpTypeFID],
                                                    [Name],
                                                    [Remarks],
                                                    [Deleted],
                                                    [IsActived],
                                                    [CreatedBy],
                                                    [CreatedDate],
                                                    [LastModifiedBy],
                                                    [LastModifiedDate]) 
                VALUES (?,?,?,?,?,?,?,getdate(),?,getdate()-1)
            ''',
            row.language_translate,
            row.HelpTypeFID,
            row.Description_Translated,
            row.Remarks,
            row.Deleted,
            row.IsActived,
            row.CreatedBy,
            row.LastModifiedBy
            )
    else:
        cursor.execute(
            ''' UPDATE [dbo].[HelpTypeDedatails] 
                SET	    Name = ?, 
                        LastModifiedDate = ? 
                WHERE   HelpTypeFID = ? 
                and     LanguageFID = ?
            ''',
            row.Description_Translated,
            row.LastModifiedDate,
            row.HelpTypeFID,
            row.language_translate
        )


cnxn.commit()
cursor.close()


















