import os
from google.cloud import translate_v2
import pyodbc
import pandas as pd
import warnings

warnings.filterwarnings('ignore')

#################################################################
# init credentials google translate api
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\PythonProject\GoogleTranslate\aqtranslate_key_new.json"

# init connection SQL Server
cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                         "Server=69.172.67.3,1400;"
                         "Database=AQ_Dining;"
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

ratingattributedetails = pd.DataFrame()
restaurantcancelpolimutillang = pd.DataFrame()
restaurantinformationdetails = pd.DataFrame()
restaurantotherinformations = pd.DataFrame()

################################################################# Get dataframe


# RatingAttributeDetails
for key, value in language.items():
    ###Insert / 1 as is_insert
    flightcancelpolimutillang_insert = pd.read_sql(
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
    ratingattributedetails = pd.concat([ratingattributedetails, flightcancelpolimutillang_insert], ignore_index=True)

    ###Update / 0 as is_insert
    flightcancelpolimutillang_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[RatingAttributeDetails] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)
        '''
        , cnxn, index_col='ID')
    ratingattributedetails = pd.concat([ratingattributedetails, flightcancelpolimutillang_update], ignore_index=True)


# RestaurantCancelPoliMutilLang
for key, value in language.items():
    ###Insert / 1 as is_insert
    restaurantcancelpolimutillang_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[RestaurantCancelPoliMutilLang] A 
            where	LanguageFID = 1 
            and     not exists (select 1 
                                from [dbo].[RestaurantCancelPoliMutilLang] B 
                                where A.CancellationPoliciFID = B.CancellationPoliciFID 
                                and B.LanguageFID = {key}) 
            and Deleted = 0
        '''
        , cnxn, index_col='ID')
    restaurantcancelpolimutillang = pd.concat([restaurantcancelpolimutillang, restaurantcancelpolimutillang_insert], ignore_index=True)

    ###Update / 0 as is_insert
    restaurantcancelpolimutillang_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[RestaurantCancelPoliMutilLang] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)
        '''
        , cnxn, index_col='ID')
    restaurantcancelpolimutillang = pd.concat([restaurantcancelpolimutillang, restaurantcancelpolimutillang_update], ignore_index=True)


# RestaurantInformationDetails
for key, value in language.items():
    ###Insert / 1 as is_insert
    restaurantinformationdetails_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[RestaurantInformationDetails] A 
            where	LanguageFID = 1 
            and     not exists (select 1 
                                from [dbo].[RestaurantInformationDetails] B 
                                where A.InformationFID = B.InformationFID 
                                and B.LanguageFID = {key}) 
            and Deleted = 0
        '''
        , cnxn, index_col='ID')
    restaurantinformationdetails = pd.concat([restaurantinformationdetails, restaurantinformationdetails_insert], ignore_index=True)

    ###Update / 0 as is_insert
    restaurantinformationdetails_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[RestaurantInformationDetails] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)
        '''
        , cnxn, index_col='ID')
    restaurantinformationdetails = pd.concat([restaurantinformationdetails, restaurantinformationdetails_update], ignore_index=True)


# RestaurantOtherInformations
for key, value in language.items():
    ###Insert / 1 as is_insert
    restaurantotherinformations_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[RestaurantOtherInformations] A 
            where	LanguageFID = 1 
            and     not exists (select 1 
                                from [dbo].[RestaurantOtherInformations] B 
                                where A.RestaurantFID = B.RestaurantFID 
                                and B.LanguageFID = {key}) 
            and Deleted = 0
        '''
        , cnxn, index_col='ID')
    restaurantotherinformations = pd.concat([restaurantotherinformations, restaurantotherinformations_insert], ignore_index=True)

    ###Update / 0 as is_insert
    restaurantotherinformations_update = pd.read_sql(
        f'''select	0 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[RestaurantOtherInformations] A 
            where	LanguageFID = 1 
            and     Deleted = 0 
            and     convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)
        '''
        , cnxn, index_col='ID')
    restaurantotherinformations = pd.concat([restaurantotherinformations, restaurantotherinformations_update], ignore_index=True)



################################################################# Call Function Translate

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

# RestaurantCancelPoliMutilLang
restaurantcancelpolimutillang_Description_Translated = []
restaurantcancelpolimutillang_termandpolicies_Translated = []
for index, row in restaurantcancelpolimutillang.iterrows():
    language_translate = row['language_translate']
    text_description = row['Name']
    text_termandpolicies = row['TermAndPolicies']

    # process NULL data in FullDescriptions column
    if text_description == None:
        restaurantcancelpolimutillang_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=language_translate)
        restaurantcancelpolimutillang_Description_Translated.append(output_description['translatedText'])

    # process NULL data in FullDescriptions column
    if text_termandpolicies == None:
        restaurantcancelpolimutillang_termandpolicies_Translated.append(text_termandpolicies)
    else:
        output_description = translate_client.translate(text_termandpolicies, target_language=language_translate)
        restaurantcancelpolimutillang_termandpolicies_Translated.append(output_description['translatedText'])
# add 1 new columns translated
restaurantcancelpolimutillang['Description_Translated'] = restaurantcancelpolimutillang_Description_Translated
restaurantcancelpolimutillang['TermAndPolicies_Translated'] = restaurantcancelpolimutillang_termandpolicies_Translated

#################################################################

# RestaurantInformationDetails
restaurantinformationdetails_Title_Translated = []
restaurantinformationdetails_Description_Translated = []
restaurantinformationdetails_ShortDescription_Translated = []
for index, row in restaurantinformationdetails.iterrows():
    language_translate = row['language_translate']
    text_title = row['Title']
    text_description = row['FullDescriptions']
    text_shortdescription = row['ShortDescriptions']
    # process NULL data in Title column
    if text_title == None:
        restaurantinformationdetails_Title_Translated.append(text_title)
    else:
        output_title = translate_client.translate(text_title, target_language=language_translate)
        restaurantinformationdetails_Title_Translated.append(output_title['translatedText'])

    # process NULL data in FullDescriptions column
    if text_description == None:
        restaurantinformationdetails_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=language_translate)
        restaurantinformationdetails_Description_Translated.append(output_description['translatedText'])

    # process NULL data in ShortDescriptions column
    if text_shortdescription == None:
        restaurantinformationdetails_ShortDescription_Translated.append(text_shortdescription)
    else:
        output_shortdescription = translate_client.translate(text_shortdescription, target_language=language_translate)
        restaurantinformationdetails_ShortDescription_Translated.append(output_shortdescription['translatedText'])
# add 3 new columns translated
restaurantinformationdetails['Title_Translated'] = restaurantinformationdetails_Title_Translated
restaurantinformationdetails['Description_Translated'] = restaurantinformationdetails_Description_Translated
restaurantinformationdetails['ShortDescription_Translated'] = restaurantinformationdetails_ShortDescription_Translated

#################################################################

# RestaurantOtherInformations
restaurantotherinformations_Title_Translated = []
restaurantotherinformations_Description_Translated = []
for index, row in restaurantotherinformations.iterrows():
    language_translate = row['language_translate']
    text_title = row['Title']
    text_description = row['Descriptions']

    # process NULL data in Title column
    if text_title == None:
        restaurantotherinformations_Title_Translated.append(text_title)
    else:
        output_title = translate_client.translate(text_title, target_language=language_translate)
        restaurantotherinformations_Title_Translated.append(output_title['translatedText'])

    # process NULL data in FullDescriptions column
    if text_description == None:
        restaurantotherinformations_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=language_translate)
        restaurantotherinformations_Description_Translated.append(output_description['translatedText'])

# add 3 new columns translated
restaurantotherinformations['Title_Translated'] = restaurantotherinformations_Title_Translated
restaurantotherinformations['Description_Translated'] = restaurantotherinformations_Description_Translated



##################################################### Insert dataframe into tables
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
            row.language_translate,
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
                        LastModifiedDate = ? 
                WHERE   RatingFID = ? 
                and LanguageFID = ?
            ''',
            row.Description_Translated,
            row.LastModifiedDate,
            row.RatingFID,
            row.language_translate
        )


# RestaurantCancelPoliMutilLang
for index, row in restaurantcancelpolimutillang.iterrows():
    if row.is_insert == 1:
        cursor.execute(
            '''INSERT INTO [dbo].[RestaurantCancelPoliMutilLang]([CancellationPoliciFID],
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
            row.language_translate,
            row.Description_Translated,
            row.TermAndPolicies_Translated,
            row.IsActive,
            row.Deleted,
            row.CreatedBy,
            row.LastModifiedBy
            )
    else:
        cursor.execute(
            ''' UPDATE [dbo].[RestaurantCancelPoliMutilLang] 
                SET	    Name = ?, 
                        TermAndPolicies = ?, 
                        LastModifiedDate = ? 
                WHERE   CancellationPoliciFID = ? 
                and     LanguageFID = ?
            ''',
            row.Description_Translated,
            row.TermAndPolicies_Translated,
            row.LastModifiedDate,
            row.CancellationPoliciFID,
            row.language_translate
        )


# RestaurantInformationDetails
for index, row in restaurantinformationdetails.iterrows():
    if row.is_insert == 1:
        cursor.execute(
            '''INSERT INTO [dbo].[RestaurantInformationDetails]([UniqueID],
                                                                [InformationFID],
                                                                [LanguageFID],
                                                                [LanguageResKey],
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
            row.language_translate,
            row.LanguageResKey,
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
            ''' UPDATE [dbo].[RestaurantInformationDetails] 
                SET     Title = ?, 
                        ShortDescriptions = ?, 
                        FullDescriptions = ?, 
                        LastModifiedDate = ? 
                WHERE   InformationFID = ? 
                and LanguageFID = ?
            ''',
            row.Title_Translated,
            row.ShortDescription_Translated,
            row.Description_Translated,
            row.LastModifiedDate,
            row.InformationFID,
            row.language_translate
        )


# RestaurantOtherInformations
for index, row in restaurantotherinformations.iterrows():
    if row.is_insert == 1:
        cursor.execute(
            '''INSERT INTO [dbo].[RestaurantOtherInformations]([RestaurantFID],
                                                                [UniqueID],
                                                                [InfoTypeFID],
                                                                [LanguageFID],
                                                                [LanguageResKey],
                                                                [FileTypeFID],
                                                                [FileStreamFID],
                                                                [Title],
                                                                [Descriptions],
                                                                [Deleted],
                                                                [IsActivated],
                                                                [ActivatedDate],
                                                                [ActivatedBy],
                                                                [CreatedBy],
                                                                [CreatedDate],
                                                                [LastModifiedBy],
                                                                [LastModifiedDate]) 
                VALUES (?,?,?,?,?,?,?,?,?,?,?,getdate(),?,?,getdate(),?,getdate()-1)
            ''',
            row.RestaurantFID,
            row.UniqueID,
            row.InfoTypeFID,
            row.language_translate,
            row.LanguageResKey,
            row.FileTypeFID,
            row.FileStreamFID,
            row.Title_Translated,
            row.Description_Translated,
            row.Deleted,
            row.IsActivated,
            row.ActivatedBy,
            row.CreatedBy,
            row.LastModifiedBy
            )
    else:
        cursor.execute(
            ''' UPDATE [dbo].[RestaurantOtherInformations] 
                SET     Title = ?, 
                        FullDescriptions = ?, 
                        LastModifiedDate = ? 
                WHERE   RestaurantFID = ? 
                and LanguageFID = ?
            ''',
            row.Title_Translated,
            row.Description_Translated,
            row.LastModifiedDate,
            row.RestaurantFID,
            row.language_translate
        )



cnxn.commit()
cursor.close()
