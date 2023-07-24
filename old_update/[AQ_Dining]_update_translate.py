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
                         "Database=AQ_Dining;"
                         "uid=giangcmh;"
                         "pwd=123qaz098@*;",
                         autocommit=True
                         )

# RatingAttributeDetails
ratingattributedetails = pd.read_sql("select	* from	[dbo].[RatingAttributeDetails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[RatingAttributeDetails] B where A.RatingFID = B.RatingFID and B.LanguageFID = 5) and Deleted = 0  and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# RestaurantCancelPoliMutilLang
restaurantcancelpolimutillang = pd.read_sql("select	* from	[dbo].[RestaurantCancelPoliMutilLang] A where	LanguageFID = 1 and exists (select 1 from [dbo].[RestaurantCancelPoliMutilLang] B where A.CancellationPoliciFID = B.CancellationPoliciFID and B.LanguageFID = 5) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# RestaurantInformationDetails
restaurantinformationdetails = pd.read_sql("select	* from	[dbo].[RestaurantInformationDetails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[RestaurantInformationDetails] B where A.InformationFID = B.InformationFID and B.LanguageFID = 5) and Deleted = 0  and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# RestaurantOtherInformations
restaurantotherinformations = pd.read_sql("select	* from	[dbo].[RestaurantOtherInformations] A where	LanguageFID = 1 and exists (select 1 from [dbo].[RestaurantOtherInformations] B where A.RestaurantFID = B.RestaurantFID and B.LanguageFID = 5) and Deleted = 0  and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# init translate VietNam language
translate_client = translate_v2.Client()
target = "vi"
# LanguageFID
## 5 : Vietnamese
languagefid = 5

#################################################################

# RatingAttributeDetails
ratingattributedetails_Description_Translated = []
for index, row in ratingattributedetails.iterrows():
    text_description = row['Name']

    # process NULL data in FullDescriptions column
    if text_description == None:
        ratingattributedetails_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=target)
        ratingattributedetails_Description_Translated.append(output_description['translatedText'])
# add 1 new columns translated

ratingattributedetails['Description_Translated'] = ratingattributedetails_Description_Translated

#################################################################

# RestaurantCancelPoliMutilLang
restaurantcancelpolimutillang_Description_Translated = []
restaurantcancelpolimutillang_termandpolicies_Translated = []
for index, row in restaurantcancelpolimutillang.iterrows():
    text_description = row['Name']
    text_termandpolicies = row['TermAndPolicies']

    # process NULL data in FullDescriptions column
    if text_description == None:
        restaurantcancelpolimutillang_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=target)
        restaurantcancelpolimutillang_Description_Translated.append(output_description['translatedText'])

    # process NULL data in FullDescriptions column
    if text_termandpolicies == None:
        restaurantcancelpolimutillang_termandpolicies_Translated.append(text_termandpolicies)
    else:
        output_description = translate_client.translate(text_termandpolicies, target_language=target)
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
    text_title = row['Title']
    text_description = row['FullDescriptions']
    text_shortdescription = row['ShortDescriptions']
    # process NULL data in Title column
    if text_title == None:
        restaurantinformationdetails_Title_Translated.append(text_title)
    else:
        output_title = translate_client.translate(text_title, target_language=target)
        restaurantinformationdetails_Title_Translated.append(output_title['translatedText'])

    # process NULL data in FullDescriptions column
    if text_description == None:
        restaurantinformationdetails_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=target)
        restaurantinformationdetails_Description_Translated.append(output_description['translatedText'])

    # process NULL data in ShortDescriptions column
    if text_shortdescription == None:
        restaurantinformationdetails_ShortDescription_Translated.append(text_shortdescription)
    else:
        output_shortdescription = translate_client.translate(text_shortdescription, target_language=target)
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
    text_title = row['Title']
    text_description = row['FullDescriptions']

    # process NULL data in Title column
    if text_title == None:
        restaurantotherinformations_Title_Translated.append(text_title)
    else:
        output_title = translate_client.translate(text_title, target_language=target)
        restaurantotherinformations_Title_Translated.append(output_title['translatedText'])

    # process NULL data in FullDescriptions column
    if text_description == None:
        restaurantotherinformations_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=target)
        restaurantotherinformations_Description_Translated.append(output_description['translatedText'])

# add 3 new columns translated

restaurantotherinformations['Title_Translated'] = restaurantotherinformations_Title_Translated
restaurantotherinformations['Description_Translated'] = restaurantotherinformations_Description_Translated


# insert dataframe into sql server
cursor = cnxn.cursor()

# RatingAttributeDetails
for index, row in ratingattributedetails.iterrows():
    cursor.execute("UPDATE [dbo].[RatingAttributeDetails] SET	Name = ? WHERE RatingFID = ? and LanguageFID = ?",
                   row.Description_Translated,
                   row.RatingFID,
                   languagefid
                   )

# RestaurantCancelPoliMutilLang
for index, row in restaurantcancelpolimutillang.iterrows():
    cursor.execute("UPDATE [dbo].[RestaurantCancelPoliMutilLang] SET	Name = ?, TermAndPolicies = ? WHERE CancellationPoliciFID = ? and LanguageFID = ?",
                   row.Description_Translated,
                   row.TermAndPolicies_Translated,
                   row.CancellationPoliciFID,
                   languagefid
                   )

# RestaurantInformationDetails
for index, row in restaurantinformationdetails.iterrows():
    cursor.execute("UPDATE [dbo].[RestaurantInformationDetails] SET Title = ?, ShortDescriptions = ?, FullDescriptions = ? WHERE InformationFID = ? and LanguageFID = ?",
                   row.Title_Translated,
                   row.ShortDescription_Translated,
                   row.Description_Translated,
                   row.InformationFID,
                   languagefid
                   )

# RestaurantOtherInformations
for index, row in restaurantinformationdetails.iterrows():
    cursor.execute("UPDATE [dbo].[RestaurantOtherInformations] SET Title = ?, FullDescriptions = ? WHERE RestaurantFID = ? and LanguageFID = ?",
                   row.Title_Translated,
                   row.Description_Translated,
                   row.RestaurantFID,
                   languagefid
                   )


cnxn.commit()
cursor.close()








