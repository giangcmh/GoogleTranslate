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
                         "Database=AQ_Accommodation;"
                         "uid=giangcmh;"
                         "pwd=123qaz098@*;",
                         autocommit=True
                         )

# hotelinformationdetails
hotelinformationdetails = pd.read_sql("select * from [dbo].[HotelInformationDetails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[HotelInformationDetails] B where A.InformationFID = B.InformationFID and B.LanguageFID = 5) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# RatingAttributeDetails
ratingattributedetails = pd.read_sql("select * from [dbo].[RatingAttributeDetails] A where LanguageFID = 1 and exists (select 1 from [dbo].[RatingAttributeDetails] B where A.RatingFID = B.RatingFID and B.LanguageFID = 5) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# RoomAmenityAttributeDetails
roomamenityattributedetails = pd.read_sql("select * from [dbo].[RoomAmenityAttributeDetails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[RoomAmenityAttributeDetails] B where A.RoomAmenitiFID = B.RoomAmenitiFID and B.LanguageFID = 5) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# HotelSandboxMutilLang
hotelsandboxmutillang = pd.read_sql("select * from [dbo].[HotelSandboxMutilLang] A where	LanguageFID = 1 and exists (select 1 from [dbo].[HotelSandboxMutilLang] B where A.SanboxFID = B.SanboxFID and B.LanguageFID = 5) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# HotelAmenityAttributeDetails
hotelamenityattributedetails = pd.read_sql("select * from [dbo].[HotelAmenityAttributeDetails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[HotelAmenityAttributeDetails] B where A.HotelAmenitiFID = B.HotelAmenitiFID and B.LanguageFID = 5) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# HotelAdditionalServiceDetails
hoteladditionalservicedetails = pd.read_sql("select * from [dbo].[HotelAdditionalServiceDetails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[HotelAdditionalServiceDetails] B where A.AdditionalServiceFID = B.AdditionalServiceFID and B.LanguageFID = 5)  and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')


# init translate VietNam language
translate_client = translate_v2.Client()
target = "vi"
# LanguageFID
## 5 : Vietnamese
languagefid = 5


##############################################################

# hotelinformationdetails
hotelinformationdetails_Title_Translated = []
hotelinformationdetails_Description_Translated = []
hotelinformationdetails_ShortDescription_Translated = []
for index, row in hotelinformationdetails.iterrows():
    text_title = row['Title']
    text_description = row['FullDescriptions']
    text_shortdescription = row['ShortDescriptions']
    # process NULL data in Title column
    if text_title == None:
        hotelinformationdetails_Title_Translated.append(text_title)
    else:
        output_title = translate_client.translate(text_title, target_language=target)
        hotelinformationdetails_Title_Translated.append(output_title['translatedText'])

    # process NULL data in FullDescriptions column
    if text_description == None:
        hotelinformationdetails_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=target)
        hotelinformationdetails_Description_Translated.append(output_description['translatedText'])

    # process NULL data in ShortDescriptions column
    if text_shortdescription == None:
        hotelinformationdetails_ShortDescription_Translated.append(text_shortdescription)
    else:
        output_shortdescription = translate_client.translate(text_shortdescription, target_language=target)
        hotelinformationdetails_ShortDescription_Translated.append(output_shortdescription['translatedText'])
# add 3 new columns translated

hotelinformationdetails['Title_Translated'] = hotelinformationdetails_Title_Translated
hotelinformationdetails['Description_Translated'] = hotelinformationdetails_Description_Translated
hotelinformationdetails['ShortDescription_Translated'] = hotelinformationdetails_ShortDescription_Translated

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

# roomamenityattributedetails
roomamenityattributedetails_Description_Translated = []
for index, row in roomamenityattributedetails.iterrows():
    text_description = row['Name']
    # process NULL data in Title column

    # process NULL data in FullDescriptions column
    if text_description == None:
        roomamenityattributedetails_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=target)
        roomamenityattributedetails_Description_Translated.append(output_description['translatedText'])
# add 1 new columns translated

roomamenityattributedetails['Description_Translated'] = roomamenityattributedetails_Description_Translated

#################################################################

# hotelsandboxmutillang
hotelsandboxmutillang_Description_Translated = []
hotelsandboxmutillang_Policies_Translated = []
hotelsandboxmutillang_ShortDescription_Translated = []
for index, row in hotelsandboxmutillang.iterrows():
    text_description = row['SanboxDescription']
    text_policies = row['SandboxPolicies']
    text_shortdescription = row['SanboxShortDescription']

    # process NULL data in FullDescriptions column
    if text_description == None:
        hotelsandboxmutillang_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=target)
        hotelsandboxmutillang_Description_Translated.append(output_description['translatedText'])

    # process NULL data in policies column
    if text_policies == None:
        hotelsandboxmutillang_Policies_Translated.append(text_policies)
    else:
        output_policies = translate_client.translate(text_policies, target_language=target)
        hotelsandboxmutillang_Policies_Translated.append(output_policies['translatedText'])

    # process NULL data in ShortDescription column
    if text_shortdescription == None:
        hotelsandboxmutillang_ShortDescription_Translated.append(text_shortdescription)
    else:
        output_shortdescription = translate_client.translate(text_shortdescription, target_language=target)
        hotelsandboxmutillang_ShortDescription_Translated.append(output_shortdescription['translatedText'])

# add 3 new columns translated
hotelsandboxmutillang['Description_Translated'] = hotelsandboxmutillang_Description_Translated
hotelsandboxmutillang['Policies_Translated'] = hotelsandboxmutillang_Policies_Translated
hotelsandboxmutillang['ShortDescription_Translated'] = hotelsandboxmutillang_ShortDescription_Translated

#################################################################

# hotelamenityattributedetails
hotelamenityattributedetails_Description_Translated = []
for index, row in hotelamenityattributedetails.iterrows():
    text_description = row['Name']

    # process NULL data in FullDescriptions column
    if text_description == None:
        hotelamenityattributedetails_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=target)
        hotelamenityattributedetails_Description_Translated.append(output_description['translatedText'])
# add 1 new columns translated

hotelamenityattributedetails['Description_Translated'] = hotelamenityattributedetails_Description_Translated

#################################################################

# hoteladditionalservicedetails
hoteladditionalservicedetails_Description_Translated = []
for index, row in hoteladditionalservicedetails.iterrows():
    text_description = row['Name']

    # process NULL data in FullDescriptions column
    if text_description == None:
        hoteladditionalservicedetails_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=target)
        hoteladditionalservicedetails_Description_Translated.append(output_description['translatedText'])
# add 2 new columns translated

hoteladditionalservicedetails['Description_Translated'] = hoteladditionalservicedetails_Description_Translated


#################################################################

# insert dataframe into sql server
cursor = cnxn.cursor()

# hotelinformationdetails

for index, row in hotelinformationdetails.iterrows():
    cursor.execute("UPDATE [dbo].[HotelInformationDetails] SET	Title = ?, ShortDescriptions = ?, FullDescriptions = ? WHERE InformationFID = ? and LanguageFID = ?",
                   row.Title_Translated,
                   row.ShortDescription_Translated,
                   row.Description_Translated,
                   row.InformationFID,
                   languagefid
                   )

# RatingAttributeDetails

for index, row in ratingattributedetails.iterrows():
    cursor.execute("UPDATE [dbo].[RatingAttributeDetails] SET	Name = ? WHERE RatingFID = ? and LanguageFID = ?",
                   row.Description_Translated,
                   row.RatingFID,
                   languagefid
                   )


# RoomAmenityAttributeDetails

for index, row in roomamenityattributedetails.iterrows():
    cursor.execute("UPDATE [dbo].[RoomAmenityAttributeDetails] SET	Name = ? WHERE RoomAmenitiFID = ? and LanguageFID = ?",
                   row.Description_Translated,
                   row.RoomAmenitiFID,
                   languagefid
                   )

# HotelSandboxMutilLang

for index, row in hotelsandboxmutillang.iterrows():
    cursor.execute("UPDATE [dbo].[HotelSandboxMutilLang] SET SanboxDescription = ?, SandboxPolicies = ? ,SanboxShortDescription = ?   WHERE SanboxFID = ? and LanguageFID = ?",
                   row.Description_Translated,
                   row.Policies_Translated,
                   row.ShortDescription_Translated,
                   row.SanboxFID,
                   languagefid
                   )

# HotelAmenityAttributeDetails

for index, row in hotelamenityattributedetails.iterrows():
    cursor.execute("UPDATE [dbo].[HotelAmenityAttributeDetails] SET	Name = ? WHERE HotelAmenitiFID = ? and LanguageFID = ?",
                   row.Description_Translated,
                   row.HotelAmenitiFID,
                   languagefid
                   )

# HotelAdditionalServiceDetails

for index, row in hoteladditionalservicedetails.iterrows():
    cursor.execute("UPDATE [dbo].[HotelAdditionalServiceDetails] SET Name = ? WHERE AdditionalServiceFID = ? and LanguageFID = ?",
                   row.Description_Translated,
                   row.AdditionalServiceFID,
                   languagefid
                   )



cnxn.commit()
cursor.close()
