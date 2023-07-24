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
cnxn = pyodbc.connect('DRIVER={SQL Server};Server=(local);Database=test;Port=1433;User ID=giangcmh;Password=123qaz098@*')

#################################################################
# translate
# hotelinformationdetails
hotelinformationdetails = pd.read_sql("select distinct * from [Giang_AQ_Accomodation].[dbo].[HotelInformationDetails] A where	LanguageFID = 1 and not exists (select 1 from [Giang_AQ_Accomodation].[dbo].[HotelInformationDetails] B where A.InformationFID = B.InformationFID and B.LanguageFID = 5) and Deleted = 0"
                   , cnxn, index_col='ID')

# RatingAttributeDetails
ratingattributedetails = pd.read_sql("select * from [Giang_AQ_Accomodation].[dbo].[RatingAttributeDetails] A where LanguageFID = 1 and not exists (select 1 from [Giang_AQ_Accomodation].[dbo].[RatingAttributeDetails] B where A.RatingFID = B.RatingFID and B.LanguageFID = 5) and Deleted = 0"
                   , cnxn, index_col='ID')

# RoomAmenityAttributeDetails
roomamenityattributedetails = pd.read_sql("select * from [Giang_AQ_Accomodation].[dbo].[RoomAmenityAttributeDetails] A where	LanguageFID = 1 and not exists (select 1 from [Giang_AQ_Accomodation].[dbo].[RoomAmenityAttributeDetails] B where A.RoomAmenitiFID = B.RoomAmenitiFID and B.LanguageFID = 5) and Deleted = 0"
                   , cnxn, index_col='ID')

# HotelSandboxMutilLang
hotelsandboxmutillang = pd.read_sql("select * from [Giang_AQ_Accomodation].[dbo].[HotelSandboxMutilLang] A where	LanguageFID = 1 and not exists (select 1 from [Giang_AQ_Accomodation].[dbo].[HotelSandboxMutilLang] B where A.SanboxFID = B.SanboxFID and B.LanguageFID = 5) and Deleted = 0"
                   , cnxn, index_col='ID')

# HotelAmenityAttributeDetails
hotelamenityattributedetails = pd.read_sql("select * from [Giang_AQ_Accomodation].[dbo].[HotelAmenityAttributeDetails] A where	LanguageFID = 1 and not exists (select 1 from [Giang_AQ_Accomodation].[dbo].[HotelAmenityAttributeDetails] B where A.HotelAmenitiFID = B.HotelAmenitiFID and B.LanguageFID = 5) and Deleted = 0"
                   , cnxn, index_col='ID')

# HotelAdditionalServiceDetails
hoteladditionalservicedetails = pd.read_sql("select * from [Giang_AQ_Accomodation].[dbo].[HotelAdditionalServiceDetails] A where	LanguageFID = 1 and not exists (select 1 from [Giang_AQ_Accomodation].[dbo].[HotelAdditionalServiceDetails] B where A.AdditionalServiceFID = B.AdditionalServiceFID and B.LanguageFID = 5)  and Deleted = 0"
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




#################################################################

# insert dataframe into sql server
cursor = cnxn.cursor()

# hotelinformationdetails

for index, row in hotelinformationdetails.iterrows():
    cursor.execute("INSERT INTO [Giang_AQ_Accomodation].[dbo].[HotelInformationDetails]([UniqueID],[InformationFID],[LanguageFID],[FileTypeFID],[FileStreamFID],[Title],[ShortDescriptions],[FullDescriptions],[Deleted],[IsActivated],[ActivatedDate],[ActivatedBy],[LastModifiedBy],[LastModifiedDate]) VALUES (?,?,?,?,?,?,?,?,?,?,getdate(),?,?,getdate())",
                   row.UniqueID,
                   row.InformationFID,
                   languagefid,
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

# RatingAttributeDetails
for index, row in ratingattributedetails.iterrows():
    cursor.execute("INSERT INTO [Giang_AQ_Accomodation].[dbo].[RatingAttributeDetails]([RatingFID],[LanguageFID],[Name],[Remark],[IsActive],[Deleted],[CreatedBy],[CreatedDate],[LastModifiedBy],[LastModifiedDate]) VALUES (?,?,?,?,?,?,?,getdate(),?,getdate())",
                   row.RatingFID,
                   languagefid,
                   row.Description_Translated,
                   row.Remark,
                   row.IsActive,
                   row.Deleted,
                   row.CreatedBy,
                   row.LastModifiedBy
                   )

# RoomAmenityAttributeDetails
for index, row in roomamenityattributedetails.iterrows():
    cursor.execute("INSERT INTO [Giang_AQ_Accomodation].[dbo].[RoomAmenityAttributeDetails]([RoomAmenitiFID],[LanguageFID],[Name],[Remark],[IsActive],[Deleted],[CreatedBy],[CreatedDate],[LastModifiedBy],[LastModifiedDate]) VALUES (?,?,?,?,?,?,?,getdate(),?,getdate())",
                   row.RoomAmenitiFID,
                   languagefid,
                   row.Description_Translated,
                   row.Remark,
                   row.IsActive,
                   row.Deleted,
                   row.CreatedBy,
                   row.LastModifiedBy
                   )

# HotelSandboxMutilLang
for index, row in hotelsandboxmutillang.iterrows():
    cursor.execute("INSERT INTO [Giang_AQ_Accomodation].[dbo].[HotelSandboxMutilLang]([SanboxFID],[LanguageFID],[SanboxName],[SanboxDescription],[IsActivated],[Deleted],[CreatedBy],[CreatedDate],[LastModifiedBy],[LastModifiedDate],[SandboxPolicies],[SanboxShortDescription]) VALUES (?,?,?,?,?,?,?,getdate(),?,getdate(),?,?)",
                   row.SanboxFID,
                   languagefid,
                   row.SanboxName,
                   row.Description_Translated,
                   row.IsActivated,
                   row.Deleted,
                   row.CreatedBy,
                   row.LastModifiedBy,
                   row.Policies_Translated,
                   row.ShortDescription_Translated
                   )

# HotelAmenityAttributeDetails
for index, row in hotelamenityattributedetails.iterrows():
    cursor.execute("INSERT INTO [Giang_AQ_Accomodation].[dbo].[HotelAmenityAttributeDetails]([HotelAmenitiFID],[LanguageFID],[Name],[Remark],[IsActive],[Deleted],[CreatedBy],[CreatedDate],[LastModifiedBy],[LastModifiedDate]) VALUES (?,?,?,?,?,?,?,getdate(),?,getdate())",
                   row.HotelAmenitiFID,
                   languagefid,
                   row.Description_Translated,
                   row.Remark,
                   row.IsActive,
                   row.Deleted,
                   row.CreatedBy,
                   row.LastModifiedBy
                   )

# HotelAdditionalServiceDetails
for index, row in hoteladditionalservicedetails.iterrows():
    cursor.execute("INSERT INTO [Giang_AQ_Accomodation].[dbo].[HotelAdditionalServiceDetails]([AdditionalServiceFID],[LanguageFID],[Name],[Remark],[Deleted],[CreatedBy],[CreatedDate],[LastModifiedBy],[LastModifiedDate]) VALUES (?,?,?,?,?,?,getdate(),?,getdate())",
                   row.AdditionalServiceFID,
                   languagefid,
                   row.Description_Translated,
                   row.Remark,
                   row.Deleted,
                   row.CreatedBy,
                   row.LastModifiedBy
                   )

cnxn.commit()
cursor.close()