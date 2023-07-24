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
                         "Database=AQ_Yacht;"
                         "uid=giangcmh;"
                         "pwd=123qaz098@*;",
                         autocommit=True
                         )

# init translate VietNam language
translate_client = translate_v2.Client()
target = "vi"
# LanguageFID
## 5 : Vietnamese
languagefid = 5

#################################################################
# RatingAttributeDetails
ratingattributedetails = pd.read_sql("select	* from	[dbo].[RatingAttributeDetails] A where	LanguageFID = 1 and not exists (select 1 from [dbo].[RatingAttributeDetails] B where A.RatingFID = B.RatingFID and B.LanguageFID = 5) and Deleted = 0"
                   , cnxn, index_col='ID')

# YachtAdditionalServiceDetails
yachtadditionalservicedetails = pd.read_sql("select	* from	[dbo].[YachtAdditionalServiceDetails] A where	LanguageFID = 1 and not exists (select 1 from [dbo].[YachtAdditionalServiceDetails] B where A.AdditionalServiceFID = B.AdditionalServiceFID and B.LanguageFID = 5) and Deleted = 0"
                   , cnxn, index_col='ID')

# YachtCancelPoliMutilLang
yachtcancelpolimutillang = pd.read_sql("select	* from	[dbo].[YachtCancelPoliMutilLang] A where	LanguageFID = 1 and not exists (select 1 from [dbo].[YachtCancelPoliMutilLang] B where A.CancellationPoliciFID = B.CancellationPoliciFID and B.LanguageFID = 5) and Deleted = 0"
                   , cnxn, index_col='ID')

# YachtInformationDetails
yachtinformationdetails = pd.read_sql("select	* from	[dbo].[YachtInformationDetails] A where	LanguageFID = 1 and not exists (select 1 from [dbo].[YachtInformationDetails] B where A.InformationFID = B.InformationFID and B.LanguageFID = 5) and Deleted = 0"
                   , cnxn, index_col='ID')

# YachtMerchantInformationDetails
yachtmerchantinformationdetails = pd.read_sql("select	* from	[dbo].[YachtMerchantInformationDetails] A where	LanguageFID = 1 and not exists (select 1 from [dbo].[YachtMerchantInformationDetails] B where A.InformationFID = B.InformationFID and B.LanguageFID = 5) and Deleted = 0"
                   , cnxn, index_col='ID')

# YachtPaymentPoliMutilLang
yachtpaymentpolimutillang = pd.read_sql("select	* from	[dbo].[YachtPaymentPoliMutilLang] A where	LanguageFID = 1 and not exists (select 1 from [dbo].[YachtPaymentPoliMutilLang] B where A.PaymentPoliciFID = B.PaymentPoliciFID and B.LanguageFID = 5) and Deleted = 0"
                   , cnxn, index_col='ID')

# YachtTourCategoryInfomations
yachttourcategoryinfomations = pd.read_sql("select	* from	[dbo].[YachtTourCategoryInfomations] A where	LanguageFID = 1 and not exists (select 1 from [dbo].[YachtTourCategoryInfomations] B where A.TourCategoryFID = B.TourCategoryFID and B.LanguageFID = 5) and Deleted = 0"
                   , cnxn, index_col='ID')

# YachtTourInformationDetails
yachttourinformationdetails = pd.read_sql("select	* from	[dbo].[YachtTourInformationDetails] A where	LanguageFID = 1 and not exists (select 1 from [dbo].[YachtTourInformationDetails] B where A.InformationFID = B.InformationFID and B.LanguageFID = 5) and Deleted = 0"
                   , cnxn, index_col='ID')

#################################################################

# RatingAttributeDetails
ratingattributedetails_Name_Translated = []

for index, row in ratingattributedetails.iterrows():
    text_name = row['Name']

    # process NULL data in FullDescriptions column
    if text_name == None:
        ratingattributedetails_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=target)
        ratingattributedetails_Name_Translated.append(output_description['translatedText'])

# add 1 new columns translated

ratingattributedetails['Name_Translated'] = ratingattributedetails_Name_Translated

#################################################################

# YachtAdditionalServiceDetails
yachtadditionalservicedetails_Name_Translated = []

for index, row in yachtadditionalservicedetails.iterrows():
    text_name = row['Name']

    # process NULL data in FullDescriptions column
    if text_name == None:
        yachtadditionalservicedetails_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=target)
        yachtadditionalservicedetails_Name_Translated.append(output_description['translatedText'])

# add 1 new columns translated

yachtadditionalservicedetails['Name_Translated'] = yachtadditionalservicedetails_Name_Translated

#################################################################

# YachtCancelPoliMutilLang
yachtcancelpolimutillang_Name_Translated = []
yachtcancelpolimutillang_Term_Translated = []
for index, row in yachtcancelpolimutillang.iterrows():
    text_name = row['Name']
    text_term = row['TermAndPolicies']

    # process NULL data in FullDescriptions column
    if text_name == None:
        yachtcancelpolimutillang_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=target)
        yachtcancelpolimutillang_Name_Translated.append(output_description['translatedText'])

    if text_term == None:
        yachtcancelpolimutillang_Term_Translated.append(text_term)
    else:
        output_description = translate_client.translate(text_term, target_language=target)
        yachtcancelpolimutillang_Term_Translated.append(output_description['translatedText'])
# add 1 new columns translated

yachtcancelpolimutillang['Name_Translated'] = yachtcancelpolimutillang_Name_Translated
yachtcancelpolimutillang['Term_Translated'] = yachtcancelpolimutillang_Term_Translated

#################################################################

# YachtInformationDetails
yachtinformationdetails_Title_Translated = []
yachtinformationdetails_ShortDescriptions_Translated = []
yachtinformationdetails_FullDescriptions_Translated = []
for index, row in yachtinformationdetails.iterrows():
    text_title = row['Title']
    text_shortdescription = row['ShortDescriptions']
    text_fulldescription = row['FullDescriptions']

    # process NULL data in FullDescriptions column
    if text_title == None:
        yachtinformationdetails_Title_Translated.append(text_title)
    else:
        output_description = translate_client.translate(text_title, target_language=target)
        yachtinformationdetails_Title_Translated.append(output_description['translatedText'])

    if text_shortdescription == None:
        yachtinformationdetails_ShortDescriptions_Translated.append(text_shortdescription)
    else:
        output_description = translate_client.translate(text_shortdescription, target_language=target)
        yachtinformationdetails_ShortDescriptions_Translated.append(output_description['translatedText'])

    if text_fulldescription == None:
        yachtinformationdetails_FullDescriptions_Translated.append(text_fulldescription)
    else:
        output_description = translate_client.translate(text_fulldescription, target_language=target)
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
    text_title = row['Title']
    text_shortdescription = row['ShortDescriptions']
    text_fulldescription = row['FullDescriptions']

    # process NULL data in FullDescriptions column
    if text_title == None:
        yachtmerchantinformationdetails_Title_Translated.append(text_title)
    else:
        output_description = translate_client.translate(text_title, target_language=target)
        yachtmerchantinformationdetails_Title_Translated.append(output_description['translatedText'])

    if text_shortdescription == None:
        yachtmerchantinformationdetails_ShortDescriptions_Translated.append(text_shortdescription)
    else:
        output_description = translate_client.translate(text_shortdescription, target_language=target)
        yachtmerchantinformationdetails_ShortDescriptions_Translated.append(output_description['translatedText'])

    if text_fulldescription == None:
        yachtmerchantinformationdetails_FullDescriptions_Translated.append(text_fulldescription)
    else:
        output_description = translate_client.translate(text_fulldescription, target_language=target)
        yachtmerchantinformationdetails_FullDescriptions_Translated.append(output_description['translatedText'])
# add 1 new columns translated

yachtmerchantinformationdetails['Title_Translated'] = yachtmerchantinformationdetails_Title_Translated
yachtmerchantinformationdetails['ShortDescriptions_Translated'] = yachtmerchantinformationdetails_ShortDescriptions_Translated
yachtmerchantinformationdetails['FullDescriptions_Translated'] = yachtmerchantinformationdetails_FullDescriptions_Translated

#################################################################

# YachtPaymentPoliMutilLang
yachtpaymentpolimutillang_Name_Translated = []
yachtpaymentpolimutillang_Term_Translated = []
for index, row in yachtpaymentpolimutillang.iterrows():
    text_name = row['Name']
    text_term = row['TermAndPolicies']

    # process NULL data in FullDescriptions column
    if text_name == None:
        yachtpaymentpolimutillang_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=target)
        yachtpaymentpolimutillang_Name_Translated.append(output_description['translatedText'])

    if text_term == None:
        yachtpaymentpolimutillang_Term_Translated.append(text_term)
    else:
        output_description = translate_client.translate(text_term, target_language=target)
        yachtpaymentpolimutillang_Term_Translated.append(output_description['translatedText'])
# add 1 new columns translated

yachtpaymentpolimutillang['Name_Translated'] = yachtpaymentpolimutillang_Name_Translated
yachtpaymentpolimutillang['Term_Translated'] = yachtpaymentpolimutillang_Term_Translated

#################################################################

# YachtTourCategoryInfomations
yachttourcategoryinfomations_ShortDescriptions_Translated = []
for index, row in yachttourcategoryinfomations.iterrows():
    text_shortdescription = row['ShortDescription']

    # process NULL data in FullDescriptions column
    if text_shortdescription == None:
        yachttourcategoryinfomations_ShortDescriptions_Translated.append(text_shortdescription)
    else:
        output_description = translate_client.translate(text_shortdescription, target_language=target)
        yachttourcategoryinfomations_ShortDescriptions_Translated.append(output_description['translatedText'])

# add 1 new columns translated

yachttourcategoryinfomations['ShortDescriptions_Translated'] = yachttourcategoryinfomations_ShortDescriptions_Translated

#################################################################

# YachtTourInformationDetails
yachttourinformationdetails_Title_Translated = []
yachttourinformationdetails_ShortDescriptions_Translated = []
yachttourinformationdetails_FullDescriptions_Translated = []
for index, row in yachttourinformationdetails.iterrows():
    text_title = row['Title']
    text_shortdescription = row['ShortDescriptions']
    text_fulldescription = row['FullDescriptions']

    # process NULL data in FullDescriptions column
    if text_title == None:
        yachttourinformationdetails_Title_Translated.append(text_title)
    else:
        output_description = translate_client.translate(text_title, target_language=target)
        yachttourinformationdetails_Title_Translated.append(output_description['translatedText'])

    if text_shortdescription == None:
        yachttourinformationdetails_ShortDescriptions_Translated.append(text_shortdescription)
    else:
        output_description = translate_client.translate(text_shortdescription, target_language=target)
        yachttourinformationdetails_ShortDescriptions_Translated.append(output_description['translatedText'])

    if text_fulldescription == None:
        yachttourinformationdetails_FullDescriptions_Translated.append(text_fulldescription)
    else:
        output_description = translate_client.translate(text_fulldescription, target_language=target)
        yachttourinformationdetails_FullDescriptions_Translated.append(output_description['translatedText'])
# add 1 new columns translated

yachttourinformationdetails['Title_Translated'] = yachttourinformationdetails_Title_Translated
yachttourinformationdetails['ShortDescriptions_Translated'] = yachttourinformationdetails_ShortDescriptions_Translated
yachttourinformationdetails['FullDescriptions_Translated'] = yachttourinformationdetails_FullDescriptions_Translated

#################################################################

# insert dataframe into sql server
cursor = cnxn.cursor()

# RatingAttributeDetails
for index, row in ratingattributedetails.iterrows():
    cursor.execute("INSERT INTO [dbo].[RatingAttributeDetails]([RatingFID],[LanguageFID],[Name],[Remark],[IsActive],[Deleted],[CreatedBy],[CreatedDate],[LastModifiedBy],[LastModifiedDate]) VALUES (?,?,?,?,?,?,?,getdate(),?,NULL)",
                   row.RatingFID,
                   languagefid,
                   row.Name_Translated,
                   row.Remark,
                   row.IsActive,
                   row.Deleted,
                   row.CreatedBy,
                   row.LastModifiedBy
                   )

# YachtAdditionalServiceDetails
for index, row in yachtadditionalservicedetails.iterrows():
    cursor.execute("INSERT INTO [dbo].[YachtAdditionalServiceDetails]([AdditionalServiceFID],[LanguageFID],[Name],[Remark],[Deleted],[CreatedBy],[CreatedDate],[LastModifiedBy],[LastModifiedDate]) VALUES (?,?,?,?,?,?,getdate(),?,NULL)",
                   row.AdditionalServiceFID,
                   languagefid,
                   row.Name_Translated,
                   row.Remark,
                   row.Deleted,
                   row.CreatedBy,
                   row.LastModifiedBy
                   )

# YachtCancelPoliMutilLang
for index, row in yachtcancelpolimutillang.iterrows():
    cursor.execute("INSERT INTO [dbo].[YachtCancelPoliMutilLang]([CancellationPoliciFID],[LanguageFID],[Name],[TermAndPolicies],[IsActive],[Deleted],[CreatedBy],[CreatedDate],[LastModifiedBy],[LastModifiedDate]) VALUES (?,?,?,?,?,?,?,getdate(),?,NULL)",
                   row.CancellationPoliciFID,
                   languagefid,
                   row.Name_Translated,
                   row.Term_Translated,
                   row.IsActive,
                   row.Deleted,
                   row.CreatedBy,
                   row.LastModifiedBy
                   )

# YachtInformationDetails
for index, row in yachtinformationdetails.iterrows():
    cursor.execute("INSERT INTO [dbo].[YachtInformationDetails]([UniqueID],[InformationFID],[LanguageFID],[FileTypeFID],[FileStreamFID],[Title],[ShortDescriptions],[FullDescriptions],[Deleted],[IsActivated],[ActivatedDate],[ActivatedBy],[LastModifiedBy],[LastModifiedDate]) VALUES (?,?,?,?,?,?,?,?,?,?,getdate(),?,?,NULL)",
                   row.UniqueID,
                   row.InformationFID,
                   languagefid,
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

# YachtMerchantInformationDetails
for index, row in yachtmerchantinformationdetails.iterrows():
    cursor.execute("INSERT INTO [dbo].[YachtMerchantInformationDetails]([UniqueID],[InformationFID],[HaveFileStream],[LanguageFID],[FileTypeFID],[FileStreamFID],[Title],[ShortDescriptions],[FullDescriptions],[Deleted],[IsActivated],[ActivatedDate],[ActivatedBy],[LastModifiedBy],[LastModifiedDate]) VALUES (?,?,?,?,?,?,?,?,?,?,?,getdate(),?,?,NULL)",
                   row.UniqueID,
                   row.InformationFID,
                   row.HaveFileStream,
                   languagefid,
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

# YachtPaymentPoliMutilLang
for index, row in yachtpaymentpolimutillang.iterrows():
    cursor.execute("INSERT INTO [dbo].[YachtPaymentPoliMutilLang]([PaymentPoliciFID],[LanguageFID],[Name],[TermAndPolicies],[IsActive],[Deleted],[CreatedBy],[CreatedDate],[LastModifiedBy],[LastModifiedDate]) VALUES (?,?,?,?,?,?,?,getdate(),?,NULL)",
                   row.PaymentPoliciFID,
                   languagefid,
                   row.Name_Translated,
                   row.Term_Translated,
                   row.IsActive,
                   row.Deleted,
                   row.CreatedBy,
                   row.LastModifiedBy
                   )

# YachtTourCategoryInfomations
for index, row in yachttourcategoryinfomations.iterrows():
    cursor.execute("INSERT INTO [dbo].[YachtTourCategoryInfomations]([TourCategoryFID],[TourCategoryResourceKey],[LanguageFID],[ShortDescription],[FullDescription],[UsingSpecialImage],[FileTypeFID],[FileStreamFID],[EffectiveDate],[Deleted],[CreatedBy],[CreatedDate],[LastModifiedBy],[LastModifiedDate]) VALUES (?,?,?,?,?,?,?,?,getdate(),?,?,getdate(),?,NULL)",
                   row.TourCategoryFID,
                   row.TourCategoryResourceKey,
                   languagefid,
                   row.ShortDescriptions_Translated,
                   row.FullDescription,
                   row.UsingSpecialImage,
                   row.FileTypeFID,
                   row.FileStreamFID,
                   row.Deleted,
                   row.CreatedBy,
                   row.LastModifiedBy
                   )

# YachtTourInformationDetails
for index, row in yachttourinformationdetails.iterrows():
    cursor.execute("INSERT INTO [dbo].[YachtTourInformationDetails]([UniqueID],[InformationFID],[LanguageFID],[FileTypeFID],[FileStreamFID],[Title],[ShortDescriptions],[FullDescriptions],[Deleted],[IsActivated],[ActivatedDate],[ActivatedBy],[LastModifiedBy],[LastModifiedDate]) VALUES (?,?,?,?,?,?,?,?,?,?,getdate(),?,?,NULL)",
                   row.UniqueID,
                   row.InformationFID,
                   languagefid,
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



cnxn.commit()
cursor.close()









