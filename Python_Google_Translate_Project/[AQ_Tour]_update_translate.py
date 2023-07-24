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
                         "Database=AQ_Tour;"
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
ratingattributedetails = pd.read_sql("select	* from	[dbo].[RatingAttributeDetails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[RatingAttributeDetails] B where A.RatingFID = B.RatingFID and B.LanguageFID = 5) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourCancelPoliMutilLang
tourcancelpolimutillang = pd.read_sql("select	* from	[dbo].[TourCancelPoliMutilLang] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourCancelPoliMutilLang] B where A.CancellationPoliciFID = B.CancellationPoliciFID and B.LanguageFID = 5) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourInformationDetails
tourinformationdetails = pd.read_sql("select	* from	[dbo].[TourInformationDetails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourInformationDetails] B where A.InformationFID = B.InformationFID and B.LanguageFID = 5) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourPaymentPoliMutilLang
tourpaymentpolimutillang = pd.read_sql("select	* from	[dbo].[TourPaymentPoliMutilLang] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourPaymentPoliMutilLang] B where A.PaymentPoliciFID = B.PaymentPoliciFID and B.LanguageFID = 5) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourPricingDetails
tourpricingdetails = pd.read_sql("select	* from	[dbo].[TourPricingDetails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourPricingDetails] B where A.PricingFID = B.PricingFID and B.LanguageFID = 5) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourTripPlanDetail
tourtripplandetail = pd.read_sql("select	* from	[dbo].[TourTripPlanDetail] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourTripPlanDetail] B where A.TripPlanFID = B.TripPlanFID and B.LanguageFID = 5) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourTypeDetail
tourtypedetail = pd.read_sql("select	* from	[dbo].[TourTypeDetail] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourTypeDetail] B where A.TourTypeFID = B.TourTypeFID and B.LanguageFID = 5) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
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

# TourCancelPoliMutilLang
tourcancelpolimutillang_Name_Translated = []
tourcancelpolimutillang_Term_Translated = []
for index, row in tourcancelpolimutillang.iterrows():
    text_name = row['Name']
    text_term = row['TermAndPolicies']

    # process NULL data in FullDescriptions column
    if text_name == None:
        tourcancelpolimutillang_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=target)
        tourcancelpolimutillang_Name_Translated.append(output_description['translatedText'])

    if text_term == None:
        tourcancelpolimutillang_Term_Translated.append(text_term)
    else:
        output_description = translate_client.translate(text_term, target_language=target)
        tourcancelpolimutillang_Term_Translated.append(output_description['translatedText'])
# add 1 new columns translated

tourcancelpolimutillang['Name_Translated'] = tourcancelpolimutillang_Name_Translated
tourcancelpolimutillang['Term_Translated'] = tourcancelpolimutillang_Term_Translated

#################################################################

# TourInformationDetails
tourinformationdetails_Title_Translated = []
tourinformationdetails_ShortDescriptions_Translated = []
tourinformationdetails_FullDescriptions_Translated = []
for index, row in tourinformationdetails.iterrows():
    text_title = row['Title']
    text_shortdescription = row['ShortDescriptions']
    text_fulldescription = row['FullDescriptions']

    # process NULL data in FullDescriptions column
    if text_title == None:
        tourinformationdetails_Title_Translated.append(text_title)
    else:
        output_description = translate_client.translate(text_title, target_language=target)
        tourinformationdetails_Title_Translated.append(output_description['translatedText'])

    if text_shortdescription == None:
        tourinformationdetails_ShortDescriptions_Translated.append(text_shortdescription)
    else:
        output_description = translate_client.translate(text_shortdescription, target_language=target)
        tourinformationdetails_ShortDescriptions_Translated.append(output_description['translatedText'])

    if text_fulldescription == None:
        tourinformationdetails_FullDescriptions_Translated.append(text_fulldescription)
    else:
        output_description = translate_client.translate(text_fulldescription, target_language=target)
        tourinformationdetails_FullDescriptions_Translated.append(output_description['translatedText'])
# add 1 new columns translated

tourinformationdetails['Title_Translated'] = tourinformationdetails_Title_Translated
tourinformationdetails['ShortDescriptions_Translated'] = tourinformationdetails_ShortDescriptions_Translated
tourinformationdetails['FullDescriptions_Translated'] = tourinformationdetails_FullDescriptions_Translated

#################################################################

# TourPaymentPoliMutilLang
tourpaymentpolimutillang_Name_Translated = []
tourpaymentpolimutillang_Term_Translated = []
for index, row in tourpaymentpolimutillang.iterrows():
    text_name = row['Name']
    text_term = row['TermAndPolicies']

    # process NULL data in FullDescriptions column
    if text_name == None:
        tourpaymentpolimutillang_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=target)
        tourpaymentpolimutillang_Name_Translated.append(output_description['translatedText'])

    if text_term == None:
        tourpaymentpolimutillang_Term_Translated.append(text_term)
    else:
        output_description = translate_client.translate(text_term, target_language=target)
        tourpaymentpolimutillang_Term_Translated.append(output_description['translatedText'])
# add 1 new columns translated

tourpaymentpolimutillang['Name_Translated'] = tourpaymentpolimutillang_Name_Translated
tourpaymentpolimutillang['Term_Translated'] = tourpaymentpolimutillang_Term_Translated

#################################################################

# TourPricingDetails
tourpricingdetails_Name_Translated = []
tourpricingdetails_Remark_Translated = []
for index, row in tourpricingdetails.iterrows():
    text_name = row['Name']
    text_remark = row['Remark']

    # process NULL data in FullDescriptions column
    if text_name == None:
        tourpricingdetails_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=target)
        tourpricingdetails_Name_Translated.append(output_description['translatedText'])

    if text_remark == None:
        tourpricingdetails_Remark_Translated.append(text_remark)
    else:
        output_description = translate_client.translate(text_remark, target_language=target)
        tourpricingdetails_Remark_Translated.append(output_description['translatedText'])
# add 1 new columns translated

tourpricingdetails['Name_Translated'] = tourpricingdetails_Name_Translated
tourpricingdetails['Remark_Translated'] = tourpricingdetails_Remark_Translated

#################################################################

# TourTripPlanDetail
tourtripplandetail_ItemName_Translated = []
tourtripplandetail_ItemDescription_Translated = []
for index, row in tourtripplandetail.iterrows():
    text_itemname = row['ItemName']
    text_itemdescription = row['ItemDescription']

    # process NULL data in FullDescriptions column
    if text_itemname == None:
        tourtripplandetail_ItemName_Translated.append(text_itemname)
    else:
        output_description = translate_client.translate(text_itemname, target_language=target)
        tourtripplandetail_ItemName_Translated.append(output_description['translatedText'])

    if text_itemdescription == None:
        tourtripplandetail_ItemDescription_Translated.append(text_itemdescription)
    else:
        output_description = translate_client.translate(text_itemdescription, target_language=target)
        tourtripplandetail_ItemDescription_Translated.append(output_description['translatedText'])
# add 1 new columns translated

tourtripplandetail['ItemName_Translated'] = tourtripplandetail_ItemName_Translated
tourtripplandetail['ItemDescription_Translated'] = tourtripplandetail_ItemDescription_Translated

#################################################################

# TourTypeDetail
tourtypedetail_Name_Translated = []

for index, row in tourtypedetail.iterrows():
    text_name = row['Name']

    # process NULL data in FullDescriptions column
    if text_name == None:
        tourtypedetail_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=target)
        tourtypedetail_Name_Translated.append(output_description['translatedText'])


# add 1 new columns translated

tourtypedetail['Name_Translated'] = tourtypedetail_Name_Translated

#################################################################

# insert dataframe into sql server
cursor = cnxn.cursor()

# RatingAttributeDetails
for index, row in ratingattributedetails.iterrows():
    cursor.execute("UPDATE [dbo].[RatingAttributeDetails] SET	Name = ? WHERE RatingFID = ? and LanguageFID = ?",
                   row.Name_Translated,
                   row.RatingFID,
                   languagefid
                   )

# TourCancelPoliMutilLang
for index, row in tourcancelpolimutillang.iterrows():
    cursor.execute("UPDATE [dbo].[TourCancelPoliMutilLang] SET	Name = ?, TermAndPolicies = ? WHERE CancellationPoliciFID = ? and LanguageFID = ?",
                   row.Name_Translated,
                   row.Term_Translated,
                   row.CancellationPoliciFID,
                   languagefid
                   )

# TourInformationDetails
for index, row in tourinformationdetails.iterrows():
    cursor.execute("UPDATE [dbo].[TourInformationDetails] SET	Title = ?, ShortDescriptions = ?, FullDescriptions = ? WHERE InformationFID = ? and LanguageFID = ?",
                   row.Title_Translated,
                   row.ShortDescriptions_Translated,
                   row.FullDescriptions_Translated,
                   row.InformationFID,
                   languagefid
                   )

# TourPaymentPoliMutilLang
for index, row in tourpaymentpolimutillang.iterrows():
    cursor.execute("UPDATE [dbo].[TourPaymentPoliMutilLang] SET	Name = ?, TermAndPolicies = ? WHERE PaymentPoliciFID = ? and LanguageFID = ?",
                   row.Name_Translated,
                   row.Term_Translated,
                   row.PaymentPoliciFID,
                   languagefid
                   )

# TourPricingDetails
for index, row in tourpricingdetails.iterrows():
    cursor.execute("UPDATE [dbo].[TourPricingDetails] SET	Name = ?, Remark = ? WHERE PricingFID = ? and LanguageFID = ?",
                   row.Name_Translated,
                   row.Term_Translated,
                   row.PaymentPoliciFID,
                   languagefid
                   )

# TourTripPlanDetail
for index, row in tourtripplandetail.iterrows():
    cursor.execute("UPDATE [dbo].[TourTripPlanDetail] SET	ItemName = ?, ItemDescription = ? WHERE TripPlanFID = ? and LanguageFID = ?",
                   row.ItemName_Translated,
                   row.ItemDescription_Translated,
                   row.TripPlanFID,
                   languagefid
                   )

# TourTypeDetail
for index, row in tourtypedetail.iterrows():
    cursor.execute("UPDATE [dbo].[TourTypeDetail] SET	Name = ? WHERE TourTypeFID = ? and LanguageFID = ?",
                   row.Name_Translated,
                   row.TourTypeFID,
                   languagefid
                   )

cnxn.commit()
cursor.close()