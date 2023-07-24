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
                         "Database=AQ_Tour;"
                         "uid=translate;"
                         "pwd=456wsx765&*;",
                         autocommit=True
                         )

# init translate VietNam language
translate_client = translate_v2.Client()

zh_cn_target = "zh-CN" #Standard Chinese #language_id = 2
th_target = "th" #Thai #language_id = 4
zh_tw_target = "zh-TW" #Traditional Chinese #language_id = 6

vi_target = "vi" #Vietnamese #language_id = 5
# LanguageFID
zh_cn_languagefid = 2 #Standard Chinese
th_languagefid = 4 #Thai
zh_tw_languagefid = 6 #Traditional Chinese

vi_languagefid = 5 #Vietnamese

#################################################################  Get dataframe

############################## 2 Standard Chinese
# RatingAttributeDetails
zh_cn_ratingattributedetails = pd.read_sql(f"select	* from	[dbo].[RatingAttributeDetails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[RatingAttributeDetails] B where A.RatingFID = B.RatingFID and B.LanguageFID = {zh_cn_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourCancelPoliMutilLang
zh_cn_tourcancelpolimutillang = pd.read_sql(f"select	* from	[dbo].[TourCancelPoliMutilLang] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourCancelPoliMutilLang] B where A.CancellationPoliciFID = B.CancellationPoliciFID and B.LanguageFID = {zh_cn_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourInformationDetails
zh_cn_tourinformationdetails = pd.read_sql(f"select	* from	[dbo].[TourInformationDetails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourInformationDetails] B where A.InformationFID = B.InformationFID and B.LanguageFID = {zh_cn_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourPaymentPoliMutilLang
zh_cn_tourpaymentpolimutillang = pd.read_sql(f"select	* from	[dbo].[TourPaymentPoliMutilLang] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourPaymentPoliMutilLang] B where A.PaymentPoliciFID = B.PaymentPoliciFID and B.LanguageFID = {zh_cn_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourPricingDetails
zh_cn_tourpricingdetails = pd.read_sql(f"select	* from	[dbo].[TourPricingDetails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourPricingDetails] B where A.PricingFID = B.PricingFID and B.LanguageFID = {zh_cn_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourTripPlanDetail
zh_cn_tourtripplandetail = pd.read_sql(f"select	* from	[dbo].[TourTripPlanDetail] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourTripPlanDetail] B where A.TripPlanFID = B.TripPlanFID and B.LanguageFID = {zh_cn_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourTypeDetail
zh_cn_tourtypedetail = pd.read_sql(f"select	* from	[dbo].[TourTypeDetail] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourTypeDetail] B where A.TourTypeFID = B.TourTypeFID and B.LanguageFID = {zh_cn_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

############################## 4 Thai
# RatingAttributeDetails
th_ratingattributedetails = pd.read_sql(f"select	* from	[dbo].[RatingAttributeDetails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[RatingAttributeDetails] B where A.RatingFID = B.RatingFID and B.LanguageFID = {th_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourCancelPoliMutilLang
th_tourcancelpolimutillang = pd.read_sql(f"select	* from	[dbo].[TourCancelPoliMutilLang] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourCancelPoliMutilLang] B where A.CancellationPoliciFID = B.CancellationPoliciFID and B.LanguageFID = {th_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourInformationDetails
th_tourinformationdetails = pd.read_sql(f"select	* from	[dbo].[TourInformationDetails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourInformationDetails] B where A.InformationFID = B.InformationFID and B.LanguageFID = {th_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourPaymentPoliMutilLang
th_tourpaymentpolimutillang = pd.read_sql(f"select	* from	[dbo].[TourPaymentPoliMutilLang] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourPaymentPoliMutilLang] B where A.PaymentPoliciFID = B.PaymentPoliciFID and B.LanguageFID = {th_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourPricingDetails
th_tourpricingdetails = pd.read_sql(f"select	* from	[dbo].[TourPricingDetails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourPricingDetails] B where A.PricingFID = B.PricingFID and B.LanguageFID = {th_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourTripPlanDetail
th_tourtripplandetail = pd.read_sql(f"select	* from	[dbo].[TourTripPlanDetail] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourTripPlanDetail] B where A.TripPlanFID = B.TripPlanFID and B.LanguageFID = {th_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourTypeDetail
th_tourtypedetail = pd.read_sql(f"select	* from	[dbo].[TourTypeDetail] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourTypeDetail] B where A.TourTypeFID = B.TourTypeFID and B.LanguageFID = {th_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

############################## 6 Traditional Chinese
# RatingAttributeDetails
zh_tw_ratingattributedetails = pd.read_sql(f"select	* from	[dbo].[RatingAttributeDetails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[RatingAttributeDetails] B where A.RatingFID = B.RatingFID and B.LanguageFID = {zh_tw_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourCancelPoliMutilLang
zh_tw_tourcancelpolimutillang = pd.read_sql(f"select	* from	[dbo].[TourCancelPoliMutilLang] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourCancelPoliMutilLang] B where A.CancellationPoliciFID = B.CancellationPoliciFID and B.LanguageFID = {zh_tw_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourInformationDetails
zh_tw_tourinformationdetails = pd.read_sql(f"select	* from	[dbo].[TourInformationDetails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourInformationDetails] B where A.InformationFID = B.InformationFID and B.LanguageFID = {zh_tw_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourPaymentPoliMutilLang
zh_tw_tourpaymentpolimutillang = pd.read_sql(f"select	* from	[dbo].[TourPaymentPoliMutilLang] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourPaymentPoliMutilLang] B where A.PaymentPoliciFID = B.PaymentPoliciFID and B.LanguageFID = {zh_tw_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourPricingDetails
zh_tw_tourpricingdetails = pd.read_sql(f"select	* from	[dbo].[TourPricingDetails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourPricingDetails] B where A.PricingFID = B.PricingFID and B.LanguageFID = {zh_tw_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourTripPlanDetail
zh_tw_tourtripplandetail = pd.read_sql(f"select	* from	[dbo].[TourTripPlanDetail] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourTripPlanDetail] B where A.TripPlanFID = B.TripPlanFID and B.LanguageFID = {zh_tw_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourTypeDetail
zh_tw_tourtypedetail = pd.read_sql(f"select	* from	[dbo].[TourTypeDetail] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourTypeDetail] B where A.TourTypeFID = B.TourTypeFID and B.LanguageFID = {zh_tw_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')


############################## 5 Vietnamese
# RatingAttributeDetails
vi_ratingattributedetails = pd.read_sql(f"select	* from	[dbo].[RatingAttributeDetails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[RatingAttributeDetails] B where A.RatingFID = B.RatingFID and B.LanguageFID = {vi_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourCancelPoliMutilLang
vi_tourcancelpolimutillang = pd.read_sql(f"select	* from	[dbo].[TourCancelPoliMutilLang] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourCancelPoliMutilLang] B where A.CancellationPoliciFID = B.CancellationPoliciFID and B.LanguageFID = {vi_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourInformationDetails
vi_tourinformationdetails = pd.read_sql(f"select	* from	[dbo].[TourInformationDetails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourInformationDetails] B where A.InformationFID = B.InformationFID and B.LanguageFID = {vi_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourPaymentPoliMutilLang
vi_tourpaymentpolimutillang = pd.read_sql(f"select	* from	[dbo].[TourPaymentPoliMutilLang] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourPaymentPoliMutilLang] B where A.PaymentPoliciFID = B.PaymentPoliciFID and B.LanguageFID = {vi_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourPricingDetails
vi_tourpricingdetails = pd.read_sql(f"select	* from	[dbo].[TourPricingDetails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourPricingDetails] B where A.PricingFID = B.PricingFID and B.LanguageFID = {vi_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourTripPlanDetail
vi_tourtripplandetail = pd.read_sql(f"select	* from	[dbo].[TourTripPlanDetail] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourTripPlanDetail] B where A.TripPlanFID = B.TripPlanFID and B.LanguageFID = {vi_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# TourTypeDetail
vi_tourtypedetail = pd.read_sql(f"select	* from	[dbo].[TourTypeDetail] A where	LanguageFID = 1 and exists (select 1 from [dbo].[TourTypeDetail] B where A.TourTypeFID = B.TourTypeFID and B.LanguageFID = {vi_languagefid}) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')





################################################################# Call Function Translate

############################## 2 Standard Chinese
# RatingAttributeDetails
zh_cn_ratingattributedetails_Name_Translated = []

for index, row in zh_cn_ratingattributedetails.iterrows():
    text_name = row['Name']


    # process NULL data in FullDescriptions column
    if text_name == None:
        zh_cn_ratingattributedetails_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=zh_cn_target)
        zh_cn_ratingattributedetails_Name_Translated.append(output_description['translatedText'])


# add 1 new columns translated
zh_cn_ratingattributedetails['Name_Translated'] = zh_cn_ratingattributedetails_Name_Translated

#################################################################

# TourCancelPoliMutilLang
zh_cn_tourcancelpolimutillang_Name_Translated = []
zh_cn_tourcancelpolimutillang_Term_Translated = []
for index, row in zh_cn_tourcancelpolimutillang.iterrows():
    text_name = row['Name']
    text_term = row['TermAndPolicies']

    # process NULL data in FullDescriptions column
    if text_name == None:
        zh_cn_tourcancelpolimutillang_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=zh_cn_target)
        zh_cn_tourcancelpolimutillang_Name_Translated.append(output_description['translatedText'])

    if text_term == None:
        zh_cn_tourcancelpolimutillang_Term_Translated.append(text_term)
    else:
        output_description = translate_client.translate(text_term, target_language=zh_cn_target)
        zh_cn_tourcancelpolimutillang_Term_Translated.append(output_description['translatedText'])
# add 1 new columns translated
zh_cn_tourcancelpolimutillang['Name_Translated'] = zh_cn_tourcancelpolimutillang_Name_Translated
zh_cn_tourcancelpolimutillang['Term_Translated'] = zh_cn_tourcancelpolimutillang_Term_Translated

#################################################################

# TourInformationDetails
zh_cn_tourinformationdetails_Title_Translated = []
zh_cn_tourinformationdetails_ShortDescriptions_Translated = []
zh_cn_tourinformationdetails_FullDescriptions_Translated = []
for index, row in zh_cn_tourinformationdetails.iterrows():
    text_title = row['Title']
    text_shortdescription = row['ShortDescriptions']
    text_fulldescription = row['FullDescriptions']

    # process NULL data in FullDescriptions column
    if text_title == None:
        zh_cn_tourinformationdetails_Title_Translated.append(text_title)
    else:
        output_description = translate_client.translate(text_title, target_language=zh_cn_target)
        zh_cn_tourinformationdetails_Title_Translated.append(output_description['translatedText'])

    if text_shortdescription == None:
        zh_cn_tourinformationdetails_ShortDescriptions_Translated.append(text_shortdescription)
    else:
        output_description = translate_client.translate(text_shortdescription, target_language=zh_cn_target)
        zh_cn_tourinformationdetails_ShortDescriptions_Translated.append(output_description['translatedText'])

    if text_fulldescription == None:
        zh_cn_tourinformationdetails_FullDescriptions_Translated.append(text_fulldescription)
    else:
        output_description = translate_client.translate(text_fulldescription, target_language=zh_cn_target)
        zh_cn_tourinformationdetails_FullDescriptions_Translated.append(output_description['translatedText'])
# add 1 new columns translated
zh_cn_tourinformationdetails['Title_Translated'] = zh_cn_tourinformationdetails_Title_Translated
zh_cn_tourinformationdetails['ShortDescriptions_Translated'] = zh_cn_tourinformationdetails_ShortDescriptions_Translated
zh_cn_tourinformationdetails['FullDescriptions_Translated'] = zh_cn_tourinformationdetails_FullDescriptions_Translated

#################################################################

# TourPaymentPoliMutilLang
zh_cn_tourpaymentpolimutillang_Name_Translated = []
zh_cn_tourpaymentpolimutillang_Term_Translated = []
for index, row in zh_cn_tourpaymentpolimutillang.iterrows():
    text_name = row['Name']
    text_term = row['TermAndPolicies']

    # process NULL data in FullDescriptions column
    if text_name == None:
        zh_cn_tourpaymentpolimutillang_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=zh_cn_target)
        zh_cn_tourpaymentpolimutillang_Name_Translated.append(output_description['translatedText'])

    if text_term == None:
        zh_cn_tourpaymentpolimutillang_Term_Translated.append(text_term)
    else:
        output_description = translate_client.translate(text_term, target_language=zh_cn_target)
        zh_cn_tourpaymentpolimutillang_Term_Translated.append(output_description['translatedText'])
# add 1 new columns translated
zh_cn_tourpaymentpolimutillang['Name_Translated'] = zh_cn_tourpaymentpolimutillang_Name_Translated
zh_cn_tourpaymentpolimutillang['Term_Translated'] = zh_cn_tourpaymentpolimutillang_Term_Translated

#################################################################

# TourPricingDetails
zh_cn_tourpricingdetails_Name_Translated = []
zh_cn_tourpricingdetails_Remark_Translated = []
for index, row in zh_cn_tourpricingdetails.iterrows():
    text_name = row['Name']
    text_remark = row['Remark']

    # process NULL data in FullDescriptions column
    if text_name == None:
        zh_cn_tourpricingdetails_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=zh_cn_target)
        zh_cn_tourpricingdetails_Name_Translated.append(output_description['translatedText'])

    if text_remark == None:
        zh_cn_tourpricingdetails_Remark_Translated.append(text_remark)
    else:
        output_description = translate_client.translate(text_remark, target_language=zh_cn_target)
        zh_cn_tourpricingdetails_Remark_Translated.append(output_description['translatedText'])
# add 1 new columns translated
zh_cn_tourpricingdetails['Name_Translated'] = zh_cn_tourpricingdetails_Name_Translated
zh_cn_tourpricingdetails['Remark_Translated'] = zh_cn_tourpricingdetails_Remark_Translated

#################################################################

# TourTripPlanDetail
zh_cn_tourtripplandetail_ItemName_Translated = []
zh_cn_tourtripplandetail_ItemDescription_Translated = []
for index, row in zh_cn_tourtripplandetail.iterrows():
    text_itemname = row['ItemName']
    text_itemdescription = row['ItemDescription']

    # process NULL data in FullDescriptions column
    if text_itemname == None:
        zh_cn_tourtripplandetail_ItemName_Translated.append(text_itemname)
    else:
        output_description = translate_client.translate(text_itemname, target_language=zh_cn_target)
        zh_cn_tourtripplandetail_ItemName_Translated.append(output_description['translatedText'])

    if text_itemdescription == None:
        zh_cn_tourtripplandetail_ItemDescription_Translated.append(text_itemdescription)
    else:
        output_description = translate_client.translate(text_itemdescription, target_language=zh_cn_target)
        zh_cn_tourtripplandetail_ItemDescription_Translated.append(output_description['translatedText'])
# add 1 new columns translated
zh_cn_tourtripplandetail['ItemName_Translated'] = zh_cn_tourtripplandetail_ItemName_Translated
zh_cn_tourtripplandetail['ItemDescription_Translated'] = zh_cn_tourtripplandetail_ItemDescription_Translated

#################################################################

# TourTypeDetail
zh_cn_tourtypedetail_Name_Translated = []

for index, row in zh_cn_tourtypedetail.iterrows():
    text_name = row['Name']

    # process NULL data in FullDescriptions column
    if text_name == None:
        zh_cn_tourtypedetail_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=zh_cn_target)
        zh_cn_tourtypedetail_Name_Translated.append(output_description['translatedText'])

# add 1 new columns translated
zh_cn_tourtypedetail['Name_Translated'] = zh_cn_tourtypedetail_Name_Translated

############################## 4 Thai
# RatingAttributeDetails
th_ratingattributedetails_Name_Translated = []

for index, row in th_ratingattributedetails.iterrows():
    text_name = row['Name']


    # process NULL data in FullDescriptions column
    if text_name == None:
        th_ratingattributedetails_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=th_target)
        th_ratingattributedetails_Name_Translated.append(output_description['translatedText'])


# add 1 new columns translated
th_ratingattributedetails['Name_Translated'] = th_ratingattributedetails_Name_Translated

#################################################################

# TourCancelPoliMutilLang
th_tourcancelpolimutillang_Name_Translated = []
th_tourcancelpolimutillang_Term_Translated = []
for index, row in th_tourcancelpolimutillang.iterrows():
    text_name = row['Name']
    text_term = row['TermAndPolicies']

    # process NULL data in FullDescriptions column
    if text_name == None:
        th_tourcancelpolimutillang_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=th_target)
        th_tourcancelpolimutillang_Name_Translated.append(output_description['translatedText'])

    if text_term == None:
        th_tourcancelpolimutillang_Term_Translated.append(text_term)
    else:
        output_description = translate_client.translate(text_term, target_language=th_target)
        th_tourcancelpolimutillang_Term_Translated.append(output_description['translatedText'])
# add 1 new columns translated
th_tourcancelpolimutillang['Name_Translated'] = th_tourcancelpolimutillang_Name_Translated
th_tourcancelpolimutillang['Term_Translated'] = th_tourcancelpolimutillang_Term_Translated

#################################################################

# TourInformationDetails
th_tourinformationdetails_Title_Translated = []
th_tourinformationdetails_ShortDescriptions_Translated = []
th_tourinformationdetails_FullDescriptions_Translated = []
for index, row in th_tourinformationdetails.iterrows():
    text_title = row['Title']
    text_shortdescription = row['ShortDescriptions']
    text_fulldescription = row['FullDescriptions']

    # process NULL data in FullDescriptions column
    if text_title == None:
        th_tourinformationdetails_Title_Translated.append(text_title)
    else:
        output_description = translate_client.translate(text_title, target_language=th_target)
        th_tourinformationdetails_Title_Translated.append(output_description['translatedText'])

    if text_shortdescription == None:
        th_tourinformationdetails_ShortDescriptions_Translated.append(text_shortdescription)
    else:
        output_description = translate_client.translate(text_shortdescription, target_language=th_target)
        th_tourinformationdetails_ShortDescriptions_Translated.append(output_description['translatedText'])

    if text_fulldescription == None:
        th_tourinformationdetails_FullDescriptions_Translated.append(text_fulldescription)
    else:
        output_description = translate_client.translate(text_fulldescription, target_language=th_target)
        th_tourinformationdetails_FullDescriptions_Translated.append(output_description['translatedText'])
# add 1 new columns translated
th_tourinformationdetails['Title_Translated'] = th_tourinformationdetails_Title_Translated
th_tourinformationdetails['ShortDescriptions_Translated'] = th_tourinformationdetails_ShortDescriptions_Translated
th_tourinformationdetails['FullDescriptions_Translated'] = th_tourinformationdetails_FullDescriptions_Translated

#################################################################

# TourPaymentPoliMutilLang
th_tourpaymentpolimutillang_Name_Translated = []
th_tourpaymentpolimutillang_Term_Translated = []
for index, row in th_tourpaymentpolimutillang.iterrows():
    text_name = row['Name']
    text_term = row['TermAndPolicies']

    # process NULL data in FullDescriptions column
    if text_name == None:
        th_tourpaymentpolimutillang_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=th_target)
        th_tourpaymentpolimutillang_Name_Translated.append(output_description['translatedText'])

    if text_term == None:
        th_tourpaymentpolimutillang_Term_Translated.append(text_term)
    else:
        output_description = translate_client.translate(text_term, target_language=th_target)
        th_tourpaymentpolimutillang_Term_Translated.append(output_description['translatedText'])
# add 1 new columns translated
th_tourpaymentpolimutillang['Name_Translated'] = th_tourpaymentpolimutillang_Name_Translated
th_tourpaymentpolimutillang['Term_Translated'] = th_tourpaymentpolimutillang_Term_Translated

#################################################################

# TourPricingDetails
th_tourpricingdetails_Name_Translated = []
th_tourpricingdetails_Remark_Translated = []
for index, row in th_tourpricingdetails.iterrows():
    text_name = row['Name']
    text_remark = row['Remark']

    # process NULL data in FullDescriptions column
    if text_name == None:
        th_tourpricingdetails_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=th_target)
        th_tourpricingdetails_Name_Translated.append(output_description['translatedText'])

    if text_remark == None:
        th_tourpricingdetails_Remark_Translated.append(text_remark)
    else:
        output_description = translate_client.translate(text_remark, target_language=th_target)
        th_tourpricingdetails_Remark_Translated.append(output_description['translatedText'])
# add 1 new columns translated
th_tourpricingdetails['Name_Translated'] = th_tourpricingdetails_Name_Translated
th_tourpricingdetails['Remark_Translated'] = th_tourpricingdetails_Remark_Translated

#################################################################

# TourTripPlanDetail
th_tourtripplandetail_ItemName_Translated = []
th_tourtripplandetail_ItemDescription_Translated = []
for index, row in th_tourtripplandetail.iterrows():
    text_itemname = row['ItemName']
    text_itemdescription = row['ItemDescription']

    # process NULL data in FullDescriptions column
    if text_itemname == None:
        th_tourtripplandetail_ItemName_Translated.append(text_itemname)
    else:
        output_description = translate_client.translate(text_itemname, target_language=th_target)
        th_tourtripplandetail_ItemName_Translated.append(output_description['translatedText'])

    if text_itemdescription == None:
        th_tourtripplandetail_ItemDescription_Translated.append(text_itemdescription)
    else:
        output_description = translate_client.translate(text_itemdescription, target_language=th_target)
        th_tourtripplandetail_ItemDescription_Translated.append(output_description['translatedText'])
# add 1 new columns translated
th_tourtripplandetail['ItemName_Translated'] = th_tourtripplandetail_ItemName_Translated
th_tourtripplandetail['ItemDescription_Translated'] = th_tourtripplandetail_ItemDescription_Translated

#################################################################

# TourTypeDetail
th_tourtypedetail_Name_Translated = []

for index, row in th_tourtypedetail.iterrows():
    text_name = row['Name']

    # process NULL data in FullDescriptions column
    if text_name == None:
        th_tourtypedetail_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=th_target)
        th_tourtypedetail_Name_Translated.append(output_description['translatedText'])

# add 1 new columns translated
th_tourtypedetail['Name_Translated'] = th_tourtypedetail_Name_Translated

############################## 6 Traditional Chinese
# RatingAttributeDetails
zh_tw_ratingattributedetails_Name_Translated = []

for index, row in zh_tw_ratingattributedetails.iterrows():
    text_name = row['Name']


    # process NULL data in FullDescriptions column
    if text_name == None:
        zh_tw_ratingattributedetails_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=zh_tw_target)
        zh_tw_ratingattributedetails_Name_Translated.append(output_description['translatedText'])


# add 1 new columns translated
zh_tw_ratingattributedetails['Name_Translated'] = zh_tw_ratingattributedetails_Name_Translated

#################################################################

# TourCancelPoliMutilLang
zh_tw_tourcancelpolimutillang_Name_Translated = []
zh_tw_tourcancelpolimutillang_Term_Translated = []
for index, row in zh_tw_tourcancelpolimutillang.iterrows():
    text_name = row['Name']
    text_term = row['TermAndPolicies']

    # process NULL data in FullDescriptions column
    if text_name == None:
        zh_tw_tourcancelpolimutillang_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=zh_tw_target)
        zh_tw_tourcancelpolimutillang_Name_Translated.append(output_description['translatedText'])

    if text_term == None:
        zh_tw_tourcancelpolimutillang_Term_Translated.append(text_term)
    else:
        output_description = translate_client.translate(text_term, target_language=zh_tw_target)
        zh_tw_tourcancelpolimutillang_Term_Translated.append(output_description['translatedText'])
# add 1 new columns translated
zh_tw_tourcancelpolimutillang['Name_Translated'] = zh_tw_tourcancelpolimutillang_Name_Translated
zh_tw_tourcancelpolimutillang['Term_Translated'] = zh_tw_tourcancelpolimutillang_Term_Translated

#################################################################

# TourInformationDetails
zh_tw_tourinformationdetails_Title_Translated = []
zh_tw_tourinformationdetails_ShortDescriptions_Translated = []
zh_tw_tourinformationdetails_FullDescriptions_Translated = []
for index, row in zh_tw_tourinformationdetails.iterrows():
    text_title = row['Title']
    text_shortdescription = row['ShortDescriptions']
    text_fulldescription = row['FullDescriptions']

    # process NULL data in FullDescriptions column
    if text_title == None:
        zh_tw_tourinformationdetails_Title_Translated.append(text_title)
    else:
        output_description = translate_client.translate(text_title, target_language=zh_tw_target)
        zh_tw_tourinformationdetails_Title_Translated.append(output_description['translatedText'])

    if text_shortdescription == None:
        zh_tw_tourinformationdetails_ShortDescriptions_Translated.append(text_shortdescription)
    else:
        output_description = translate_client.translate(text_shortdescription, target_language=zh_tw_target)
        zh_tw_tourinformationdetails_ShortDescriptions_Translated.append(output_description['translatedText'])

    if text_fulldescription == None:
        zh_tw_tourinformationdetails_FullDescriptions_Translated.append(text_fulldescription)
    else:
        output_description = translate_client.translate(text_fulldescription, target_language=zh_tw_target)
        zh_tw_tourinformationdetails_FullDescriptions_Translated.append(output_description['translatedText'])
# add 1 new columns translated
zh_tw_tourinformationdetails['Title_Translated'] = zh_tw_tourinformationdetails_Title_Translated
zh_tw_tourinformationdetails['ShortDescriptions_Translated'] = zh_tw_tourinformationdetails_ShortDescriptions_Translated
zh_tw_tourinformationdetails['FullDescriptions_Translated'] = zh_tw_tourinformationdetails_FullDescriptions_Translated

#################################################################

# TourPaymentPoliMutilLang
zh_tw_tourpaymentpolimutillang_Name_Translated = []
zh_tw_tourpaymentpolimutillang_Term_Translated = []
for index, row in zh_tw_tourpaymentpolimutillang.iterrows():
    text_name = row['Name']
    text_term = row['TermAndPolicies']

    # process NULL data in FullDescriptions column
    if text_name == None:
        zh_tw_tourpaymentpolimutillang_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=zh_tw_target)
        zh_tw_tourpaymentpolimutillang_Name_Translated.append(output_description['translatedText'])

    if text_term == None:
        zh_tw_tourpaymentpolimutillang_Term_Translated.append(text_term)
    else:
        output_description = translate_client.translate(text_term, target_language=zh_tw_target)
        zh_tw_tourpaymentpolimutillang_Term_Translated.append(output_description['translatedText'])
# add 1 new columns translated
zh_tw_tourpaymentpolimutillang['Name_Translated'] = zh_tw_tourpaymentpolimutillang_Name_Translated
zh_tw_tourpaymentpolimutillang['Term_Translated'] = zh_tw_tourpaymentpolimutillang_Term_Translated

#################################################################

# TourPricingDetails
zh_tw_tourpricingdetails_Name_Translated = []
zh_tw_tourpricingdetails_Remark_Translated = []
for index, row in zh_tw_tourpricingdetails.iterrows():
    text_name = row['Name']
    text_remark = row['Remark']

    # process NULL data in FullDescriptions column
    if text_name == None:
        zh_tw_tourpricingdetails_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=zh_tw_target)
        zh_tw_tourpricingdetails_Name_Translated.append(output_description['translatedText'])

    if text_remark == None:
        zh_tw_tourpricingdetails_Remark_Translated.append(text_remark)
    else:
        output_description = translate_client.translate(text_remark, target_language=zh_tw_target)
        zh_tw_tourpricingdetails_Remark_Translated.append(output_description['translatedText'])
# add 1 new columns translated
zh_tw_tourpricingdetails['Name_Translated'] = zh_tw_tourpricingdetails_Name_Translated
zh_tw_tourpricingdetails['Remark_Translated'] = zh_tw_tourpricingdetails_Remark_Translated

#################################################################

# TourTripPlanDetail
zh_tw_tourtripplandetail_ItemName_Translated = []
zh_tw_tourtripplandetail_ItemDescription_Translated = []
for index, row in zh_tw_tourtripplandetail.iterrows():
    text_itemname = row['ItemName']
    text_itemdescription = row['ItemDescription']

    # process NULL data in FullDescriptions column
    if text_itemname == None:
        zh_tw_tourtripplandetail_ItemName_Translated.append(text_itemname)
    else:
        output_description = translate_client.translate(text_itemname, target_language=zh_tw_target)
        zh_tw_tourtripplandetail_ItemName_Translated.append(output_description['translatedText'])

    if text_itemdescription == None:
        zh_tw_tourtripplandetail_ItemDescription_Translated.append(text_itemdescription)
    else:
        output_description = translate_client.translate(text_itemdescription, target_language=zh_tw_target)
        zh_tw_tourtripplandetail_ItemDescription_Translated.append(output_description['translatedText'])
# add 1 new columns translated
zh_tw_tourtripplandetail['ItemName_Translated'] = zh_tw_tourtripplandetail_ItemName_Translated
zh_tw_tourtripplandetail['ItemDescription_Translated'] = zh_tw_tourtripplandetail_ItemDescription_Translated

#################################################################

# TourTypeDetail
zh_tw_tourtypedetail_Name_Translated = []

for index, row in zh_tw_tourtypedetail.iterrows():
    text_name = row['Name']

    # process NULL data in FullDescriptions column
    if text_name == None:
        zh_tw_tourtypedetail_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=zh_tw_target)
        zh_tw_tourtypedetail_Name_Translated.append(output_description['translatedText'])

# add 1 new columns translated
zh_tw_tourtypedetail['Name_Translated'] = zh_tw_tourtypedetail_Name_Translated


############################## 5 Vietnamese
# RatingAttributeDetails
vi_ratingattributedetails_Name_Translated = []

for index, row in vi_ratingattributedetails.iterrows():
    text_name = row['Name']


    # process NULL data in FullDescriptions column
    if text_name == None:
        vi_ratingattributedetails_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=vi_target)
        vi_ratingattributedetails_Name_Translated.append(output_description['translatedText'])


# add 1 new columns translated
vi_ratingattributedetails['Name_Translated'] = vi_ratingattributedetails_Name_Translated

#################################################################

# TourCancelPoliMutilLang
vi_tourcancelpolimutillang_Name_Translated = []
vi_tourcancelpolimutillang_Term_Translated = []
for index, row in vi_tourcancelpolimutillang.iterrows():
    text_name = row['Name']
    text_term = row['TermAndPolicies']

    # process NULL data in FullDescriptions column
    if text_name == None:
        vi_tourcancelpolimutillang_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=vi_target)
        vi_tourcancelpolimutillang_Name_Translated.append(output_description['translatedText'])

    if text_term == None:
        vi_tourcancelpolimutillang_Term_Translated.append(text_term)
    else:
        output_description = translate_client.translate(text_term, target_language=vi_target)
        vi_tourcancelpolimutillang_Term_Translated.append(output_description['translatedText'])
# add 1 new columns translated
vi_tourcancelpolimutillang['Name_Translated'] = vi_tourcancelpolimutillang_Name_Translated
vi_tourcancelpolimutillang['Term_Translated'] = vi_tourcancelpolimutillang_Term_Translated

#################################################################

# TourInformationDetails
vi_tourinformationdetails_Title_Translated = []
vi_tourinformationdetails_ShortDescriptions_Translated = []
vi_tourinformationdetails_FullDescriptions_Translated = []
for index, row in vi_tourinformationdetails.iterrows():
    text_title = row['Title']
    text_shortdescription = row['ShortDescriptions']
    text_fulldescription = row['FullDescriptions']

    # process NULL data in FullDescriptions column
    if text_title == None:
        vi_tourinformationdetails_Title_Translated.append(text_title)
    else:
        output_description = translate_client.translate(text_title, target_language=vi_target)
        vi_tourinformationdetails_Title_Translated.append(output_description['translatedText'])

    if text_shortdescription == None:
        vi_tourinformationdetails_ShortDescriptions_Translated.append(text_shortdescription)
    else:
        output_description = translate_client.translate(text_shortdescription, target_language=vi_target)
        vi_tourinformationdetails_ShortDescriptions_Translated.append(output_description['translatedText'])

    if text_fulldescription == None:
        vi_tourinformationdetails_FullDescriptions_Translated.append(text_fulldescription)
    else:
        output_description = translate_client.translate(text_fulldescription, target_language=vi_target)
        vi_tourinformationdetails_FullDescriptions_Translated.append(output_description['translatedText'])
# add 1 new columns translated
vi_tourinformationdetails['Title_Translated'] = vi_tourinformationdetails_Title_Translated
vi_tourinformationdetails['ShortDescriptions_Translated'] = vi_tourinformationdetails_ShortDescriptions_Translated
vi_tourinformationdetails['FullDescriptions_Translated'] = vi_tourinformationdetails_FullDescriptions_Translated

#################################################################

# TourPaymentPoliMutilLang
vi_tourpaymentpolimutillang_Name_Translated = []
vi_tourpaymentpolimutillang_Term_Translated = []
for index, row in vi_tourpaymentpolimutillang.iterrows():
    text_name = row['Name']
    text_term = row['TermAndPolicies']

    # process NULL data in FullDescriptions column
    if text_name == None:
        vi_tourpaymentpolimutillang_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=vi_target)
        vi_tourpaymentpolimutillang_Name_Translated.append(output_description['translatedText'])

    if text_term == None:
        vi_tourpaymentpolimutillang_Term_Translated.append(text_term)
    else:
        output_description = translate_client.translate(text_term, target_language=vi_target)
        vi_tourpaymentpolimutillang_Term_Translated.append(output_description['translatedText'])
# add 1 new columns translated
vi_tourpaymentpolimutillang['Name_Translated'] = vi_tourpaymentpolimutillang_Name_Translated
vi_tourpaymentpolimutillang['Term_Translated'] = vi_tourpaymentpolimutillang_Term_Translated

#################################################################

# TourPricingDetails
vi_tourpricingdetails_Name_Translated = []
vi_tourpricingdetails_Remark_Translated = []
for index, row in vi_tourpricingdetails.iterrows():
    text_name = row['Name']
    text_remark = row['Remark']

    # process NULL data in FullDescriptions column
    if text_name == None:
        vi_tourpricingdetails_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=vi_target)
        vi_tourpricingdetails_Name_Translated.append(output_description['translatedText'])

    if text_remark == None:
        vi_tourpricingdetails_Remark_Translated.append(text_remark)
    else:
        output_description = translate_client.translate(text_remark, target_language=vi_target)
        vi_tourpricingdetails_Remark_Translated.append(output_description['translatedText'])
# add 1 new columns translated
vi_tourpricingdetails['Name_Translated'] = vi_tourpricingdetails_Name_Translated
vi_tourpricingdetails['Remark_Translated'] = vi_tourpricingdetails_Remark_Translated

#################################################################

# TourTripPlanDetail
vi_tourtripplandetail_ItemName_Translated = []
vi_tourtripplandetail_ItemDescription_Translated = []
for index, row in vi_tourtripplandetail.iterrows():
    text_itemname = row['ItemName']
    text_itemdescription = row['ItemDescription']

    # process NULL data in FullDescriptions column
    if text_itemname == None:
        vi_tourtripplandetail_ItemName_Translated.append(text_itemname)
    else:
        output_description = translate_client.translate(text_itemname, target_language=vi_target)
        vi_tourtripplandetail_ItemName_Translated.append(output_description['translatedText'])

    if text_itemdescription == None:
        vi_tourtripplandetail_ItemDescription_Translated.append(text_itemdescription)
    else:
        output_description = translate_client.translate(text_itemdescription, target_language=vi_target)
        vi_tourtripplandetail_ItemDescription_Translated.append(output_description['translatedText'])
# add 1 new columns translated
vi_tourtripplandetail['ItemName_Translated'] = vi_tourtripplandetail_ItemName_Translated
vi_tourtripplandetail['ItemDescription_Translated'] = vi_tourtripplandetail_ItemDescription_Translated

#################################################################

# TourTypeDetail
vi_tourtypedetail_Name_Translated = []

for index, row in vi_tourtypedetail.iterrows():
    text_name = row['Name']

    # process NULL data in FullDescriptions column
    if text_name == None:
        vi_tourtypedetail_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=vi_target)
        vi_tourtypedetail_Name_Translated.append(output_description['translatedText'])

# add 1 new columns translated
vi_tourtypedetail['Name_Translated'] = vi_tourtypedetail_Name_Translated




################################################################# Insert dataframe into tables
cursor = cnxn.cursor()
############################## 2 Standard Chinese
# RatingAttributeDetails
for index, row in zh_cn_ratingattributedetails.iterrows():
    cursor.execute("UPDATE [dbo].[RatingAttributeDetails] SET	Name = ? WHERE RatingFID = ? and LanguageFID = ?",
                   row.Name_Translated,
                   row.RatingFID,
                   zh_cn_languagefid
                   )

# TourCancelPoliMutilLang
for index, row in zh_cn_tourcancelpolimutillang.iterrows():
    cursor.execute("UPDATE [dbo].[TourCancelPoliMutilLang] SET	Name = ?, TermAndPolicies = ? WHERE CancellationPoliciFID = ? and LanguageFID = ?",
                   row.Name_Translated,
                   row.Term_Translated,
                   row.CancellationPoliciFID,
                   zh_cn_languagefid
                   )

# TourInformationDetails
for index, row in zh_cn_tourinformationdetails.iterrows():
    cursor.execute("UPDATE [dbo].[TourInformationDetails] SET	Title = ?, ShortDescriptions = ?, FullDescriptions = ? WHERE InformationFID = ? and LanguageFID = ?",
                   row.Title_Translated,
                   row.ShortDescriptions_Translated,
                   row.FullDescriptions_Translated,
                   row.InformationFID,
                   zh_cn_languagefid
                   )

# TourPaymentPoliMutilLang
for index, row in zh_cn_tourpaymentpolimutillang.iterrows():
    cursor.execute("UPDATE [dbo].[TourPaymentPoliMutilLang] SET	Name = ?, TermAndPolicies = ? WHERE PaymentPoliciFID = ? and LanguageFID = ?",
                   row.Name_Translated,
                   row.Term_Translated,
                   row.PaymentPoliciFID,
                   zh_cn_languagefid
                   )

# TourPricingDetails
for index, row in zh_cn_tourpricingdetails.iterrows():
    cursor.execute("UPDATE [dbo].[TourPricingDetails] SET	Name = ?, Remark = ? WHERE PricingFID = ? and LanguageFID = ?",
                   row.Name_Translated,
                   row.Term_Translated,
                   row.PaymentPoliciFID,
                   zh_cn_languagefid
                   )

# TourTripPlanDetail
for index, row in zh_cn_tourtripplandetail.iterrows():
    cursor.execute("UPDATE [dbo].[TourTripPlanDetail] SET	ItemName = ?, ItemDescription = ? WHERE TripPlanFID = ? and LanguageFID = ?",
                   row.ItemName_Translated,
                   row.ItemDescription_Translated,
                   row.TripPlanFID,
                   zh_cn_languagefid
                   )

# TourTypeDetail
for index, row in zh_cn_tourtypedetail.iterrows():
    cursor.execute("UPDATE [dbo].[TourTypeDetail] SET	Name = ? WHERE TourTypeFID = ? and LanguageFID = ?",
                   row.Name_Translated,
                   row.TourTypeFID,
                   zh_cn_languagefid
                   )

############################## 4 Thai
# RatingAttributeDetails
for index, row in th_ratingattributedetails.iterrows():
    cursor.execute("UPDATE [dbo].[RatingAttributeDetails] SET	Name = ? WHERE RatingFID = ? and LanguageFID = ?",
                   row.Name_Translated,
                   row.RatingFID,
                   th_languagefid
                   )

# TourCancelPoliMutilLang
for index, row in th_tourcancelpolimutillang.iterrows():
    cursor.execute("UPDATE [dbo].[TourCancelPoliMutilLang] SET	Name = ?, TermAndPolicies = ? WHERE CancellationPoliciFID = ? and LanguageFID = ?",
                   row.Name_Translated,
                   row.Term_Translated,
                   row.CancellationPoliciFID,
                   th_languagefid
                   )

# TourInformationDetails
for index, row in th_tourinformationdetails.iterrows():
    cursor.execute("UPDATE [dbo].[TourInformationDetails] SET	Title = ?, ShortDescriptions = ?, FullDescriptions = ? WHERE InformationFID = ? and LanguageFID = ?",
                   row.Title_Translated,
                   row.ShortDescriptions_Translated,
                   row.FullDescriptions_Translated,
                   row.InformationFID,
                   th_languagefid
                   )

# TourPaymentPoliMutilLang
for index, row in th_tourpaymentpolimutillang.iterrows():
    cursor.execute("UPDATE [dbo].[TourPaymentPoliMutilLang] SET	Name = ?, TermAndPolicies = ? WHERE PaymentPoliciFID = ? and LanguageFID = ?",
                   row.Name_Translated,
                   row.Term_Translated,
                   row.PaymentPoliciFID,
                   th_languagefid
                   )

# TourPricingDetails
for index, row in th_tourpricingdetails.iterrows():
    cursor.execute("UPDATE [dbo].[TourPricingDetails] SET	Name = ?, Remark = ? WHERE PricingFID = ? and LanguageFID = ?",
                   row.Name_Translated,
                   row.Term_Translated,
                   row.PaymentPoliciFID,
                   th_languagefid
                   )

# TourTripPlanDetail
for index, row in th_tourtripplandetail.iterrows():
    cursor.execute("UPDATE [dbo].[TourTripPlanDetail] SET	ItemName = ?, ItemDescription = ? WHERE TripPlanFID = ? and LanguageFID = ?",
                   row.ItemName_Translated,
                   row.ItemDescription_Translated,
                   row.TripPlanFID,
                   th_languagefid
                   )

# TourTypeDetail
for index, row in th_tourtypedetail.iterrows():
    cursor.execute("UPDATE [dbo].[TourTypeDetail] SET	Name = ? WHERE TourTypeFID = ? and LanguageFID = ?",
                   row.Name_Translated,
                   row.TourTypeFID,
                   th_languagefid
                   )

############################## 6 Traditional Chinese
# RatingAttributeDetails
for index, row in zh_tw_ratingattributedetails.iterrows():
    cursor.execute("UPDATE [dbo].[RatingAttributeDetails] SET	Name = ? WHERE RatingFID = ? and LanguageFID = ?",
                   row.Name_Translated,
                   row.RatingFID,
                   zh_tw_languagefid
                   )

# TourCancelPoliMutilLang
for index, row in zh_tw_tourcancelpolimutillang.iterrows():
    cursor.execute("UPDATE [dbo].[TourCancelPoliMutilLang] SET	Name = ?, TermAndPolicies = ? WHERE CancellationPoliciFID = ? and LanguageFID = ?",
                   row.Name_Translated,
                   row.Term_Translated,
                   row.CancellationPoliciFID,
                   zh_tw_languagefid
                   )

# TourInformationDetails
for index, row in zh_tw_tourinformationdetails.iterrows():
    cursor.execute("UPDATE [dbo].[TourInformationDetails] SET	Title = ?, ShortDescriptions = ?, FullDescriptions = ? WHERE InformationFID = ? and LanguageFID = ?",
                   row.Title_Translated,
                   row.ShortDescriptions_Translated,
                   row.FullDescriptions_Translated,
                   row.InformationFID,
                   zh_tw_languagefid
                   )

# TourPaymentPoliMutilLang
for index, row in zh_tw_tourpaymentpolimutillang.iterrows():
    cursor.execute("UPDATE [dbo].[TourPaymentPoliMutilLang] SET	Name = ?, TermAndPolicies = ? WHERE PaymentPoliciFID = ? and LanguageFID = ?",
                   row.Name_Translated,
                   row.Term_Translated,
                   row.PaymentPoliciFID,
                   zh_tw_languagefid
                   )

# TourPricingDetails
for index, row in zh_tw_tourpricingdetails.iterrows():
    cursor.execute("UPDATE [dbo].[TourPricingDetails] SET	Name = ?, Remark = ? WHERE PricingFID = ? and LanguageFID = ?",
                   row.Name_Translated,
                   row.Term_Translated,
                   row.PaymentPoliciFID,
                   zh_tw_languagefid
                   )

# TourTripPlanDetail
for index, row in zh_tw_tourtripplandetail.iterrows():
    cursor.execute("UPDATE [dbo].[TourTripPlanDetail] SET	ItemName = ?, ItemDescription = ? WHERE TripPlanFID = ? and LanguageFID = ?",
                   row.ItemName_Translated,
                   row.ItemDescription_Translated,
                   row.TripPlanFID,
                   zh_tw_languagefid
                   )

# TourTypeDetail
for index, row in zh_tw_tourtypedetail.iterrows():
    cursor.execute("UPDATE [dbo].[TourTypeDetail] SET	Name = ? WHERE TourTypeFID = ? and LanguageFID = ?",
                   row.Name_Translated,
                   row.TourTypeFID,
                   zh_tw_languagefid
                   )


############################## 5 Vietnamese
# RatingAttributeDetails
for index, row in vi_ratingattributedetails.iterrows():
    cursor.execute("UPDATE [dbo].[RatingAttributeDetails] SET	Name = ? WHERE RatingFID = ? and LanguageFID = ?",
                   row.Name_Translated,
                   row.RatingFID,
                   vi_languagefid
                   )

# TourCancelPoliMutilLang
for index, row in vi_tourcancelpolimutillang.iterrows():
    cursor.execute("UPDATE [dbo].[TourCancelPoliMutilLang] SET	Name = ?, TermAndPolicies = ? WHERE CancellationPoliciFID = ? and LanguageFID = ?",
                   row.Name_Translated,
                   row.Term_Translated,
                   row.CancellationPoliciFID,
                   vi_languagefid
                   )

# TourInformationDetails
for index, row in vi_tourinformationdetails.iterrows():
    cursor.execute("UPDATE [dbo].[TourInformationDetails] SET	Title = ?, ShortDescriptions = ?, FullDescriptions = ? WHERE InformationFID = ? and LanguageFID = ?",
                   row.Title_Translated,
                   row.ShortDescriptions_Translated,
                   row.FullDescriptions_Translated,
                   row.InformationFID,
                   vi_languagefid
                   )

# TourPaymentPoliMutilLang
for index, row in vi_tourpaymentpolimutillang.iterrows():
    cursor.execute("UPDATE [dbo].[TourPaymentPoliMutilLang] SET	Name = ?, TermAndPolicies = ? WHERE PaymentPoliciFID = ? and LanguageFID = ?",
                   row.Name_Translated,
                   row.Term_Translated,
                   row.PaymentPoliciFID,
                   vi_languagefid
                   )

# TourPricingDetails
for index, row in vi_tourpricingdetails.iterrows():
    cursor.execute("UPDATE [dbo].[TourPricingDetails] SET	Name = ?, Remark = ? WHERE PricingFID = ? and LanguageFID = ?",
                   row.Name_Translated,
                   row.Term_Translated,
                   row.PaymentPoliciFID,
                   vi_languagefid
                   )

# TourTripPlanDetail
for index, row in vi_tourtripplandetail.iterrows():
    cursor.execute("UPDATE [dbo].[TourTripPlanDetail] SET	ItemName = ?, ItemDescription = ? WHERE TripPlanFID = ? and LanguageFID = ?",
                   row.ItemName_Translated,
                   row.ItemDescription_Translated,
                   row.TripPlanFID,
                   vi_languagefid
                   )

# TourTypeDetail
for index, row in vi_tourtypedetail.iterrows():
    cursor.execute("UPDATE [dbo].[TourTypeDetail] SET	Name = ? WHERE TourTypeFID = ? and LanguageFID = ?",
                   row.Name_Translated,
                   row.TourTypeFID,
                   vi_languagefid
                   )




cnxn.commit()
cursor.close()