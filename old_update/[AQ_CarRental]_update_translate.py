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
                         "Database=AQ_CarRental;"
                         "uid=giangcmh;"
                         "pwd=123qaz098@*;",
                         autocommit=True
                         )


# CarAdditionalServiceDetails
caradditionalservicedetails = pd.read_sql("select	* from	[dbo].[CarAdditionalServiceDetails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[CarAdditionalServiceDetails] B where A.AdditionalServiceFID = B.AdditionalServiceFID and B.LanguageFID = 5) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# CarCancelPoliMutilLang
carcancelpolimutillang = pd.read_sql("select	* from	[dbo].[CarCancelPoliMutilLang] A where	LanguageFID = 1 and exists (select 1 from [dbo].[CarCancelPoliMutilLang] B where A.CancellationPoliciFID = B.CancellationPoliciFID and B.LanguageFID = 5) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# CarInformationDetails
carinformationdetails = pd.read_sql("select	* from	[dbo].[CarInformationDetails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[CarInformationDetails] B where A.InformationFID = B.InformationFID and B.LanguageFID = 5) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# CarPaymentPoliMutilLang
carpaymentpolimutillang = pd.read_sql("select	* from	[dbo].[CarPaymentPoliMutilLang] A where	LanguageFID = 1 and exists (select 1 from [dbo].[CarPaymentPoliMutilLang] B where A.PaymentPoliciFID = B.PaymentPoliciFID and B.LanguageFID = 5) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# RatingAttributeDetails
ratingattributedetails = pd.read_sql("select	* from	[dbo].[RatingAttributeDetails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[RatingAttributeDetails] B where A.RatingFID = B.RatingFID and B.LanguageFID = 5) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')



# init translate VietNam language
translate_client = translate_v2.Client()
target = "vi"
# LanguageFID
## 5 : Vietnamese
languagefid = 5

##############################################################

# CarAdditionalServiceDetails
caradditionalservicedetails_Description_Translated = []
for index, row in caradditionalservicedetails.iterrows():
    text_description = row['Name']

    # process NULL data in FullDescriptions column
    if text_description == None:
        caradditionalservicedetails_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=target)
        caradditionalservicedetails_Description_Translated.append(output_description['translatedText'])
# add 1 new columns translated

caradditionalservicedetails['Description_Translated'] = caradditionalservicedetails_Description_Translated

#################################################################

# CarCancelPoliMutilLang
carcancelpolimutillang_Description_Translated = []
carcancelpolimutillang_termandpolicies_Translated = []
for index, row in carcancelpolimutillang.iterrows():
    text_description = row['Name']
    text_termandpolicies = row['TermAndPolicies']

    # process NULL data in FullDescriptions column
    if text_description == None:
        carcancelpolimutillang_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=target)
        carcancelpolimutillang_Description_Translated.append(output_description['translatedText'])

    # process NULL data in FullDescriptions column
    if text_termandpolicies == None:
        carcancelpolimutillang_termandpolicies_Translated.append(text_termandpolicies)
    else:
        output_description = translate_client.translate(text_termandpolicies, target_language=target)
        carcancelpolimutillang_termandpolicies_Translated.append(output_description['translatedText'])
# add 1 new columns translated

carcancelpolimutillang['Description_Translated'] = carcancelpolimutillang_Description_Translated
carcancelpolimutillang['TermAndPolicies_Translated'] = carcancelpolimutillang_termandpolicies_Translated

#################################################################

# CarInformationDetails
carinformationdetails_Title_Translated = []
carinformationdetails_Description_Translated = []
carinformationdetails_ShortDescription_Translated = []
for index, row in carinformationdetails.iterrows():
    text_title = row['Title']
    text_description = row['FullDescriptions']
    text_shortdescription = row['ShortDescriptions']
    # process NULL data in Title column
    if text_title == None:
        carinformationdetails_Title_Translated.append(text_title)
    else:
        output_title = translate_client.translate(text_title, target_language=target)
        carinformationdetails_Title_Translated.append(output_title['translatedText'])

    # process NULL data in FullDescriptions column
    if text_description == None:
        carinformationdetails_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=target)
        carinformationdetails_Description_Translated.append(output_description['translatedText'])

    # process NULL data in ShortDescriptions column
    if text_shortdescription == None:
        carinformationdetails_ShortDescription_Translated.append(text_shortdescription)
    else:
        output_shortdescription = translate_client.translate(text_shortdescription, target_language=target)
        carinformationdetails_ShortDescription_Translated.append(output_shortdescription['translatedText'])
# add 3 new columns translated

carinformationdetails['Title_Translated'] = carinformationdetails_Title_Translated
carinformationdetails['Description_Translated'] = carinformationdetails_Description_Translated
carinformationdetails['ShortDescription_Translated'] = carinformationdetails_ShortDescription_Translated

#################################################################

# CarPaymentPoliMutilLang
carpaymentpolimutillang_Description_Translated = []
carpaymentpolimutillang_termandpolicies_Translated = []
for index, row in carpaymentpolimutillang.iterrows():
    text_description = row['Name']
    text_termandpolicies = row['TermAndPolicies']

    # process NULL data in FullDescriptions column
    if text_description == None:
        carpaymentpolimutillang_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=target)
        carpaymentpolimutillang_Description_Translated.append(output_description['translatedText'])

    # process NULL data in FullDescriptions column
    if text_termandpolicies == None:
        carpaymentpolimutillang_termandpolicies_Translated.append(text_termandpolicies)
    else:
        output_description = translate_client.translate(text_termandpolicies, target_language=target)
        carpaymentpolimutillang_termandpolicies_Translated.append(output_description['translatedText'])
# add 1 new columns translated

carpaymentpolimutillang['Description_Translated'] = carpaymentpolimutillang_Description_Translated
carpaymentpolimutillang['TermAndPolicies_Translated'] = carpaymentpolimutillang_termandpolicies_Translated

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


# insert dataframe into sql server
cursor = cnxn.cursor()

# CarAdditionalServiceDetails
for index, row in caradditionalservicedetails.iterrows():
    cursor.execute("UPDATE [dbo].[CarAdditionalServiceDetails] SET	Name = ? WHERE AdditionalServiceFID = ? and LanguageFID = ?",
                   row.Description_Translated,
                   row.AdditionalServiceFID,
                   languagefid
                   )

# CarCancelPoliMutilLang
for index, row in carcancelpolimutillang.iterrows():
    cursor.execute("UPDATE [dbo].[CarCancelPoliMutilLang] SET	Name = ?, TermAndPolicies = ? WHERE CancellationPoliciFID = ? and LanguageFID = ?",
                   row.Description_Translated,
                   row.TermAndPolicies_Translated,
                   row.CancellationPoliciFID,
                   languagefid
                   )

# CarInformationDetails
for index, row in carinformationdetails.iterrows():
    cursor.execute("UPDATE [dbo].[CarInformationDetails] SET Title = ?, ShortDescriptions = ?, FullDescriptions = ? WHERE InformationFID = ? and LanguageFID = ?",
                   row.Title_Translated,
                   row.ShortDescription_Translated,
                   row.Description_Translated,
                   row.InformationFID,
                   languagefid
                   )

# CarPaymentPoliMutilLang
for index, row in carpaymentpolimutillang.iterrows():
    cursor.execute("UPDATE [dbo].[CarPaymentPoliMutilLang] SET	Name = ?, TermAndPolicies = ? WHERE PaymentPoliciFID = ? and LanguageFID = ?",
                   row.Description_Translated,
                   row.TermAndPolicies_Translated,
                   row.PaymentPoliciFID,
                   languagefid
                   )

# RatingAttributeDetails
for index, row in ratingattributedetails.iterrows():
    cursor.execute("UPDATE [dbo].[RatingAttributeDetails] SET	Name = ? WHERE RatingFID = ? and LanguageFID = ?",
                   row.Description_Translated,
                   row.RatingFID,
                   languagefid
                   )

cnxn.commit()
cursor.close()