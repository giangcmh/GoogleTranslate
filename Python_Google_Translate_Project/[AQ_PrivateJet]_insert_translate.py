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
                         "Database=AQ_PrivateJet;"
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
# FlightCancelPoliMutilLang
flightcancelpolimutillang = pd.read_sql("select	* from	[dbo].[FlightCancelPoliMutilLang] A where	LanguageFID = 1 and not exists (select 1 from [dbo].[FlightCancelPoliMutilLang] B where A.CancellationPoliciFID = B.CancellationPoliciFID and B.LanguageFID = 5) and Deleted = 0"
                   , cnxn, index_col='ID')

# FlightInformationDetails
flightinformationdetails = pd.read_sql("select * from [dbo].[FlightInformationDetails] A where	LanguageFID = 1 and not exists (select 1 from [dbo].[FlightInformationDetails] B where A.InformationFID = B.InformationFID and B.LanguageFID = 5) and Deleted = 0"
                   , cnxn, index_col='ID')

# FlightPaymentPoliMutilLang
flightpaymentpolimutillang = pd.read_sql("select * from	[dbo].[FlightPaymentPoliMutilLang] A where	LanguageFID = 1 and not exists (select 1 from [dbo].[FlightPaymentPoliMutilLang] B where A.PaymentPoliciFID = B.PaymentPoliciFID and B.LanguageFID = 5) and Deleted = 0"
                   , cnxn, index_col='ID')


#################################################################

# FlightCancelPoliMutilLang
flightcancelpolimutillang_Name_Translated = []
flightcancelpolimutillang_Term_Translated = []
for index, row in flightcancelpolimutillang.iterrows():
    text_name = row['Name']
    text_term = row['TermAndPolicies']

    # process NULL data in FullDescriptions column
    if text_name == None:
        flightcancelpolimutillang_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=target)
        flightcancelpolimutillang_Name_Translated.append(output_description['translatedText'])

    if text_term == None:
        flightcancelpolimutillang_Term_Translated.append(text_term)
    else:
        output_description = translate_client.translate(text_term, target_language=target)
        flightcancelpolimutillang_Term_Translated.append(output_description['translatedText'])
# add 1 new columns translated

flightcancelpolimutillang['Name_Translated'] = flightcancelpolimutillang_Name_Translated
flightcancelpolimutillang['Term_Translated'] = flightcancelpolimutillang_Term_Translated

#################################################################

# FlightInformationDetails
flightinformationdetails_Title_Translated = []
flightinformationdetails_ShortDescriptions_Translated = []
flightinformationdetails_FullDescriptions_Translated = []
for index, row in flightinformationdetails.iterrows():
    text_title = row['Title']
    text_shortdescription = row['ShortDescriptions']
    text_fulldescription = row['FullDescriptions']

    # process NULL data in FullDescriptions column
    if text_title == None:
        flightinformationdetails_Title_Translated.append(text_title)
    else:
        output_description = translate_client.translate(text_title, target_language=target)
        flightinformationdetails_Title_Translated.append(output_description['translatedText'])

    if text_shortdescription == None:
        flightinformationdetails_ShortDescriptions_Translated.append(text_shortdescription)
    else:
        output_description = translate_client.translate(text_shortdescription, target_language=target)
        flightinformationdetails_ShortDescriptions_Translated.append(output_description['translatedText'])

    if text_fulldescription == None:
        flightinformationdetails_FullDescriptions_Translated.append(text_fulldescription)
    else:
        output_description = translate_client.translate(text_fulldescription, target_language=target)
        flightinformationdetails_FullDescriptions_Translated.append(output_description['translatedText'])
# add 1 new columns translated

flightinformationdetails['Title_Translated'] = flightinformationdetails_Title_Translated
flightinformationdetails['ShortDescriptions_Translated'] = flightinformationdetails_ShortDescriptions_Translated
flightinformationdetails['FullDescriptions_Translated'] = flightinformationdetails_FullDescriptions_Translated

#################################################################

# FlightPaymentPoliMutilLang
flightpaymentpolimutillang_Name_Translated = []
flightpaymentpolimutillang_Term_Translated = []
for index, row in flightpaymentpolimutillang.iterrows():
    text_name = row['Name']
    text_term = row['TermAndPolicies']

    # process NULL data in FullDescriptions column
    if text_name == None:
        flightpaymentpolimutillang_Name_Translated.append(text_name)
    else:
        output_description = translate_client.translate(text_name, target_language=target)
        flightpaymentpolimutillang_Name_Translated.append(output_description['translatedText'])

    if text_term == None:
        flightpaymentpolimutillang_Term_Translated.append(text_term)
    else:
        output_description = translate_client.translate(text_term, target_language=target)
        flightpaymentpolimutillang_Term_Translated.append(output_description['translatedText'])
# add 1 new columns translated

flightpaymentpolimutillang['Name_Translated'] = flightpaymentpolimutillang_Name_Translated
flightpaymentpolimutillang['Term_Translated'] = flightpaymentpolimutillang_Term_Translated

#################################################################

# insert dataframe into sql server
cursor = cnxn.cursor()

# FlightCancelPoliMutilLang
for index, row in flightcancelpolimutillang.iterrows():
    cursor.execute("INSERT INTO [dbo].[FlightCancelPoliMutilLang]([CancellationPoliciFID],[LanguageFID],[Name],[TermAndPolicies],[IsActive],[Deleted],[CreatedBy],[CreatedDate],[LastModifiedBy],[LastModifiedDate]) VALUES (?,?,?,?,?,?,?,getdate(),?,NULL)",
                   row.CancellationPoliciFID,
                   languagefid,
                   row.Name_Translated,
                   row.Term_Translated,
                   row.IsActive,
                   row.Deleted,
                   row.CreatedBy,
                   row.LastModifiedBy
                   )

# FlightInformationDetails
for index, row in flightinformationdetails.iterrows():
    cursor.execute("INSERT INTO [dbo].[FlightInformationDetails]([UniqueID],[InformationFID],[LanguageFID],[FileTypeFID],[FileStreamFID],[Title],[ShortDescriptions],[FullDescriptions],[Deleted],[IsActivated],[ActivatedDate],[ActivatedBy],[LastModifiedBy],[LastModifiedDate]) VALUES (?,?,?,?,?,?,?,?,?,?,getdate(),?,?,NULL)",
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

# FlightPaymentPoliMutilLang
for index, row in flightpaymentpolimutillang.iterrows():
    cursor.execute("INSERT INTO [dbo].[FlightPaymentPoliMutilLang]([PaymentPoliciFID],[LanguageFID],[Name],[TermAndPolicies],[IsActive],[Deleted],[CreatedBy],[CreatedDate],[LastModifiedBy],[LastModifiedDate]) VALUES (?,?,?,?,?,?,?,getdate(),?,NULL)",
                   row.PaymentPoliciFID,
                   languagefid,
                   row.Name_Translated,
                   row.Term_Translated,
                   row.IsActive,
                   row.Deleted,
                   row.CreatedBy,
                   row.LastModifiedBy
                   )


cnxn.commit()
cursor.close()
