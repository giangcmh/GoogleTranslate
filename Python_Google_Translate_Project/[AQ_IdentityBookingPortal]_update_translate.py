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

# HelpContentsDetails
helpcontentsdetails = pd.read_sql("select	* from	[dbo].[HelpContentsDetails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[HelpContentsDetails] B where A.HelpContentFID = B.HelpContentFID and B.LanguageFID = 5) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# HelpTypeDedatails
helptypededatails = pd.read_sql("select	* from	[dbo].[HelpTypeDedatails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[HelpTypeDedatails] B where A.HelpTypeFID = B.HelpTypeFID and B.LanguageFID = 5) and Deleted = 0 and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

#################################################################
# HelpContentsDetails
helpcontentsdetails_Title_Translated = []
helpcontentsdetails_FullDescriptions_Translated = []
for index, row in helpcontentsdetails.iterrows():
    text_title = row['Title']
    text_fulldescription = row['FullDescriptions']

    # process NULL data in FullDescriptions column
    if text_title == None:
        helpcontentsdetails_Title_Translated.append(text_title)
    else:
        output_description = translate_client.translate(text_title, target_language=target)
        helpcontentsdetails_Title_Translated.append(output_description['translatedText'])

    if text_fulldescription == None:
        helpcontentsdetails_FullDescriptions_Translated.append(text_fulldescription)
    else:
        output_description = translate_client.translate(text_fulldescription, target_language=target)
        helpcontentsdetails_FullDescriptions_Translated.append(output_description['translatedText'])
# add 1 new columns translated

helpcontentsdetails['Title_Translated'] = helpcontentsdetails_Title_Translated
helpcontentsdetails['FullDescriptions_Translated'] = helpcontentsdetails_FullDescriptions_Translated


#################################################################

# HelpTypeDedatails
helptypededatails_Description_Translated = []
for index, row in helptypededatails.iterrows():
    text_description = row['Name']

    # process NULL data in FullDescriptions column
    if text_description == None:
        helptypededatails_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=target)
        helptypededatails_Description_Translated.append(output_description['translatedText'])
# add 1 new columns translated

helptypededatails['Description_Translated'] = helptypededatails_Description_Translated

#################################################################
# insert dataframe into sql server
cursor = cnxn.cursor()

# HelpContentsDetails
for index, row in helpcontentsdetails.iterrows():
    cursor.execute("UPDATE [dbo].[HelpContentsDetails] SET	Title = ?, FullDescriptions = ? WHERE HelpContentFID = ? and LanguageFID = ?",
                   row.Title_Translated,
                   row.FullDescriptions_Translated,
                   row.HelpContentFID,
                   languagefid
                   )

# HelpTypeDedatails
for index, row in helptypededatails.iterrows():
    cursor.execute("UPDATE [dbo].[HelpTypeDedatails] SET	Name = ? WHERE HelpTypeFID = ? and LanguageFID = ?",
                   row.Description_Translated,
                   row.HelpTypeFID,
                   languagefid
                   )


cnxn.commit()
cursor.close()