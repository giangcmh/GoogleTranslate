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
                         "Database=AQ_CMS;"
                         "uid=giangcmh;"
                         "pwd=123qaz098@*;",
                         autocommit=True
                         )

# PostCategoryDetails
postcategorydetails = pd.read_sql("select	* from	[dbo].[PostCategoryDetails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[PostCategoryDetails] B where A.PostCategoryFID = B.PostCategoryFID and B.LanguageFID = 5) and Deleted = 0  and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# PostDetails
postdetails = pd.read_sql("select	* from	[dbo].[PostDetails] A where	LanguageFID = 1 and exists (select 1 from [dbo].[PostDetails] B where A.PostFId = B.PostFId and B.LanguageFID = 5) and Deleted = 0  and convert(nvarchar(15), LastModifiedDate ,102) = convert(nvarchar(15), getdate() -1 ,102)"
                   , cnxn, index_col='ID')

# init translate VietNam language
translate_client = translate_v2.Client()
target = "vi"
# LanguageFID
## 5 : Vietnamese
languagefid = 5

##############################################################

# PostCategoryDetails
postcategorydetails_Description_Translated = []
for index, row in postcategorydetails.iterrows():
    text_description = row['Name']

    # process NULL data in FullDescriptions column
    if text_description == None:
        postcategorydetails_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=target)
        postcategorydetails_Description_Translated.append(output_description['translatedText'])
# add 1 new columns translated

postcategorydetails['Description_Translated'] = postcategorydetails_Description_Translated

# PostDetails
postdetails_Title_Translated = []
postdetails_MetaDescription_Translated = []
postdetails_Body_Translated = []
postdetails_ShortDescription_Translated = []
for index, row in postdetails.iterrows():
    text_title = row['Title']
    text_metadescription = row['MetaDescription']
    text_body = row['Body']
    text_shortdescription = row['ShortDescription']

    # process NULL data
    if text_title == None:
        postdetails_Title_Translated.append(text_title)
    else:
        output_description = translate_client.translate(text_title, target_language=target)
        postdetails_Title_Translated.append(output_description['translatedText'])

    # process NULL data
    if text_metadescription == None:
        postdetails_MetaDescription_Translated.append(text_metadescription)
    else:
        output_description = translate_client.translate(text_metadescription, target_language=target)
        postdetails_MetaDescription_Translated.append(output_description['translatedText'])

    # process NULL data
    if text_body == None:
        postdetails_Body_Translated.append(text_body)
    else:
        output_description = translate_client.translate(text_body, target_language=target)
        postdetails_Body_Translated.append(output_description['translatedText'])

    # process NULL data
    if text_shortdescription == None:
        postdetails_ShortDescription_Translated.append(text_shortdescription)
    else:
        output_description = translate_client.translate(text_shortdescription, target_language=target)
        postdetails_ShortDescription_Translated.append(output_description['translatedText'])
# add 1 new columns translated

postdetails['Title_Translated'] = postdetails_Title_Translated
postdetails['MetaDescription_Translated'] = postdetails_MetaDescription_Translated
postdetails['Body_Translated'] = postdetails_Body_Translated
postdetails['ShortDescription_Translated'] = postdetails_ShortDescription_Translated


# insert dataframe into sql server
cursor = cnxn.cursor()

# PostCategoryDetails
for index, row in postcategorydetails.iterrows():
    cursor.execute("UPDATE [dbo].[PostCategoryDetails] SET	Name = ? WHERE PostCategoryFID = ? and LanguageFID = ?",
                   row.Description_Translated,
                   row.PostCategoryFID,
                   languagefid
                   )

# PostDetails
for index, row in postdetails.iterrows():
    cursor.execute("UPDATE [dbo].[PostDetails] SET	Title = ?, MetaDescription = ?, Body = ?, ShortDescription = ?  WHERE PostFId = ? and LanguageFID = ?",
                   row.Title_Translated,
                   row.MetaDescription_Translated,
                   row.Body_Translated,
                   row.ShortDescription_Translated,
                   row.PostFId,
                   languagefid
                   )



cnxn.commit()
cursor.close()
