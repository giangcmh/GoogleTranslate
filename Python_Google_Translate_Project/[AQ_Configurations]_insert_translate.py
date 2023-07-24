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
                         "Database=AQ_Configurations;"
                         "uid=giangcmh;"
                         "pwd=123qaz098@*;",
                         autocommit=True
                         )

# CommonResources
commonresources = pd.read_sql("select	* from	[dbo].[CommonResources] A where	LanguageFID = 1 and not exists (select 1 from [dbo].[CommonResources] B where A.ResourceKey = B.ResourceKey and B.LanguageFID = 5)"
                   , cnxn)

# init translate VietNam language
translate_client = translate_v2.Client()
target = "vi"
# LanguageFID
## 5 : Vietnamese
languagefid = 5

##############################################################

# CommonResources
commonresources_Description_Translated = []
for index, row in commonresources.iterrows():
    text_description = row['ResourceValue']

    # process NULL data in FullDescriptions column
    if text_description == None:
        commonresources_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=target)
        commonresources_Description_Translated.append(output_description['translatedText'])
# add 1 new columns translated

commonresources['Description_Translated'] = commonresources_Description_Translated

#################################################################
# insert dataframe into sql server
cursor = cnxn.cursor()

# CommonResources
for index, row in commonresources.iterrows():
    cursor.execute("INSERT INTO [dbo].[CommonResources]([ResourceKey],[LanguageFID],[ResourceValue],[TypeOfResource],[Descriptions]) VALUES (?,?,?,?,?)",
                   row.ResourceKey,
                   languagefid,
                   row.Description_Translated,
                   row.TypeOfResource,
                   row.Descriptions
                   )


cnxn.commit()
cursor.close()
