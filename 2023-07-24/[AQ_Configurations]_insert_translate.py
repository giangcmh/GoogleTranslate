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
                         "uid=translate;"
                         "pwd=456wsx765&*;",
                         autocommit=True
                         )


# init translate language
translate_client = translate_v2.Client()

language = {
    2: "zh-CN",
    4: "th",
    6: "zh-TW",
    5: "vi"
}

commonresources = pd.DataFrame()

################################################################# Get dataframe

# CommonResources
for key, value in language.items():
    ###Insert / 1 as is_insert
    commonresources_insert = pd.read_sql(
        f'''select	1 as is_insert,
                    {key} as translatefid,
                    '{value}' as language_translate,
                    * 
            from	[dbo].[CommonResources] A 
            where	LanguageFID = 1 
            and     not exists (select 1 
                                from [dbo].[CommonResources] B 
                                where A.ResourceKey = B.ResourceKey 
                                and B.LanguageFID = {key})
        '''
        , cnxn, index_col='ID')
    commonresources = pd.concat([commonresources, commonresources_insert], ignore_index=True)


############################################################## Call Function Translate

# CommonResources
commonresources_Description_Translated = []
for index, row in commonresources.iterrows():
    language_translate = row['language_translate']
    text_description = row['ResourceValue']

    # process NULL data in FullDescriptions column
    if text_description == None:
        commonresources_Description_Translated.append(text_description)
    else:
        output_description = translate_client.translate(text_description, target_language=language_translate)
        commonresources_Description_Translated.append(output_description['translatedText'])
# add 1 new columns translated
commonresources['Description_Translated'] = commonresources_Description_Translated

################################################################# Insert dataframe into tables
cursor = cnxn.cursor()

# CommonResources
for index, row in commonresources.iterrows():
    cursor.execute('''INSERT INTO [dbo].[CommonResources]([ResourceKey],
                                                            [LanguageFID],
                                                            [ResourceValue],
                                                            [TypeOfResource],
                                                            [Descriptions]) 
                VALUES (?,?,?,?,?)
                ''',
                row.ResourceKey,
                row.language_translate,
                row.Description_Translated,
                row.TypeOfResource,
                row.Descriptions
                )


cnxn.commit()
cursor.close()
