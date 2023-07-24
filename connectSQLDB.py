import os
from google.cloud import translate_v2
import pyodbc
import pandas as pd
import warnings

warnings.filterwarnings('ignore')

col2 = 6
col3 = 7
#################################################################
# init credentials google translate api
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\PythonProject\GoogleTranslate\aqtranslate_key.json"

# init connection SQL Server
cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                         "Server=(local);"
                         "Database=test;"
                         "uid=giangcmh;"
                         "pwd=123qaz098@*;",
                         autocommit=True
                         )

df_test = pd.read_sql("SELECT * FROM [test].[dbo].[Test]"
                   , cnxn, index_col='col1')


cursor = cnxn.cursor()
cursor.execute("UPDATE [dbo].[Test] SET col2 = ?, col3 = ? where col1 > 2", col2, col3)
cnxn.commit()
cursor.close()
