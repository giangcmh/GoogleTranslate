import pandas as pd
import warnings

warnings.filterwarnings('ignore')


class CommonFunction:

    def __init__(self, databasename, tables, exception_tables, language,cnxn):
        self.databasename = databasename
        self.tables = tables
        self.exception_tables = exception_tables
        self.language = language
        self.cnxn = cnxn
        self.translate_date = {}
        self.exception_translate_date = {}
        self.insert_track = []
        self.exception_insert_track = []
        self.df = pd.DataFrame()

    def get_last_translated_date(self):
        ### Normal Tables
        for key, value in self.tables.items():
            df_translate_date = pd.read_sql(
                f'''with cte_get_10_date_asc as (
                                        select	distinct top 10 COALESCE(LastModifiedDate, CreatedDate) as LastModifiedDate
                                        from	[dbo].[{key}]
                                        where	LanguageFID = 1
                                        order by LastModifiedDate
                                        ),
                                        cte_get_all as (
                                        select	LastModifiedDate
                                        from	cte_get_10_date_asc
                                        UNION ALL
                                        select	max([LastTranlastedDate])
                                        from	[AQ_Configurations].[dbo].[Translate_Tracking_Log]
                                        where	[Database_Name] = '{self.databasename}'
                                        and		[Table_Name] = '{key}'
                                        )
                                        select convert(nvarchar(15), max(LastModifiedDate),102) as LastTranlastedDate from cte_get_all
                                ''', self.cnxn)
            self.translate_date[key] = [df_translate_date['LastTranlastedDate'][0], value]

        ### Exception Tables
        for key, value in self.exception_tables.items():
            df_exception_translate_date = pd.read_sql(
                f'''with cte_get_10_date_asc as (
                                        select	distinct top 10 COALESCE(LastModifiedDate, ActivatedDate) as LastModifiedDate
                                        from	[dbo].[{key}]
                                        where	LanguageFID = 1
                                        order by LastModifiedDate
                                        ),
                                        cte_get_all as (
                                        select	LastModifiedDate
                                        from	cte_get_10_date_asc
                                        UNION ALL
                                        select	max([LastTranlastedDate])
                                        from	[AQ_Configurations].[dbo].[Translate_Tracking_Log]
                                        where	[Database_Name] = '{self.databasename}'
                                        and		[Table_Name] = '{key}'
                                        )
                                        select convert(nvarchar(15), max(LastModifiedDate),102) as LastTranlastedDate from cte_get_all
                                ''', self.cnxn)
            self.exception_translate_date[key] = [df_exception_translate_date['LastTranlastedDate'][0], value]

        return self.translate_date, self.exception_translate_date
