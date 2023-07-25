import pandas as pd
import warnings

warnings.filterwarnings('ignore')


class CommonFunction:

    def __init__(self, databasename, tables, exception_tables, language,cnxn, translate_client):
        self.databasename = databasename
        self.tables = tables
        self.exception_tables = exception_tables
        self.language = language
        self.cnxn = cnxn
        self.translate_client = translate_client
        self.translate_date = {}
        self.exception_translate_date = {}
        self.insert_track = []
        self.exception_insert_track = []
        self.normal_df = pd.DataFrame()
        self.exception_df = pd.DataFrame()
        self.normal_list_column = []
        self.exception_list_column = []

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

            self.translate_date[key] = [df_translate_date['LastTranlastedDate'][0], value[0]]

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

            self.exception_translate_date[key] = [df_exception_translate_date['LastTranlastedDate'][0], value[0]]

        return self.translate_date, self.exception_translate_date

    def get_data_frame(self):
        translate_date, exception_translate_date = self.get_last_translated_date()

        ### Normal Tables
        for key, value in self.language.items():
            for translate_key, translate_value in translate_date.items():
                ###Insert / 1 as is_insert
                normal_insert = pd.read_sql(
                    f'''select	1 as is_insert,
                                {key} as translatefid,
                                '{value}' as language_translate,
                                '{translate_key}' as table_name,
                                0 as world_translated,
                                * 
                        from	[dbo].[{translate_key}] A 
                        where	LanguageFID = 1 
                        and     not exists (select 1 
                                            from [dbo].[{translate_key}] B 
                                            where A.{translate_value[1]} = B.{translate_value[1]} 
                                            and B.LanguageFID = {key}) 
                        and     Deleted = 0
                        and		PostCategoryFID in (1, 27)
                    '''
                    , self.cnxn, index_col='ID')
                self.normal_df = pd.concat([self.normal_df, normal_insert], ignore_index=True)

                ###Update / 0 as is_insert
                normal_update = pd.read_sql(
                    f'''select	0 as is_insert,
                                {key} as translatefid,
                                '{value}' as language_translate,
                                '{translate_key}' as table_name,
                                0 as world_translated,
                                * 
                        from	[dbo].[{translate_key}] A 
                        where	LanguageFID = 1 
                        and     Deleted = 0 
                        and     convert(nvarchar(15), COALESCE(LastModifiedDate, CreatedDate),102)
                            <= '{translate_value[1]}'
                        and		PostCategoryFID in (1, 27)
                    '''
                    , self.cnxn, index_col='ID')
                self.normal_df = pd.concat([self.normal_df, normal_update], ignore_index=True)

        # check insert into track log
        if self.normal_df.empty:
            self.insert_track.append(0)
        else:
            self.insert_track.append(1)

        ### Exception Tables
        for key, value in self.language.items():
            for translate_key, translate_value in exception_translate_date.items():
                ###Insert / 1 as is_insert
                exception_insert = pd.read_sql(
                    f'''select	1 as is_insert,
                                {key} as translatefid,
                                '{value}' as language_translate,
                                '{translate_key}' as table_name,
                                0 as world_translated,
                                * 
                        from	[dbo].[{translate_key}] A 
                        where	LanguageFID = 1 
                        and not exists (select 1 
                                        from [dbo].[{translate_key}] B 
                                        where A.{translate_value[1]} = B.{translate_value[1]} 
                                        and B.LanguageFID = {key}) 
                        and     Deleted = 0
                        and		PostFId = 89
                    '''
                    , self.cnxn, index_col='ID')
                self.exception_df = pd.concat([self.exception_df, exception_insert], ignore_index=True)

                ###Update / 0 as is_insert
                exception_update = pd.read_sql(
                    f'''select	0 as is_insert,
                                {key} as translatefid,
                                '{value}' as language_translate,
                                '{translate_key}' as table_name,
                                0 as world_translated,
                                * 
                        from	[dbo].[{translate_key}] A 
                        where	LanguageFID = 1 
                        and     Deleted = 0 
                        and     convert(nvarchar(15), COALESCE(LastModifiedDate, ActivatedDate),102)
                            <= '{translate_value[1]}'
                        and		PostFId = 89
                    '''
                    , self.cnxn, index_col='ID')
                self.exception_df = pd.concat([self.exception_df, exception_update], ignore_index=True)

        # check insert into track log
        if self.exception_df.empty:
            self.exception_insert_track.append(0)
        else:
            self.exception_insert_track.append(1)

        return self.normal_df, self.exception_df


    def call_api_translate(self):
        self.normal_df, self.exception_df = self.get_data_frame()
        for key, value in self.tables.items():
            for i in range(0,len(value[1])):
                self.normal_list_column.append(value[1][i])

        for key, value in self.exception_tables.items():
            for i in range(0,len(value[1])):
                self.exception_list_column.append(value[1][i])

        for index, row in self.normal_df.iterrows():
            word_translated = 0
            for i in range(0,len(self.normal_list_column)):
                language_translate = row['language_translate']
                column_name = self.normal_list_column[i]
                text_description = row[f'{column_name}']
                word_translated += len(text_description)

                # process NULL data in column
                if text_description != None:
                    output_description = self.translate_client.translate(text_description,
                                                                        target_language=language_translate)
                    self.normal_df.at[index, f'{column_name}'] = output_description['translatedText']
            # self.normal_df.iloc[index]['word_translated']= word_translated
            self.normal_df.at[index, 'after_word_translated'] = word_translated

        for index, row in self.exception_df.iterrows():
            word_translated = 0
            for i in range(0,len(self.exception_list_column)):
                language_translate = row['language_translate']
                column_name = self.exception_list_column[i]
                text_description = row[f'{column_name}']
                word_translated += len(text_description)
                # process NULL data in column
                if text_description != None:
                    output_description = self.translate_client.translate(text_description,
                                                                        target_language=language_translate)
                    self.exception_df.at[index, f'{column_name}'] = output_description['translatedText']
            self.exception_df.at[index, 'after_word_translated'] = word_translated
        return self.normal_df, self.exception_df

    def insert_tracking_log(self):
        cursor = self.cnxn.cursor()
        #normal
        for i in range(0, len(self.insert_track)):
            for key, value in self.translate_date.items():
                if self.insert_track[i] == 1:
                    cursor.execute(
                        f'''INSERT INTO [AQ_Configurations].[dbo].[Translate_Tracking_Log]([Database_Name],
                                                                                            [Table_Name],
                                                                                            [LastTranlastedDate])
                            VALUES (?,?,?)
                        ''',
                        self.databasename,
                        key,
                        value[0]
                    )
        #exception
        for i in range(0, len(self.exception_insert_track)):
            for key, value in self.exception_translate_date.items():
                if self.exception_insert_track[i] == 1:
                    cursor.execute(
                        f'''INSERT INTO [AQ_Configurations].[dbo].[Translate_Tracking_Log]([Database_Name],
                                                                                            [Table_Name],
                                                                                            [LastTranlastedDate])
                            VALUES (?,?,?)
                        ''',
                        self.databasename,
                        key,
                        value[0]
                    )

    def insert_tracking_row_and_word(self):
        # self.normal_df, self.exception_df = self.call_api_translate()
        tracking_normal_df = self.normal_df.groupby('table_name') \
                                            .agg({'is_insert': 'count',
                                                  'after_word_translated': 'sum'}) \
                                            .reset_index() \
                                            .rename(columns={'is_insert': 'row_count'})

        tracking_exception_df = self.exception_df.groupby('table_name') \
                                                    .agg({'is_insert': 'count',
                                                          'after_word_translated': 'sum'}) \
                                                    .reset_index() \
                                                    .rename(columns={'is_insert': 'row_count'})
        cursor = self.cnxn.cursor()
        # normal
        for index, row in tracking_normal_df.iterrows():
            cursor.execute(
                f'''INSERT INTO [AQ_Configurations].[dbo].[Translate_Tracking_RowAndWord_Log]([Database_Name],
                                                                                            [Table_Name],
                                                                                            [Translated_Row],
                                                                                            [Translated_Word],
                                                                                            [CreatedDate])
                            VALUES (?,?,?,?,getdate())
                        ''',
                self.databasename,
                row['table_name'],
                row['row_count'],
                row['after_word_translated']
            )
        # exception
        for index, row in tracking_exception_df.iterrows():
            cursor.execute(
                f'''INSERT INTO [AQ_Configurations].[dbo].[Translate_Tracking_RowAndWord_Log]([Database_Name],
                                                                                            [Table_Name],
                                                                                            [Translated_Row],
                                                                                            [Translated_Word],
                                                                                            [CreatedDate])
                            VALUES (?,?,?,?,getdate())
                        ''',
                self.databasename,
                row['table_name'],
                row['row_count'],
                row['after_word_translated']
            )