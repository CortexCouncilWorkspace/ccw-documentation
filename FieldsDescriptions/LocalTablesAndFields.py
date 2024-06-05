from typing import Any, Dict, List, Optional, Union
from mo_sql_parsing import parse
import os
import pandas as pd

class LocalTablesAndFields:

    def __init__(self) -> None:
        self.script_directory = os.path.dirname(os.path.abspath(__file__))

    @classmethod
    def extract_name_value_pairs(cls, data: Optional[Union[Dict, List]],
                                column_names: List[str],
                                data_type: str,
                                previous_data: Optional[Union[Dict, List]] = None) -> List[Dict]:
        """
        Extract name-value pairs from JSON data.

        Args:
            data: JSON data to extract from.
            column_names: List of column names.
            data_type: Type of data extraction.
            previous_data: JSON data before the current data.

        Returns:
            List of extracted name-value pairs.
        """
        if data is None:
            return []

        pairs = []

        if isinstance(data, list):
            for item in data:
                pairs.extend(cls.extract_name_value_pairs(item, column_names, data_type, data))
        elif isinstance(data, dict):
            if 'value' in data and 'name' in data:
                pairs.append({column_names[0]: data['value'], column_names[1]: data['name']})
            elif 'value' in data and 'name' not in data and 'over' not in data:
                pairs.append({column_names[0]: data['value'], column_names[1]: data['value']})
            elif 'value' not in data and 'name' in data:
                pairs.append({column_names[0]: data['name'], column_names[1]: data['name']})
            elif 'inner' in data:
                pairs.append({column_names[0]: data['inner join'], column_names[1]: data['inner join']})
            else:
                for key, value in data.items():
                    if key != 'on':
                        if isinstance(value, (dict, list)):
                            if data_type == 'tables' and key != 'select' and 'by' not in key:
                                pairs.extend(cls.extract_name_value_pairs(value, column_names, data_type, data))
                            elif data_type == 'columns':
                                pairs.extend(cls.extract_name_value_pairs(value, column_names, data_type, data))
                    if 'join' in key and isinstance(value, str):
                        pairs.append({column_names[0]: value, column_names[1]: value})

        return pairs

    @classmethod
    def get_used_tables_and_views(cls, source_list: Union[str, List[str]]) -> pd.DataFrame:
      columns = ['Used Tables','Alias']
      if isinstance(source_list, str):     
          name_value_pairs = []   
          name_value_pairs.append({'Used Tables': source_list, 'Alias': source_list})    
          df_tables = pd.DataFrame(name_value_pairs)         
      else:    
          name_value_pairs = cls.extract_name_value_pairs(source_list,columns,'tables','')

      if isinstance(source_list, list):
          for item in source_list:
              if isinstance(item, str):
                  name_value_pairs.append({'Used Tables': item, 'Alias': item}) 
          
      df_tables = pd.DataFrame(name_value_pairs)
      if df_tables.empty:
        return []
      else:
        df_tables['Used Tables'] = df_tables['Used Tables'].replace(r'\.\.', '.', regex=True)
        df_tables['Alias'] = df_tables['Alias'].replace(r'\.\.', '.', regex=True)
        return df_tables
    
    @classmethod
    def get_columns(cls, js_select: Optional[Union[str, Dict]],
                    js_from: Optional[Union[str, Dict]]) -> pd.DataFrame:
        columns = ['Source Field', 'Target Field']
        if js_select is None or js_from is None:
            return pd.DataFrame(columns=columns)

        if isinstance(js_select, str):
            name_value_pairs = [{'Alias': js_from, 'Source Field': js_from}]
        else:
            name_value_pairs = cls.extract_name_value_pairs(
                js_select, columns, 'columns', '')

        columns_data = []
        for item in name_value_pairs:
            alias = None
            source_field = None
            source_field_value = item.get('Source Field')
            if source_field_value is None:
                continue
            if isinstance(source_field_value,str):   
                source_field_value = source_field_value.replace('..', '.')
                if '.' in source_field_value:
                    alias, source_field = source_field_value.rsplit('.', 1)
                else:
                    alias, source_field = '', source_field_value

            target_field = (item.get('Target Field').split('.', 1)[-1]
                            if '.' in item.get('Target Field')
                            else item.get('Target Field'))
            if target_field is None:
                continue
            if not alias is None and not source_field is None:
                columns_data.append(
                    {'Alias': alias, 'Source Field': source_field,
                    'Target Field': target_field})

        return pd.DataFrame(data=columns_data)
            
    @classmethod
    def get_used_tables_and_columns(cls,View_name,query):      
        df_tables = []  
        # Parse query data
        try:
            parse_query = parse(query)            
        except Exception as e:
            return []
                       
        # Get used tables
        js_from = parse_query.get('from')    
        df_tables = cls.get_used_tables_and_views(js_from)
        # Get Columns
        js_select = parse_query.get('select')
        df_columns = cls.get_columns(js_select,js_from)        
        # Get With Queries
        js_with = parse_query.get('with')
        if js_with:    

            with_tables_all = pd.DataFrame()
            with_columns_all = pd.DataFrame()

            if isinstance(js_with,list):
                
                for line in js_with: 
                    # Get With name
                    with_name = line.get('name')

                    # Get Tables
                    with_value = line.get('value')
                    with_from = with_value.get('from')
                    if with_from:
                        with_tables = cls.get_used_tables_and_views(with_from)
                        # Get Columns
                        with_cols = with_value.get('select')
                        with_columns = cls.get_columns(with_cols,js_from)
                        # print(df_columns)
                    else:
                        with_tables = cls.get_used_tables_and_views(with_value)            
                        with_columns = cls.get_columns(with_value,js_from)
                    if len(with_tables) == 0:
                        continue
                    with_tables['with_name'] = with_name
                    with_columns['with_name'] = with_name
                    if not with_tables_all.empty:
                        with_tables_all = pd.concat([with_tables_all, with_tables])
                        with_columns_all = pd.concat([with_columns_all, with_columns])
                    else:
                        with_tables_all = with_tables                        
                        with_columns_all = with_columns
            else:
                with_name = js_with.get('name')

                # Get Tables
                with_value = js_with.get('value')
                with_from = with_value.get('from')
                if with_from:
                    with_tables = cls.get_used_tables_and_views(with_from)
                    # Get Columns
                    with_cols = with_value.get('select')
                    with_columns = cls.get_columns(with_cols,js_from)
                    # print(df_columns)
                else:
                    with_tables = cls.get_used_tables_and_views(with_value)            
                    with_columns = cls.get_columns(with_value,js_from)

                with_tables['with_name'] = with_name
                with_columns['with_name'] = with_name
                if not with_tables_all.empty:
                    with_tables_all = pd.concat([with_tables_all, with_tables])
                    with_columns_all = pd.concat([with_columns_all, with_columns])
                else:
                    with_tables_all = with_tables                        
                    with_columns_all = with_columns            

            if with_tables_all.empty == False and with_columns_all.empty == False:
                with_tables_all = with_tables_all[with_tables_all['Used Tables'].apply(lambda x: isinstance(x, str))]
                with_tables_all = with_tables_all.drop_duplicates()
                with_tab_cols = pd.merge(with_tables_all[['Used Tables','Alias']], with_columns_all, on='Alias',how='right')  
                with_tab_fil = with_tab_cols.loc[with_tab_cols['Used Tables'].isna()]   
                with_tab_fil = with_tab_fil[['Source Field','Target Field','with_name']]
                with_tab_fil = pd.merge(with_tables_all[['Used Tables','Alias','with_name']], with_tab_fil, on='with_name',how='inner')  
                with_tab_cols = with_tab_cols[with_tab_cols['Used Tables'].notna()]
                with_tab_cols = pd.concat([with_tab_cols,with_tab_fil],ignore_index=True)

                if df_tables.empty == False:
                  df_tables = df_tables[~df_tables['Alias'].isin(with_tables_all['with_name'])]    
        
        # Get subquery
        sub_table_all = pd.DataFrame()
        sub_columns_all = pd.DataFrame()
        if len(df_tables) > 0:
            for index,row in df_tables.iterrows():
                if isinstance(row['Used Tables'], dict):        
                    sub_table = cls.get_used_tables_and_views(row['Used Tables'].get('from'))
                    sub_columns = cls.get_columns(row['Used Tables'].get('select'),js_from)  
                    if sub_table_all.empty:
                        sub_table_all = sub_table
                        sub_columns_all = sub_columns
                    else:
                        sub_table_all = pd.concat([sub_table_all, sub_table])          
                        sub_columns_all = pd.concat([sub_columns_all, sub_columns])
                    sub_table_all['Subname'] = row['Alias']
                    sub_columns_all['Subname'] = row['Alias']

        if 'with_tables_all' in locals():
            for index,row in with_tables_all.iterrows():
                if isinstance(row['Used Tables'], dict):        
                    sub_table = cls.get_used_tables_and_views(row['Used Tables'].get('from'))
                    sub_columns = cls.get_used_tables_and_views(row['Used Tables'].get('select'))  
                    if sub_table_all.empty:
                        sub_table_all = sub_table
                        sub_columns_all = sub_columns
                    else:
                        sub_table_all = pd.concat([sub_table_all, sub_table])          
                        sub_columns_all = pd.concat([sub_columns_all, sub_columns])
                    sub_table_all['Subname'] = row['Alias']
                    sub_columns_all['Subname'] = row['Alias']
  
        if sub_table_all.empty == False and sub_columns_all.empty == False:        
            sub_table_all = sub_table_all.drop_duplicates()
            sub_tab_cols = pd.merge(sub_table_all[['Used Tables','Alias']], sub_columns_all, on='Alias',how='right')  
            sub_tab_fil = sub_tab_cols.loc[sub_tab_cols['Used Tables'].isna()]   
            sub_tab_fil = sub_tab_fil[['Source Field','Target Field','Subname']]
            sub_tab_fil = pd.merge(sub_table_all[['Used Tables','Alias','Subname']], sub_tab_fil, on='Subname',how='inner')  
            sub_tab_cols = sub_tab_cols[sub_tab_cols['Used Tables'].notna()]
            sub_tab_cols = pd.concat([sub_tab_cols,sub_tab_fil],ignore_index=True)

            df_tables = df_tables[~df_tables['Alias'].isin(sub_table_all['Subname'])]
            # print(df_tables)

        # Output Data 
        output = []

        for index, row in df_columns.iterrows():
            Alias = row['Alias']
            src_field = row['Source Field']
            
            # Get data from withquery
            if 'with_tables_all' in locals():
                if with_tables_all.empty == False and with_columns_all.empty == False:
                    conditions = (with_tab_cols['with_name'] == Alias) & (with_tab_cols['Target Field'] == src_field)
                    with_alias = with_tab_cols.loc[conditions]
                    if with_alias.empty:
                        # Verify if table refer to withquery
                        conditions = (with_tables_all['with_name'] == Alias)
                        with_Alias = with_tables_all.loc[conditions,'Alias']
                        for item in with_Alias:
                            conditions = (with_tab_cols['with_name'] == item) & (with_tab_cols['Target Field'] == src_field)
                            with_alias = with_tab_cols.loc[conditions]
                    for w_ind, w_row in with_alias.iterrows():
                        output.append({'View': View_name,'Table': w_row['Used Tables'], 'SourceField':src_field, 'TargetField':row['Target Field']})          

            # Get data from subquery
            if 'sub_table_all' in locals():
                if sub_table_all.empty == False and sub_columns_all.empty == False: 
                    conditions = (sub_tab_cols['Subname'] == Alias) & (sub_tab_cols['Target Field'] == src_field)
                    sub_alias = sub_tab_cols.loc[conditions]
                    if sub_alias.empty:
                        # Verify if table refer to withquery
                        conditions = (sub_table_all['Subname'] == Alias)
                        sub_Alias = sub_table_all.loc[conditions,'Alias']
                        for item in sub_Alias:
                            conditions = (sub_tab_cols['Subname'] == item) & (sub_tab_cols['Target Field'] == src_field)
                            sub_alias = sub_tab_cols.loc[conditions]
                    for w_ind, w_row in sub_alias.iterrows():
                        output.append({'View': View_name,'Table': w_row['Used Tables'], 'SourceField':src_field, 'TargetField':row['Target Field']})   

            # Get data from table
            if df_tables.empty == False:
              conditions = (df_tables['Alias'] == Alias)
              df_tables_alias = df_tables.loc[conditions]
              for w_ind, w_row in df_tables_alias.iterrows():
                  output.append({'View': View_name,'Table': w_row['Used Tables'], 'SourceField':src_field, 'TargetField':row['Target Field']})   
                                        
        return pd.DataFrame(output)         

# cls = LocalTablesAndFields()
# # Work only with aliases and identified fields, this query works 
# query = """
# select tst.* from tbl_teste tst
# """
# # This query doesn't work
# # query = """
# # select field from tbl_teste
# # """
# # This query doesn't work
# # query = """
# # select tst.* from tbl_teste tst
# # """
# View_name = 'bseg'
# df_output = cls.get_used_tables_and_columns(View_name,query)        
# path = f'{cls.script_directory}/Test/LocalTableFields.csv'
# df_output.to_csv(f'{path}', index=False)   