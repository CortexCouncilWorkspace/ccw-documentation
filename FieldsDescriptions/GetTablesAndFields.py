import sys, os, re
from google.cloud import bigquery
from vertexai.preview.generative_models import GenerativeModel, HarmCategory, HarmBlockThreshold
import vertexai

# Get the absolute path of the parent folder
parent_folder_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
# Add the parent folder to the system path
sys.path.append(parent_folder_path)
# Import from the parent folder
from globalfunctions import *
from FieldsDescriptions.TraceUsedTables import *
from FieldsDescriptions.LocalTablesAndFields import *

class GetTablesAndFields:
    def __init__(self):        
        
        # Get the directory of the current script
        self.path = os.path.dirname(os.path.abspath(__file__))

        # Get the parent folder
        self.parent_folder = os.path.dirname(self.path)
                
        self.var_cl_global = globalfunctions()

        # Set priority settings
        self.variables = self.var_cl_global.get_all_variables('config/config.json')        
        self.project_id = self.variables['projectId'] # ProjectId for all        
        self.authJson = self.variables['authJson'] # Auth JSON for all         

        # Set Catalog Variables      
        self.catalog_vars = self.variables.get("Catalog", {})  
        if not self.project_id: 
            self.cat_projectId = self.catalog_vars['projectId'] # ProjectID for BigQuery Dataset Catalog Tables 
            self.cat_authJson = self.catalog_vars['authJson'] # Auth JSON for BigQuery Dataset Catalog Tables 
        else:
            self.cat_projectId = self.project_id
            self.cat_authJson = self.authJson
        self.cat_datasetId = self.catalog_vars['datasetId'] # For BigQuery Dataset Catalog (DD03L, DD02T, DD04T)

        # Set Documentation Variables     
        self.catalog_vars = self.variables.get("Documentation", {})  
        if not self.project_id: 
            self.doc_projectId = self.catalog_vars['projectId'] # ProjectID for BigQuery Dataset Documentation views 
            self.doc_authJson = self.catalog_vars['authJson'] # Auth JSON for BigQuery Dataset Documentation views
        else:
            self.doc_projectId = self.project_id
            self.doc_authJson = self.authJson        
        self.doc_datasetId = self.catalog_vars['datasetId'] # For BigQuery Dataset Documentation
        self.doc_languages = self.catalog_vars['languages'] # Portuguese, English...
        self.doc_views = self.catalog_vars['views'] # Views 
               
        # Set Vertex Variables
        self.catalog_vars = self.variables.get("Vertex", {})  
        if not self.project_id: 
            self.ver_projectId = self.catalog_vars['projectId'] # ProjectID for Vertex
            self.ver_authJson = self.catalog_vars['authJson'] # Auth JSON for Vertex
        else:
            self.ver_projectId = self.project_id
            self.ver_authJson = self.authJson        
        self.gemini = self.catalog_vars['gemini'] 
        self.location = self.catalog_vars['location']   

        # Get credentials
        vertex_scope = [
            "https://www.googleapis.com/auth/cloud-platform",            
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery"
        ]         
        credentials = self.var_cl_global.authentication(self.ver_authJson,vertex_scope) 
        vertexai.init(project=self.ver_projectId, location=self.location, credentials=credentials) 
       
        # Create a BigQuery client using the JSON key file
        self.cat_BQclient = bigquery.Client.from_service_account_json(f'{self.parent_folder}/{self.cat_authJson}')
        self.doc_BQclient = bigquery.Client.from_service_account_json(f'{self.parent_folder}/{self.doc_authJson}')
        
    def get_bq_views(self):
        # Get tables in the dataset and filter for views
        tables = self.doc_BQclient.list_tables(self.doc_datasetId)
        views = [table.table_id for table in tables if table.table_type == "VIEW"]
        if len(self.doc_views) > 0:
            views = [view for view in views if view in self.doc_views]         
        return views
    
    def get_view_metadata(self,view_id,projectid,datasetid):
        view_ref = self.doc_BQclient.dataset(datasetid, project=projectid).table(view_id)
        try:
            view = self.doc_BQclient.get_table(view_ref)
        except:     
            return None           
        return view        

    def language_code(self):
        df_language_base = pd.read_csv(
            f'{self.path}/config/Language.csv', sep=';')
        df_language = df_language_base[df_language_base['Language_Description'].str.upper().isin(
            list(map(str.upper, self.doc_languages)))].reset_index(drop=True)
        return df_language_base, df_language['Language'].tolist() 

    def get_bq_query_data(self,query):
        try:
            # Execute the SQL query
            query_job = self.cat_BQclient.query(query)

            # Fetch the results as a Pandas DataFrame
            return query_job.to_dataframe()
        except Exception as e:
            print(f"Error executing query: {e}")
            return None 
        
    def get_customer_catalog(self,table_list):

        # Convert language description to code
        df_lang, language = self.language_code()          
        language = ", ".join([f"'{item.upper()}'" for item in language])
        table = ", ".join([f"'{item.split('.')[-1].upper()}'" for item in table_list])        
            
        # SQL query to execute
        query = f"""
        SELECT DD03L.TABNAME as Table,
               DD03L.FIELDNAME as Field,
               IFNULL(DD04T.DDTEXT,DD03L.FIELDNAME) as FieldDescription,
               DD02T.ddlanguage as Language
        FROM `{self.cat_datasetId}.dd02t`  DD02T
        INNER JOIN `{self.cat_datasetId}.dd03l` DD03L
            ON DD02T.tabname = DD03L.tabname
            AND DD03L.comptype = 'E' 
            LEFT JOIN `{self.cat_datasetId}.dd04t` DD04T
            ON DD03L.ROLLNAME = DD04T.ROLLNAME   
            AND DD02T.ddlanguage = DD04T.ddlanguage   
        where UPPER(DD02T.ddlanguage) in ({language})
          and UPPER(DD03L.TABNAME) in ({table})                      
        """    
        
        df_cat = self.get_bq_query_data(query)    
        df_cat = pd.merge(df_cat, df_lang, on='Language', how='inner')   
        df_cat = df_cat[['Table','Field','FieldDescription','Language_Description']]  
        df_cat = df_cat.rename(columns={'Language_Description': 'Language'})
        return df_cat

    def remove_commented_lines(self,query):
        # Remove comments starting with #
        query = re.sub(r'#.*', '', query)    
        # Remove comments starting with --
        query = re.sub(r'--.*', '', query)
        # Remove comments enclosed in /* */
        query = re.sub(r'/\*.*?\*/', '', query, flags=re.DOTALL)    
        return query
    
    def get_bq_views_dependencies(self,view,position,projectId,datasetId,view_list,df_LTabField_all):
        # Get view metadata                
        print(f'Running - Get dependencies - {view}')
        view_detail = self.get_view_metadata(view, projectId, datasetId)  
        if view_detail: 
            if view_detail.table_type == 'VIEW':  
                position = position + 1
                # Iterate through the list and modify matching
                for item in view_list:
                    if item.get('view') == view:
                        # Modify the 'Position' key in the matching dictionary
                        item['Position'] = item['Position'] + 1          
                        return view_list,df_LTabField_all                      
                view_list.append({'view': view, 'Position': position})                                  
                query = self.remove_commented_lines(view_detail.view_query)    
                # Get used tables
                df_LTabField = cls_localtabfiel.get_used_tables_and_columns(view,query)  
                if df_LTabField_all.empty:
                    df_LTabField_all = df_LTabField.copy()
                elif len(df_LTabField) > 0:
                    df_LTabField_all = pd.concat([df_LTabField_all, df_LTabField])       
                if len(df_LTabField) > 0:
                    tables = df_LTabField['Table'].drop_duplicates().tolist()                 
                    for table in tables:
                        if table.count('.') == 2:
                            # table = re.sub(r"['`]+|(\bAND\b)", "", table)
                            split_table = table.split('.')                    
                            view_list,df_LTabField_all = self.get_bq_views_dependencies(split_table[2],position,split_table[0],split_table[1],view_list,df_LTabField_all)
        return view_list,df_LTabField_all 

    def merge_dicts(self,d1, d2):
        for key, value in d2.items():
            if key in d1:
                if isinstance(d1[key], dict) and isinstance(value, dict):
                    self.merge_dicts(d1[key], value)
                elif isinstance(d1[key], list) and isinstance(value, list):
                    d1[key].extend(x for x in value if x not in d1[key])
                else:
                    d1[key] = value
            else:
                d1[key] = value
                
    def gemini_run(self,query,columns_notin,column_of_view,language,clear_json_before):                  

        replacements = {'+input_sql+': query,'+input_fields+':columns_notin,'+input_language+': language}
        prompt =  cls.var_cl_global.update_text_file(f'{cls.path}/config/fields_prompt.txt',replacements)  
        
        # Load Gemini        
        model = GenerativeModel(self.gemini)

        max_output_tokens = 8192 # 8192

        try:
            response = model.generate_content(prompt,
                generation_config={
                    "max_output_tokens": max_output_tokens,
                    "temperature": 0.1,
                    "top_p": 1,                    
                },
                stream=False,       
                safety_settings={
                                HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
                                HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                                HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                                HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
                            }                                        
            )
            clear_json = self.var_cl_global.extract_json_from_content(response.text) 
        except:
            print("Generate Gemini Error")
            return None
                         
        if clear_json:
            json_combined = clear_json.copy()
            if clear_json_before:
                self.merge_dicts(json_combined, clear_json_before)        
            json_data = response.text
            return json_combined
        else:
            correct_json = self.var_cl_global.correct_json(response.text)
            clear_json = self.var_cl_global.extract_json_from_content(correct_json)   
            json_data = correct_json
            if response.usage_metadata.candidates_token_count == max_output_tokens:
                # Call Gemini again
                # Get Language json data                
                json_combined = clear_json.copy()
                if clear_json_before:
                    self.merge_dicts(json_combined, clear_json_before)
                columns = json_combined.get(language.strip().upper(), {})
                columns_notin_r = [column for column in column_of_view if not column in columns and not column in columns_notin]
                if columns_notin_r != []:
                    clear_json = self.gemini_run(query,columns_notin_r,column_of_view,language,json_combined)    
                json_combined = clear_json.copy()
                if clear_json_before:
                    self.merge_dicts(json_combined, clear_json_before)                
                return json_combined        

        return None
    
print(f'Running - Start to extract fields and descriptions')
        
cls = GetTablesAndFields()  
cls_trace = TraceUsedTables()
cls_localtabfiel = LocalTablesAndFields()
# Get views from dataset
views_list = cls.get_bq_views()    
# Get data from all views
traced_tables_all = pd.DataFrame()
df_LTabField_all = pd.DataFrame() 

df_output = pd.DataFrame() 

# Get dependencies
view_list_ret = []
for view in views_list:    
    view_list_ret,df_LTabField_all = cls.get_bq_views_dependencies(view, 0,cls.doc_projectId, cls.doc_datasetId,view_list_ret,df_LTabField_all)   
views_df = pd.DataFrame(view_list_ret) 
views_df = views_df.sort_values(by='Position', ascending=False)

for view in views_df['view'].tolist():    
    print(f'Running - Get Trace Data - {view}')
    # Get view metadata   
    view_meta = cls.get_view_metadata(view,cls.doc_projectId,cls.doc_datasetId) 
    # Get traced tables   
    traced_tables = cls_trace.get_view_trace(cls.doc_BQclient,view_meta.full_table_id) 
    traced_tables['View'] = view
    if traced_tables_all.empty:
        traced_tables_all = traced_tables               
    else:
        traced_tables_all = pd.concat([traced_tables_all, traced_tables]) 
    # # Get tables and fields from local catalog
    # print(f'Running - Get tables and fields from local catalog - {view}')
    # df_LTabField = cls_localtabfiel.get_used_tables_and_columns(view,view_meta.view_query)    
    # if df_LTabField_all.empty:
    #     df_LTabField_all = df_LTabField
    # else:
    #     df_LTabField_all = pd.concat([df_LTabField_all, df_LTabField])    

if len(df_LTabField_all) > 0:
    # Save Trace to csv file
    path = f'{cls.path}/output/Trace.csv'
    traced_tables_all.to_csv(f'{path}', index=False)       
    # Get Catalog data
    print(f'Running - Get Catalog Data')
    if not traced_tables_all.empty:
        df_catalog = cls.get_customer_catalog(traced_tables_all['Table'].drop_duplicates().tolist())
        # Save Catalog to csv file
        # path = f'{cls.path}/output/Catalog.csv'
        # df_catalog.to_csv(f'{path}', index=False)    
        # Save Local tables and fieldsto csv file
        # path = f'{cls.path}/output/LocalTablesFields.csv'
        # df_LTabField_all.to_csv(f'{path}', index=False)  
        # Data unification
        df_LTabField_dun = df_LTabField_all.copy()
        df_LTabField_dun['Table'] = df_LTabField_dun['Table'].str.split('.').str[-1].str.upper()
        df_LTabField_dun['SourceField'] = df_LTabField_dun['SourceField'].str.split('.').str[-1].str.upper()
        df_output = pd.merge(df_catalog,df_LTabField_dun, left_on=['Table','Field'], right_on=['Table','SourceField'])
        df_output = df_output[['View','Language','TargetField','FieldDescription']]
        df_output['FieldDescription'] = '* ' + df_output['FieldDescription']

        df_LTabField_nin = df_LTabField_all[~(df_LTabField_all['View'].isin(df_output['View']) & df_LTabField_all['TargetField'].isin(df_output['TargetField']))]
        df_output_view = df_output.copy()        
        # df_LTabField_nin['Table'] = df_LTabField_nin['Table'].str.split('.').str[-1]
        df_LTabField_nin.loc[:, 'Table'] = df_LTabField_nin['Table'].str.split('.').str[-1]
        df_output_view = pd.merge(df_LTabField_nin[['View', 'Table', 'SourceField']], df_output_view[['Language', 'TargetField', 'FieldDescription', 'View']], left_on=['Table', 'SourceField'], right_on=['View', 'TargetField'], how='inner')
        df_output_view = df_output_view[['View_x','Language','TargetField','FieldDescription']]
        df_output_view =  df_output_view.rename(columns={'View_x': 'View'}) 
        df_output = pd.concat([df_output, df_output_view])    

gemini_data = []
columns_notin = []
# Get fields description from Gemini
for view in views_list:
    print(f'Running - Get Fields description from Gemini - {view}')
    # Get view metadata   
    view_meta = cls.get_view_metadata(view,cls.doc_projectId,cls.doc_datasetId) 
    # Get columns from view
    column_names = [field.name for field in view_meta.schema]
    # Get fields that are not in the catalog
    if df_output.empty == False:
        df_fields_notin = df_output[(df_output['View'] == view)]    
        columns_notin = [column for column in column_names if not column in df_fields_notin['TargetField'].tolist()]
    if len(columns_notin) == 0:
        columns_notin = column_names
    query = cls.remove_commented_lines(view_meta.view_query)  
    json_combined = []
    for language in cls.doc_languages:        
        print(f'Running - Get Fields description from Gemini - {view} - {language}')           
        response = cls.gemini_run(query,columns_notin,column_names,language,[])       
        if response != None:  
            if len(json_combined) == 0:
                json_combined = response.copy()
            else:    
                cls.merge_dicts(json_combined, response)     

    if json_combined != []:                
        for language, fields in json_combined.items():
            for field, description in fields.items():
                gemini_data.append([view,language, field, '** ' + description])        
                            
if len(gemini_data) > 0:        
    df_gemini = pd.DataFrame(gemini_data, columns=['View','Language', 'TargetField', 'FieldDescription'])   
    df_output = pd.concat([df_output, df_gemini])  

path = f'{cls.path}/output/cat_field_desc.csv'
df_output.drop_duplicates().to_csv(f'{path}', index=False)  

print(f'End to extract fields and descriptions')               
    
