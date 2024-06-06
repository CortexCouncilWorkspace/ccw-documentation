from globalfunctions import *
from google.cloud import bigquery
from vertexai.preview.generative_models import GenerativeModel, HarmCategory, HarmBlockThreshold
import vertexai
import re
import pandas as pd
from FieldsDescriptions.GetTablesAndFields import *
from translation import *

class createdocumentation:
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
        self.doc_BQclient = bigquery.Client.from_service_account_json(f'{self.path}/{self.doc_authJson}')                   

    def get_docs_sql_file(self):

        # Fixed SQL Source for Cortex Git
        # Clone https://github.com/GoogleCloudPlatform/cortex-reporting.git to sql_source
        # git clone https://github.com/GoogleCloudPlatform/cortex-reporting.git

        self.sql_files_dir = f'{self.path}/sql_source/cortex-reporting/{self.sap_environment}'
        # self.sql_files_dir = 'C:\Git'

        sql_files = [f for f in os.listdir(self.sql_files_dir) if f.endswith('.sql')]
        return sql_files

    def get_BQ_list_objects(self):

        # Get a reference to the dataset
        dataset_ref = self.doc_BQclient.dataset(self.doc_datasetId, project=self.doc_projectId )

        # List tables in the dataset
        return list(self.doc_BQclient.list_tables(dataset_ref))

    # Get the view metadata
    def get_view_metadata(self,view_id):
        view_ref = self.doc_BQclient.dataset(self.doc_datasetId, project=self.doc_projectId).table(view_id)
        view = self.doc_BQclient.get_table(view_ref)
        return view.view_query

    def gemini_run(self,prompt):

        # Load Gemini
        model = GenerativeModel(self.gemini) #gemini-pro / gemini-1.5-pro-preview-0409 / gemini-1.5-pro-001

        response = model.generate_content(prompt,
            generation_config={
                "max_output_tokens": 8192,
                "temperature": 0.1,
                "top_p": 1,
            },
            stream=False,
            safety_settings={
                            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
                            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_ONLY_HIGH,
                            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
                            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
                        }

        )

        clear_json = self.var_cl_global.extract_json_from_content(response.text)
        json_data = response.text
        if not clear_json:
            correct_json = self.var_cl_global.correct_json(response.text)
            clear_json = self.var_cl_global.extract_json_from_content(correct_json)
            json_data = correct_json

        return clear_json, response.usage_metadata.candidates_token_count,json_data

    def remove_commented_lines(self,query):
        lines = query.split('\n')
        clean_lines = [line for line in lines if not re.match(r'^\s*--', line)]
        return '\n'.join(clean_lines)

    def create_documentation(self,json_file, language, view_id, md_path, df_fields,df_traced,is_cortex_frame):

        try:

            # url_source_sql = (f"https://github.com/GoogleCloudPlatform/cortex-reporting/blob/b05a3345d55d5e196d8d636daf0c81541d0604e7/{self.sap_environment.lower()}/{view_id}.sql")
            # Read the JSON file
            with open(f'{self.path}/config/md_translation.json', encoding='utf-8') as f:
                md_trans_json_file = json.load(f)

            # Create a Markdown file
            working_md_file = self.var_cl_global.create_folder(md_path)
            working_md_file = os.path.join(working_md_file, f'{view_id + ".md"}')
            with open(working_md_file, 'w', encoding='utf-8') as md_file:
                # Enable Comments System
                # md_file.write('---\n')
                # md_file.write('comments: true\n')
                # md_file.write('---\n')

                # Write the header
                md_file.write(f'# {view_id}\n\n')

                if is_cortex_frame == 'X':
                    #Write the Original Url
                    md_file.write(f'## {md_trans_json_file[""+language+""]["ORIGINAL_LINK"]}\n\n')
                    md_file.write(f'Google Cloud Cortex Foudantion - Cortex Reporting')
                    # md_file.write(f'Google Cloud Cortex Foudantion - Cortex Reporting - ({self.sap_environment.upper()}) - [{view_id}]({url_source_sql})\n\n')

                # Write the overview
                md_file.write(f'## {md_trans_json_file[""+language+""]["OVERVIEW"]} \n\n')
                md_file.write(json_file['OVERVIEW'] + '\n\n')

                # Write the data sources
                md_file.write(f'## {md_trans_json_file[""+language+""]["DATA_SOURCES"]["Section_Description"]}\n\n')
                md_file.write(f'| {md_trans_json_file[""+language+""]["DATA_SOURCES"]["Source"]} | {md_trans_json_file[""+language+""]["DATA_SOURCES"]["DS_Description"]} |\n')
                md_file.write('|---|---|')
                for source, description in json_file["DATA_SOURCES"].items():
                    md_file.write('\n')
                    md_file.write('| ' + str(source) + ' | ' + str(description) + ' |')

                # Write required tables
                md_file.write(f'\n\n## {md_trans_json_file[""+language+""]["REQUIRED_TABLES"]["Source"]}\n\n')    
                for index,row in df_traced.iterrows():
                    md_file.write(f'\n - {row["Table"]}')
                
                # Write the key use cases
                md_file.write(f'\n\n## {md_trans_json_file[""+language+""]["KEY_USE"]}\n\n')
                for use_case in json_file['KEY_USE']:
                    md_file.write(f'- {use_case}\n')

                # Write the query logic
                md_file.write(f'\n\n## {md_trans_json_file[""+language+""]["QUERY_LOGIC"]}\n\n')
                md_file.write(json_file['QUERY_LOGIC'] + '\n\n')

                # Write the SAP transactions
                md_file.write(f'## {md_trans_json_file[""+language+""]["SAP_TRANSACTIONS"]["Section_Description"]}\n\n')
                md_file.write(f'| {md_trans_json_file[""+language+""]["SAP_TRANSACTIONS"]["Transaction"]} | {md_trans_json_file[""+language+""]["SAP_TRANSACTIONS"]["Tx_Description"]} |\n')
                md_file.write('|---|---|')
                for transaction, description in json_file["SAP_TRANSACTIONS"].items():
                    md_file.write('\n')
                    md_file.write('| ' + str(transaction) + ' | ' + str(description) + ' |')

                var = f'## {md_trans_json_file[""+language+""]["SAP_MODULE"]} \n\n'
                # Write core module "SAP_MODULE": "Financial Accounting (FI)"
                md_file.write(f'\n\n## {md_trans_json_file[""+language+""]["SAP_MODULE"]} \n\n')
                md_file.write(json_file['SAP_MODULE'] + '\n\n')

                #Write Data Lineage
                # md_file.write(f'\n\n## {md_trans_json_file[""+language+""]["LINEAGE"]["Section_Description"]}\n\n')
                # md_file.write(f'{md_trans_json_file[""+language+""]["LINEAGE"]["Description"]}\n\n')

                #Write View Fields

                md_file.write(f'## {md_trans_json_file[""+language+""]["SOURCE_FIELDS"]["Section_Description"]}\n\n')
                md_file.write(f'| {md_trans_json_file[""+language+""]["SOURCE_FIELDS"]["Field"]} | {md_trans_json_file[""+language+""]["SOURCE_FIELDS"]["Description"]} |\n')
                md_file.write('|---|---|')
                for index, row in df_fields.iterrows():
                    md_file.write('\n')
                    md_file.write('| ' + str(row['TargetField']) + ' | ' + str(row['FieldDescription']) + ' | ')
        except:
            print(f"Run again: {view_id}")

cl_docs = createdocumentation()
cl_translation = translation()
cl_fields = GetTablesAndFields()
objects = cl_docs.get_BQ_list_objects()
objects = [view for view in objects if view.table_type == 'VIEW']
if len(objects) > 0 and len(cl_docs.doc_views) > 0:
    objects = [view for view in objects if view.table_id in cl_docs.doc_views]

# Get fields descriptions
df_fields_all = pd.read_csv(f'{cl_docs.path}/FieldsDescriptions/output/cat_field_desc.csv',sep=',')
# Get traced fields
df_traced = pd.read_csv(f'{cl_docs.path}/FieldsDescriptions/output/Trace.csv',sep=',')

for obj in objects:
    print(f'{obj.table_id} - Running Gemini')
    sql_code = cl_docs.get_view_metadata(obj.table_id)
    sql_code = cl_docs.remove_commented_lines(sql_code)
    # Run Gemini to get SAP documentation
    replacements = {'+input_sql+': sql_code, '+input_language+': cl_docs.doc_languages}
    prompt =  cl_docs.var_cl_global.update_text_file(f'{cl_docs.path}/config/documentation_prompt.txt',replacements)
    gemini_response, tokens, json_data = cl_docs.gemini_run(prompt)
    # Loop through each value in the list
    for lang in cl_docs.doc_languages:
        lang = lang.strip()
        print(f'{obj.table_id} - Create {lang} documentation')
        # Get fields descriptions
        df_fields = df_fields_all[(df_fields_all['View'] == obj.table_id) & (df_fields_all['Language'].str.upper() == lang.upper())]
        # Create Local folders
        folder_path = cl_docs.var_cl_global.create_folder(f'documentation/json/{cl_docs.doc_datasetId.strip().lower()}/{lang.strip().lower()}')
        # Get Language json data
        if gemini_response is None:
            continue
        lang_data = gemini_response.get(lang, {})
        if not lang_data:
            lang_data = gemini_response.get(lang.strip().upper(), {})
        # Write json file
        cl_docs.var_cl_global.write_json(lang_data,f'{folder_path}/{obj.table_id}')
        # Write md file
        md_path = f'documentation/md/{cl_docs.doc_datasetId.strip().lower()}/{lang.strip().lower()}'
        cl_docs.create_documentation(lang_data, lang, obj.table_id, md_path,df_fields,df_traced[df_traced['View'] == obj.table_id],'')
