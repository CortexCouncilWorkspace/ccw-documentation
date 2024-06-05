from globalfunctions import *
from vertexai.preview.generative_models import GenerativeModel, HarmCategory, HarmBlockThreshold
import vertexai

class translation:
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
        self.doc_languages = self.catalog_vars['languages'] # Portuguese, English...                

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
            # safety_settings={
            #                 HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
            #                 HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
            #                 HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
            #                 HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
            #             }  
            
        )
               
        clear_json = self.var_cl_global.extract_json_from_content(response.text)   
        json_data = response.text
        if not clear_json:
            correct_json = self.var_cl_global.correct_json(response.text)
            clear_json = self.var_cl_global.extract_json_from_content(correct_json)   
            json_data = correct_json

        return clear_json, response.usage_metadata.candidates_token_count,json_data

print('Running Translation')
cls = translation()
replacements = {'+input_language+': cls.doc_languages}
prompt =  cls.var_cl_global.update_text_file(f'{cls.path}/config/translation.txt',replacements)            
gemini_response, tokens, json_data = cls.gemini_run(prompt) 

cls.var_cl_global.write_json(gemini_response, f'{cls.path}/config/md_translation')
