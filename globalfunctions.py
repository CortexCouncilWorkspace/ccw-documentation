import json
import os
from google.oauth2 import service_account

class globalfunctions:
    def __init__(self):
        self.path = os.path.dirname(os.path.abspath(__file__))           

    def get_all_variables(self,variables_path):

        # Read the JSON file
        with open(os.path.join(self.path, f'{variables_path}')) as f:
            self.config_data = json.load(f)   
                    
        return self.config_data

    def get_variable(self,variables_path,variable):

        # Read the JSON file
        with open(os.path.join(self.path, f'{variables_path}')) as f:
            self.config_data = json.load(f)   
            
        return self.config_data[f'{variable}']

    def authentication(self,auth_json, scope_list):        
        
        # Define the scope
        # scope = self.get_variable('variables/global_variables.json','auth_scope')
        # scopes_list = [scope.strip().strip("'") for scope in scope.split(',')]
        # auth_json = self.get_variable('variables/global_variables.json','auth_json')

        # Define the credentials        
        credentials = service_account.Credentials.from_service_account_file(f'{self.path}/{auth_json}')
        return credentials.with_scopes(scope_list)    

    def create_folder(self,folder_name):
        try:        

            current_path = os.path.dirname(os.path.abspath(__file__))    

            # Create a new folder path within the current directory
            new_folder_path = os.path.join(current_path, folder_name)

            # Check if the folder already exists
            if os.path.exists(new_folder_path):
                # print(f"Folder '{folder_name}' already exists at '{new_folder_path}'")
                return new_folder_path
            else:
                # Create the new folder
                os.makedirs(new_folder_path)
                # print(f"Folder '{folder_name}' created successfully at '{new_folder_path}'")
                return new_folder_path
        except OSError as e:
            print(f"Error creating folder '{folder_name}': {e}")
            return None    

    def readtxtfile(self,file_path):

        try:
            # Open the file in read mode
            with open(file_path, 'r') as file:
                # Read the entire contents of the file into a string variable
                return file.read()
                # print("File contents read successfully.")
                # Print or use the file contents as needed
                # print(file_contents)
        except FileNotFoundError:
            print(f"Error: File '{file_path}' not found.")
        except IOError as e:
            print(f"Error reading file: {e}")                        
        return ''

    def extract_json_from_content(self,content):
        
        try:            
            # Find the start and end positions of JSON content
            start_index = content.find('{')
            end_index = content.rfind('}') + 1
            # Extract JSON part
            json_part = content[start_index:end_index]
            # Parse JSON
            json_data = json.loads(json_part)
            return json_data
        except json.JSONDecodeError:
            print("Correcting Json Code")  

    def correct_json(self,content):
        # Find the index of the first " { " and the last " , "
        start_index = content.find('{') + 1  # Adding 1 to skip the '{' itself
        end_index = content.rfind(',')

        # Extract the substring
        correct_str = content[start_index:end_index].strip()
        clear_json = self.extract_json_from_content('{' + correct_str + '}')   
        if clear_json:
            return '{' + correct_str + '}'                    
        else:
            return '{' + correct_str + '} }'      

    def write_json(self,jsondata,file):
        try:
            with open(f'{file}.json', 'w', encoding='utf-8') as f:
                json.dump(jsondata, f, indent=2)            
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}")     

    def update_text_file(self,file_path, replacements):
        # Read the content of the file
        with open(file_path, 'r') as file:
            file_content = file.read()

        # Perform replacements for each variable
        for variable, value in replacements.items():
            placeholder = f"{variable}"
            file_content = file_content.replace(placeholder, str(value))

        return file_content  
                                       
    def count_substrings_between_commas(self,input_string):
        # Split the string into substrings using comma as the separator
        substrings = input_string.split(',')
        
        # The number of substrings will be one less than the number of commas
        num_substrings = len(substrings) - 1
        
        return num_substrings    