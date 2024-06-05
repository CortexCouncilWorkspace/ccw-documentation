import os
import pandas as pd
from google.cloud import bigquery

class TraceUsedTables:

    def __init__(self) -> None:
        self.script_directory = os.path.dirname(os.path.abspath(__file__))
   
    def get_view_trace(self, BQClient, view_name): 
        view_name = view_name.replace(':','.')
        query = f'select * from {view_name} limit 10'
        # Config job
        job_config = bigquery.QueryJobConfig()
        job_config.dry_run = False  
        job_config.use_query_cache = False           
        # API request - start dry-run query
        query_job = BQClient.query(query, job_config=job_config)     
        # Wait for query to complete
        query_job.result() 
        # Get the query plan.
        self.query_plan = query_job.query_plan
        return self.get_used_tables()

    def get_used_tables(self):
        # O Execution Graph é uma lista de etapas, onde cada etapa é um dicionário
        tables = []
        logs = []
        for stage in self.query_plan:
            logs.append({f"Stage: {stage.name}"})    
            for step in stage.steps:
                if step.kind == 'READ':
                    logs.append({f"Step: {step.kind} - {step.substeps}"})
                    substring = 'FROM'
                    # Using list comprehension with enumerate to find positions
                    positions = [index for index, value in enumerate(step.substeps) if substring in value and '__' not in value[value.find(substring) + len(substring):]]            
                    if positions:
                        position = positions[0]                
                        tables.append({'Table': step.substeps[position]})
        # logs_df = pd.DataFrame(logs)
        tables_df = pd.DataFrame(tables)
        tables_df = tables_df.replace(' ','', regex=True).replace('FROM','', regex=True).drop_duplicates()    
        # logs_df.to_csv(r'C:\Git\cortex-ccw-documentation\FieldsDescriptions\Test\output.csv', index=False)                
        # print(tables_df)         
        return tables_df   