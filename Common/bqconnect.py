 
from google.cloud import bigquery
 
from google.api_core.exceptions import BadRequest

import google.oauth2.credentials
import google.auth
#--> Connect to bigquery and run query.=
class BQConnect:
    bucket = ''
    storage_client = ''
    bq_client = ''
    myProject = ''
    myBucket = ''
    
    def __init__(self, project='' ):
        
        # Instantiates a client
        credentials,  self.myProject = google.auth.default()
        
        #--> Use passed project as default.
        if project:
            self.myProject = project
        
        print(f"Default project set to: {self.myProject}")
        # Instantiates a client
        self.bq_client = bigquery.Client(credentials=credentials, project=self.myProject)
        
    def query(self,qry):
        #--> Run query and return results in a dataframe.
        try:
            df = (self.bq_client.query(qry).result().to_dataframe(create_bqstorage_client=True))   
        except Exception as e:
            print(e)
            exit()
        return df
    
    
    def write(self,df,table_id,disposition):
        
        num_rows_begin = 0

        job_config = bigquery.LoadJobConfig(autodetect=True , source_format=bigquery.SourceFormat.PARQUET)
        
        #--> Clear the destination table(if truncate passed). (if there is more than one input file, this will be changed to append.)
        if disposition == 'append':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        else:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        
        load_job = self.bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)  # Make an API request.  
        
        print(f"==> Copying to BigQuery table - {table_id}") 
        
        try:
            load_job.result()  # Waits for the job to complete.
        except:
            for e in load_job.errors:
                print('ERROR: {}'.format(e['message']))
            exit() 
        
        num_rows_total = self.bq_client.get_table(table_id).num_rows #--> get number of rows at end.

        print(f"Total rows added for {table_id} = {num_rows_total}")