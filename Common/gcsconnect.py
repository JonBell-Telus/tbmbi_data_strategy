 
 
 
from google.cloud import bigquery
from google.cloud import storage
##from google.cloud import error_reporting
from google.api_core.exceptions import BadRequest
 
 
#--> Transfer file to cloud storage.
class GCSConnect:
    
    scopes = [
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/drive',
    ]
    
    bucket = ''
    storage_client = ''
    bq_client = ''
    myProject = ''
    myBucket = ''
    
    def __init__(self, project='',bucket=''):

        self.myProject = project
        self.myBucket = bucket

        # Instantiates a client
       
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.get_bucket(self.myBucket)
        
    def copy2bucket(self,inpfile,outfile):
        gsStorageFile = outfile
        
        blob = self.bucket.blob(gsStorageFile)

        print(f"==> Transferring Avro file to Cloud Storage - {self.bucket} {gsStorageFile}") 
        blob.upload_from_filename(inpfile)
    def createbucket(self,bucketname):
         # Creates the new bucket
         self.bucket =  self.storage_client.create_bucket(bucketname)
    def getBucketFile(self,inpfile):
         blob = self.bucket.get_blob(inpfile)
         data_string = blob.download_as_text()
         return(data_string)