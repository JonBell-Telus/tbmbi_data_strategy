 
from zipfile import ZipFile
from zipfile import is_zipfile
import io
import os
import sys
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import error_reporting
from google.api_core.exceptions import BadRequest
from google.oauth2 import service_account
os.environ['HTTP_PROXY']='http://198.161.14.26:1328'
os.environ['HTTPS_PROXY']='http://198.161.14.26:1328'

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
commpath = parent + os.path.sep + "Common"
sys.path.append(commpath)

real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)

myDirectory = dir_path.replace('\\','/') #--> replace backslashed to forward for consistency.

#--> Transfer file to cloud storage.
scopes = [
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/drive',
]

keyspath = parent + os.path.sep + "Keys"
keyspath = keyspath.replace('\\','/') 
mySvcAcct = f"{keyspath}/cio-datahub-ws-tmbbi-pr-051c33-deb9e1fe65be.json"
myProject = "cio-datahub-ws-tmbbi-pr-051c33"
myBucket = "cio-datahub-ws-tmbbi-pr-051c33-default"
myWorkspace = "workspace_tmbbi"

credentials = service_account.Credentials.from_service_account_file(
    mySvcAcct , scopes=scopes,
)

client = storage.Client(credentials=credentials,project=myProject)

bucket = client.get_bucket(myBucket)

gsStorageFile = f'zoominfo/testfile.zip'
blob = bucket.blob(gsStorageFile)

print(f"==> Transferring Avro file to Cloud Storage - {myBucket} {gsStorageFile}") 
blob.upload_from_filename(avroFileName)

def zipextract(bucketname, zipfilename_with_path):

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucketname)

    destination_blob_pathname = zipfilename_with_path
    
    blob = bucket.blob(destination_blob_pathname)
    zipbytes = io.BytesIO(blob.download_as_string())

    if is_zipfile(zipbytes):
        with ZipFile(zipbytes, 'r') as myzip:
            for contentfilename in myzip.namelist():
                contentfile = myzip.read(contentfilename)
                blob = bucket.blob(zipfilename_with_path + "/" + contentfilename)
                blob.upload_from_string(contentfile)

zipextract("mybucket", "path/file.zip") # if the file is gs://mybucket/path/file.zip