import pandas as pd
import os
import sys
import math
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
#from jdbc import Jdbc
#from sqlconnect import SqlServer
'''
from sqlserver import SqlServer
from oracle import Oracle
from hive import Hive
from dntl import DNTL
'''
import getopt 
import avro
import avro.datafile
import avro.io
import json
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

import pydata_google_auth
 
import datetime
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import error_reporting
from google.api_core.exceptions import BadRequest
from google.oauth2 import service_account
import tempfile
import warnings
import smtplib
from email.message import EmailMessage
from json import dumps
from httplib2 import Http

os.environ['HTTP_PROXY']='http://198.161.14.26:1328'
os.environ['HTTPS_PROXY']='http://198.161.14.26:1328'
inpDBAppend = ''
inpDBTruncate = ''

if len(sys.argv) < 4:
    print("Missing input arguements")
    exit()

fileName = sys.argv[1]
inpFolder = sys.argv[2]

#--> append or truncate value, optional, means table must be empty or not exist.
if len(sys.argv) > 3:
    if sys.argv[3] == 'append':
        inpDBAppend = True
    elif sys.argv[3] == 'truncate':
        inpDBTruncate = True

reportdate = sys.argv[4]

#--> split table name from suffix.
nameOnly = fileName.split('.')[0]

#--> remove any suffixes to table name.
tableName = nameOnly.split('__')[0]
 
def sendmail(myMessage):
    sender = 'noreply@telus.com'
    #receivers = ['jonathan.bell@telus.com','Prasanta.Bose@telus.com','DEEPAK.GOPALAKRISHNAN@TELUS.COM',  'taju.punnoose@telus.com',  'Matthew.Maclellan@telus.com', 'Boris.Guo@telus.com',  'frincy.clement@telus.com',  'Rizwan.Dawood@telus.com']
    receivers =  ['jonathan.bell@telus.com']
    msg =  EmailMessage()
    msg.set_content( myMessage )
    msg['Subject'] = "GCP Loading"
    msg['From'] = sender
    msg['To'] = receivers
    
    try:
        smtpObj = smtplib.SMTP('bcmsg.tsl.telus.com')
        smtpObj.send_message(msg)
     #   smtpObj.sendmail(sender, receivers, msg)         
        print ("Successfully sent email")
    except smtplib.SMTPException:
        print ("Error: unable to send email")

def sendChat(myMessage):
    os.environ['HTTP_PROXY']='http://198.161.14.26:1328'
    os.environ['HTTPS_PROXY']='http://198.161.14.26:1328'

    Data360_Alerts = 'https://chat.googleapis.com/v1/spaces/AAAA83Gb3Go/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=OvAcSGS3Hfj_8u5wQbXKLdRZ7xWbTO3ZenLvXtZAa7w%3D'
    bot_message = {}

    bot_message["text"] = myMessage
    
    message_headers = {'Content-Type': 'application/json; charset=UTF-8'}

    http_obj = Http()

    response = http_obj.request(
        uri=Data360_Alerts,
        method='POST',
        headers=message_headers,
        body=dumps(bot_message),
    )
#--> Transfer file to cloud storage.
scopes = [
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/drive',
]

keyspath = parent + os.path.sep + "Keys"
mySvcAcct = f"{keyspath}/cio-datahub-ws-tmbbi-pr-051c33-deb9e1fe65be.json"

myProject = "cio-datahub-ws-tmbbi-pr-051c33"
myBucket = "cio-datahub-ws-tmbbi-pr-051c33-default"
myWorkspace = "workspace_tmbbi"
myFolder = inpFolder

gsStorageFile = f'{myFolder}/{fileName}'

credentials = service_account.Credentials.from_service_account_file(
    mySvcAcct , scopes=scopes,
)
 
# Construct a BigQuery client object.
client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

table_id = f"{myProject}.{myWorkspace}.{tableName.lower()}"
 
num_rows_begin = 0

job_config = bigquery.LoadJobConfig(autodetect=True, use_avro_logical_types=True, source_format=bigquery.SourceFormat.AVRO,schema_update_options="ALLOW_FIELD_ADDITION")
 
if inpDBAppend:
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    try:
       num_rows_begin = client.get_table(table_id).num_rows  #--> Get number of rows at start when append.
    except:
      num_rows_begin = 0

elif inpDBTruncate:
    try:
        client.delete_table(table_id)
    except Exception:
        pass
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
#    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
else:
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY

uri = f"gs://{myBucket}/{gsStorageFile}"

load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.

print(f"==> Copying {gsStorageFile} to BigQuery table - {table_id}") 
try:
    load_job.result()  # Waits for the job to complete.
except  BadRequest as e:
    for e in load_job.errors:
        print('ERROR: {}'.format(e['message']))
        sendChat(f"ERROR: Table: {table_id} \n{str(e['message'])}")
    exit() 

num_rows_total = client.get_table(table_id).num_rows #--> get number of rows at end.
 
print("==> Total Rows added {} rows.".format(num_rows_total - num_rows_begin))

myMaxDate = 'none'

if reportdate != 'none' :
    qry = f'select max({reportdate}) max_date from `{table_id}`'
    query_job = client.query(qry)

    try:
        results = query_job.result()  # Waits for job to complete.
        for row in results:
            myMaxDate = row.max_date
    except Exception as e:
            print("whoops something went wrong:" + qry) 

curr_date_time = str(datetime.datetime.utcnow())

if tableName == nameOnly :    #--> temporary, data from datalake is split into many smaller files, do not want to notify for every one.
  #  sendmail(f"Table: {table_id} has been loaded into GCP. \n{reportdate}: {myMaxDate} \nNumber of rows added: {num_rows_total - num_rows_begin}, for a total of {num_rows_total}. \nJob ran at: {curr_date_time} UTC")
    sendChat(f"Table: {table_id} has been loaded into GCP. \n{reportdate}: {myMaxDate} \nNumber of rows added: {num_rows_total - num_rows_begin}, for a total of {num_rows_total}. \nJob ran at: {curr_date_time} UTC")

#print(f"==> Removing Cloud Storage file:  {gsStorageFile}") 
#blob.delete()   #--> Remove file from cloud storage.
'''
print(f"==> Removing Avro data file:  {avroFileName}") 
os.remove(avroFileName)
'''
print(f"==> End of process: {str(datetime.datetime.now())}")