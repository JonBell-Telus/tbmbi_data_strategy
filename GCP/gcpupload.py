from pyasn1.type.univ import Null
import pydata_google_auth
import pandas as pd
import datetime
import sys
import os
import math
import glob

from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
from google.cloud import error_reporting
import pydata_google_auth
from dateutil.relativedelta import relativedelta

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
commpath = parent + os.path.sep + "Common"
sys.path.append(commpath)
driverpath = parent + os.path.sep + "Drivers"

from sqlserver_spark  import SqlServer
from oracle_spark import Oracle
from hive import Hive
from dntl import DNTL
from httplib2 import Http
from json import dumps
import yaml
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf 

inpQuery = ''
inpServer = ''
inpWorkSpace = ''
inpBigQuery = ''
inpDBAppend = False
inpDBTruncate= False
inpDntlCol=''

os.environ['HTTP_PROXY']='http://198.161.14.26:1328'
os.environ['HTTPS_PROXY']='http://198.161.14.26:1328'

real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)

myDirectory = dir_path.replace('\\','/') #--> replace backslashed to forward for consistency.

now = datetime.datetime.now()

myFileSuffix = now.strftime("%Y%m%d%H%M%S")
 
logFileName = f"gcpupload_{myFileSuffix}"

def sendChat(myMessage):
     
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
    
def startSpark():
    appName = "GCP"
    master = "local[*]"
    
    conf = SparkConf() \
    .setAppName(appName) \
    .setMaster(master) \
    .set("spark.driver.extraClassPath",f"{driverpath}/mssql-jdbc-7.4.1.jre8.jar{os.pathsep}{driverpath}/ojdbc6.jar") \
    .set("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.1.2") \
    .set("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "LEGACY") \
    .set("spark.driver.memory", "15g") \
    .set("spark.executor.memory", "45g") \
    .set("spark.logConf", "true") \
    .set("parquet.enable.summary-metadata", "false") \
    .set("spark.sql.parquet.mergeSchema", "false") \
    .set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED") \
    .set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED") \
    .set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
    .set ("spark.sql.shuffle.partitions","100")  
    
    spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
    
    return spark

def writeLog(message):
     
    curr_date_time = str(datetime.datetime.utcnow())
    with open(f"{errorDir}/{logFileName}.log",'a', encoding="utf-8") as f:
                print(f"{curr_date_time} - {message}" ,file=f)

def getDBData(inpQuery,inpServer,inpAction,inpCol,inpGCPTable,inpSplitby,dbConn):

    writeLog(f"{inpTable} Getting data from source")
    datadf = dbConn.query(f"({inpQuery}) query")
    
    writeLog(f"{inpTable} Writing parquet data to output directory")
    if inpSplitby == 'none': 
        datadf.write.format("parquet").mode('overwrite').save(f"{myDirectory}/uploadfiles/{inpGCPTable.lower()}")
    else:
        datadf.write.format("parquet").partitionBy(inpSplitby).mode('overwrite').save(f"{myDirectory}/uploadfiles/{inpGCPTable.lower()}")

def upload2GCP(tableName,disposition):

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

    '''
    credentialsx = pydata_google_auth.get_user_credentials(
        scopes=scopes,
        auth_local_webserver=False
    )
    ''' 
    client = storage.Client(credentials=credentials,project=myProject)

    bucket = client.get_bucket(myBucket)
     
    #--> remove directory in gcs.
    blobs = bucket.list_blobs(prefix=f'uploadfiles/{tableName}/')
    for blob in blobs:
        blob.delete()
    
    writeLog(f"{inpTable} uploading parquet files to cloud storage")
    #--> For each file found in directory upload to gcs bucket. 
    for localf in glob.glob(f"{myDirectory}/uploadfiles/{tableName}/**/*.parquet", recursive=True) :
        myfile =  os.path.basename(localf)
   #     print(myfile)
        blob = bucket.blob(f'uploadfiles/{tableName}/{myfile}')
        blob.upload_from_filename(localf)
    
    # Construct a BigQuery client object.
    client = bigquery.Client(credentials=credentials, project=myProject)
        
    table_id = f"{myProject}.{myWorkspace}.{tableName}"
    
    try:
       num_rows_begin = client.get_table(table_id).num_rows #--> get number of rows at begin.
    except:
       num_rows_begin = 0

    job_config = bigquery.LoadJobConfig(autodetect=True, use_avro_logical_types=True, source_format=bigquery.SourceFormat.PARQUET)
    
    #--> Clear the destination table(if truncate passed). (if there is more than one input file, this will be changed to append.)
    if disposition == 'append':
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    elif disposition == 'truncate':
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    else:
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
        
    blobs = bucket.list_blobs(prefix=f'uploadfiles/{tableName}/')
    for blob in blobs:
        uri =  f"gs://{myBucket}/{blob.name}"

        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)  # Make an API request.
        writeLog(f"{inpTable} Copying {uri} to BigQuery table - {table_id}") 
        try:
            load_job.result()  # Waits for the job to complete.
        except Exception as e:
            for x in load_job.errors:
                print('ERROR: {}'.format(x['message']))
            exit() 
        #--> Change the write disposition to append if there are multiple files to add into table. 
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        
    num_rows_total = client.get_table(table_id).num_rows #--> get number of rows at end.

    num_rows_added = num_rows_total - num_rows_begin
    writeLog(f"{inpTable} Total rows added for {table_id} = {num_rows_added} for a total table count of {num_rows_total}")

scopes = [
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/drive',
]

keyspath = parent + os.path.sep + "Keys"
mySvcAcct = f"{keyspath}/cio-datahub-ws-tmbbi-pr-051c33-deb9e1fe65be.json"
 
credentials = service_account.Credentials.from_service_account_file(mySvcAcct , scopes=scopes,)

client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

 
if len(sys.argv) < 2:
    print("missing input arguments, Input yaml file, and log directory")
    exit()
    
inpFile = sys.argv[1]
if len(sys.argv) < 3:
    errorDir = myDirectory
else:
    errorDir = sys.argv[2]

fileName = f"{myDirectory}/{inpFile}.yml"
with open(fileName, 'r') as file:
    tbls = yaml.safe_load(file)
 
prev_server = ''
dbConn = ''
notifyTables = {}

myspark = startSpark()

for row in tbls:   
 
    inpServer = str(row['server']).strip().lower()

    if inpServer != prev_server:
       
        if SqlServer().rtnMydict(inpServer):
            dbConn = SqlServer(inpServer,myspark)
        elif Oracle().rtnMydict(inpServer):
            dbConn = Oracle(inpServer,myspark)
        elif inpServer == "datalake":
            dbConn = Hive()
        else:
            message = f"{inpTable} does not have a valid server - {inpServer}"
            with open(errorDir + '/gcpupload.txt','a', encoding="utf-8") as f:
                print(message,file=f) 

    inpTable = str(row['tablename']).strip().lower()
    inpGCPTable = str(row['gcptable']).strip().lower()
    inpCol =  str(row['datecolumn']).strip().lower()
    inpAction = str(row['action']).strip().lower()
    inpQuery = str(row['query']).strip()
    inpDNTL = str(row['dntl']).strip()
    inpSplitby = str(row['splitby']).strip().lower()


    #--> split table name from schema.
    tableName = inpTable.split('.')[-1]

    writeLog(f"{inpTable} Starting")

    if inpCol == 'none':   #--> if no date available, then no incremental.
        writeLog(f"{inpTable} No date comparison, copying full table")
        notifyTables[inpGCPTable] = '1970-01-01 00:00:00'
        inpQuery = inpQuery.replace('$1',inpTable) 
        print(inpQuery)
        #inpQuery = inpQuery.replace('$4',inpDNTL) 
        getDBData(inpQuery,inpServer,inpAction,inpCol,inpGCPTable,inpSplitby,dbConn)
        upload2GCP(inpGCPTable,inpAction)
        continue

    qry = f'select max({inpCol.lower()}) nddate from `cio-datahub-ws-tmbbi-pr-051c33.workspace_tmbbi.{inpGCPTable}`'

    query_job = client.query(qry)
    
    try:
        results = query_job.result()  # Waits for job to complete.
        
        for row in results:
            myMaxDate = row.nddate 
            if myMaxDate is None: # test if no row returned.
                myMaxDate = '1970-01-01 00:00:00' 
            onlydate = str(myMaxDate).split('+')[0]
          
    except:
            myMaxDate = '1970-01-01 00:00:00'  #--> default to the epoch if table doesn't exist.
            #myMaxDate = '2019-01-01 00:00:00'
            onlydate = myMaxDate

    writeLog(f"{inpTable} GCP table:{inpGCPTable}  {inpCol} Max date = {onlydate}")

    if isinstance(dbConn, Oracle):
         onlydate = onlydate[:19] # ensure date is no more than 19 bytes.
         qry = f"select /*+ parallel */ count(*) count from {inpTable} where {inpCol} > to_date('{onlydate}','yyyy-mm-dd hh24:mi:ss')" 
         
    elif isinstance(dbConn, SqlServer):
            
    #--> For datetime2 types, which return epoch(integer), convert to actual date, except for year/month columns.
        if isinstance((myMaxDate), int) and (myMaxDate//1000 > 100000):
           try:
              onlydate = str(datetime.datetime.fromtimestamp(myMaxDate//1000)) #--> remove milli seconds.
           except:
               pass
        else:
            #--> need to strip off milliseconds to 3 digits (if any)
            _temp1 = str(onlydate).split('.') #--> should be 2 in list.
            if len(_temp1) > 1:  #--> make sure there is a microsecond.
               onlydate = f"{_temp1[0]}.{_temp1[1][:3]}" #--> reconstruct with just 3 digits. 
            
            qry = f"select count(*) count from {inpTable} where {inpCol} >  '{onlydate}' "  

    try: 
        df = dbConn.query(f"({qry}) query") #--> Retrieve count of  rows from the input table.
    except Exception as e:
            message = f"***ERROR*** GCP Loading: {qry} --> {e}"
            with open(errorDir + '/gcperrors.txt','a', encoding="utf-8") as f:
                print(message,file=f)
         #   sendChat(message)

    numrows = df.head()[0]
    writeLog(f"{inpTable} Rows to load = {str(numrows).split('.')[0]}")
          
    if df.head()[0] > 0:
         notifyTables[inpGCPTable] = onlydate
         ## inpQuery = ssDntlQuery
         inpQuery = inpQuery.replace('$1',inpTable) 
         inpQuery = inpQuery.replace('$2',inpCol)
         inpQuery = inpQuery.replace('$3',onlydate) 
        #inpQuery = inpQuery.replace('$4',inpDNTL) 
         print(f"{inpQuery};{inpServer};{inpAction};{inpCol};{inpGCPTable};")
         getDBData(inpQuery,inpServer,inpAction,inpCol,inpGCPTable,inpSplitby,dbConn)
         upload2GCP(inpGCPTable,inpAction)
    else:
         writeLog(f"{inpTable} does not have any new rows to upload.")
    
exit()
curr_date_time = str(datetime.datetime.utcnow())
message = ''
if notifyTables:
    message = "The following GCP tables have been scheduled for synchronization:\n"
    for key,value in notifyTables.items():
        message = message + f"{key} : Last date: {value}\n"
    message =  message + f"Tested at {curr_date_time} UTC."
else:
    message = f"GCP tables are up to date.\nTested at {curr_date_time} UTC."
sendChat(message)
