from pyasn1.type.univ import Null
import pydata_google_auth
import pandas as pd
import datetime
import sys
from google.cloud import bigquery
from google.cloud import storage
import os
from google.oauth2 import service_account
from dateutil.relativedelta import relativedelta

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
commpath = parent + os.path.sep + "Common"
sys.path.append(commpath)

from sqlserver import SqlServer
from oracle import Oracle
from hive import Hive
from dntl import DNTL
from httplib2 import Http
from json import dumps

inpQuery = ''
inpServer = ''
inpWorkSpace = ''
inpBigQuery = ''
inpDBAppend = False
inpDBTruncate= False
inpDntlCol=''

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

os.environ['HTTP_PROXY']='http://198.161.14.26:1328'
os.environ['HTTPS_PROXY']='http://198.161.14.26:1328'

real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)

myDirectory = dir_path.replace('\\','/') #--> replace backslashed to forward for consistency.

scopes = [
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/drive',
]

keyspath = parent + os.path.sep + "Keys"
mySvcAcct = f"{keyspath}/cio-datahub-ws-tmbbi-pr-051c33-deb9e1fe65be.json"
 
credentials = service_account.Credentials.from_service_account_file(mySvcAcct , scopes=scopes,)

client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

inpFile = sys.argv[1]
errorDir = sys.argv[2]

fileName = f"{myDirectory}/{inpFile}.csv"
df = pd.read_csv(fileName, sep=',' )

prev_server = ''
dbConn = ''
notifyTables = {}

for index, row in df.iterrows():
    
    inpServer = str(row['server']).strip().lower()

    if inpServer != prev_server:
        if dbConn:
            dbConn.close()

        if SqlServer().rtnMydict(inpServer):
            dbConn = SqlServer(inpServer)
        elif Oracle().rtnMydict(inpServer):
            dbConn = Oracle(inpServer)
        elif inpServer == "datalake":
            dbConn = Hive()

    inpTable = str(row['tablename']).strip().lower()
    inpGCPTable = str(row['gcptable']).strip().lower()
    inpCol =  str(row['datecolumn']).strip().lower()
    inpAction = str(row['action']).strip().lower()
    inpQuery = str(row['query']).strip()

    #--> split table name from schema.
    tableName = inpTable.split('.')[-1]

    
    qry = f'select max({inpCol.lower()}) nddate from `cio-datahub-ws-tmbbi-pr-051c33.workspace_tmbbi.{inpGCPTable}`'

    query_job = client.query(qry)
    
    try:
        results = query_job.result()  # Waits for job to complete.
        
        for row in results:
            myMaxDate = row.nddate 
            onlydate = str(myMaxDate).split('+')[0]
          
    except:
            myMaxDate = '1970-01-01 00:00:00'  #--> default to the epoch if table doesn't exist.
            onlydate = myMaxDate

    try:      
        if isinstance(dbConn, Oracle):
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
        elif isinstance(dbConn, Hive):
            qry = f"select count(*) count from {inpTable} where {inpCol} >  '{onlydate}' "  

        df = dbConn.query(qry) #--> Retrieve count of  rows from the input table.
        
        df.columns= df.columns.str.lower()
        if df.loc[0,"count"] > 0:
            notifyTables[inpGCPTable] = onlydate
            if inpServer != "datalake":
                inpQuery = inpQuery.replace('$1',inpTable) 
                inpQuery = inpQuery.replace('$2',inpCol)
                inpQuery = inpQuery.replace('$3',onlydate) 
                print(f"{inpQuery};{inpServer};{inpAction};{inpCol};{inpGCPTable};")
            else:
                print(f"{inpGCPTable} {onlydate}  ")
         ##       command = f"plink -ssh  svc_dlk_r1_pr@qcr-hadoop-e001.oss.ads -pw D01ph1nPwd -batch /home/svc_dlk_r1_pr/gcp/runRemote.sh incrementdl.sh {inpGCPTable}__dntl '{onlydate}'"
                command = f"ssh  svc_dlk_r1_pr@qcr-hadoop-e001.oss.ads  /home/svc_dlk_r1_pr/gcp/runRemote.sh \"incrementdl.sh {inpGCPTable}__dntl '{onlydate}'\""
                rtn = os.system(command)
                print(rtn)
      
    except Exception as e:
            message = f"***ERROR*** GCP Loading: {qry} --> {e}"
            with open(errorDir + '/gcperrors.txt','a', encoding="utf-8") as f:
                print(message,file=f) 
            sendChat(message)
    

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