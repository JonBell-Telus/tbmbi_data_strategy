import sys
import os
import pandas  as pd
import getopt 
import datetime

os.environ['HTTP_PROXY']='http://198.161.14.26:1328'
os.environ['HTTPS_PROXY']='http://198.161.14.26:1328'
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
commpath = parent + os.path.sep + "Common"
sys.path.append(commpath)

real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)

myDirectory = dir_path.replace('\\','/') #--> replace backslashed to forward for consistency.
from bqconnect import BQConnect
def rmvBlankLines(x):
    if len(x.strip()) > 0:
        return True
    else:
        return False
# Permanently changes the pandas settings
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


if len(sys.argv) < 2:
    print ('runbq.py -e <Query> -f <File>' )
    sys.exit(2)

inpFile = ''
inpQuery = ''
     
#--> Only 1 input argument allowed, either a query or a file.
argv = sys.argv[1:]
try:
      opts, args = getopt.getopt(argv,"he:f:",["query=","file=" ])
except getopt.GetoptError:
      print ('runbq.py -e <Query> -f <File>' )
      sys.exit(2)
for opt, arg in opts:
    if opt == '-h':
        print ('runbq.py -e <Query> -f <File>')
        sys.exit()
    elif opt in ("-e", "--query"):
        inpQuery = arg
    elif opt in ("-f", "--file"):
        inpFile = arg
    else:
        print ('runbq.py -e <Query> -f <File>')
        sys.exit()
        
if inpFile:
    print("Input File=" + inpFile)

    #--> read the entire file into variable
    with open(inpFile, 'r') as f:
        inp = f.read()
        
    #--> split the file into a list.
    sqlstmts = filter(rmvBlankLines, inp.split(';'))
elif inpQuery:
    sqlstmts = [inpQuery]

print(f"==> start script: {str(datetime.datetime.now())}") 

mybq = BQConnect("tbmbi-jonb-pr-af7fe4")

for sqlstmt in sqlstmts:
  #  print(sqlstmt)
   
    df = mybq.query(sqlstmt)
    
    if len(df.index) > 0:
        print(df)
    else:
        print("Statement complete")

print(f"==> end script: {str(datetime.datetime.now())}") 
exit()  


myquery = ('SELECT chatbot_msg_txt name FROM `cio-datahub-enterprise-pr-183a.ent_chat.bq_chatbot_intractn_nlp_session` WHERE chatbot_intractn_part_dt > "2022-02-18" LIMIT 3')
myquery2 = 'SELECT count(*) FROM `orca.bq_c360_ag_rcvbl`'
df = mybq.query(myquery2)
print(df.head())
print("finished")