import sys
import os
import pandas  
os.environ['HTTP_PROXY']='http://198.161.14.26:1328'
os.environ['HTTPS_PROXY']='http://198.161.14.26:1328'
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
commpath = parent + os.path.sep + "Common"
sys.path.append(commpath)

real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)

myDirectory = dir_path.replace('\\','/') #--> replace backslashed to forward for consistency.

#inpTable= sys.argv[1]

mylist = ['bq_c360_campaign_tracking' 
 
 ]
 
 
import yaml

with open("table.yaml",'r') as f:
    myyaml = yaml.safe_load(f)

from bqconnect import BQConnect


mybq = BQConnect("tbmbi-jonb-pr-af7fe4")

for tbl in mylist:
    inpTable = tbl 
    qry2 = f"SELECT column_name as name, data_type as type, 'Nullable' as mode , column_name as description " \
            "FROM `orca.INFORMATION_SCHEMA.COLUMNS` " \
        f"WHERE table_name = '{inpTable}'"
    print(qry2)
    
    df  = mybq.query(qry2)

    json = df.to_json(orient ='records')
    myyaml['resource_name'] = inpTable
    myyaml['description'] = inpTable
    myyaml['schema'] = json
    print(yaml.dump(myyaml,sort_keys=False, default_flow_style=False))

    with open(f'{current}/yamls/{inpTable}.yaml', 'w') as f:
        data = yaml.dump(myyaml, f, sort_keys=False, default_flow_style=False)
 
#mybq.copy2bucket("./gcptest.csv","gcptest2.csv")
#print(mybq.getBucketFile("gcptest2.csv"))
print("finished")