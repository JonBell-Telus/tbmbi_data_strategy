import pandas as pd
import json
import sys
import os
from datetime import datetime,timedelta

'''
if len(sys.argv) < 2:
    print("Missing filename")
    exit()
 
#--> Name of input file.
inpFile = sys.argv[1]
'''

inpFile = 'jan2021_small.csv'
df = pd.read_csv(inpFile, sep=',' ,nrows=2)
df.columns = df.columns.str.strip() #--> remove spaces at beginning and end of column names.
mylist = []
for col in df.columns:
    mydict = {}
    mydict['description'] = 'None'
    mydict['name'] = col
    mydict['type'] = 'string'
    '''
    if df[col].dtype == 'float64':
        mydict['type'] = 'float'
    elif df[col].dtype == 'int64':
        mydict['type'] = 'string'
    else:
        mydict['type'] = 'string'
    '''
    mydict['mode'] = 'nullable'
    
   
    mylist.append(mydict)
jsonString = json.dumps(mylist)
print(jsonString)
exit()

mylist = df.values.tolist() #--> Convert dataframe to list
 
#--> add 2nd set of values for merge statement. 
for idx,item in enumerate(mylist):
    mylist[idx].extend(item)
     