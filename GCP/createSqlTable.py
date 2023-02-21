import pandas as pd
import json
import avro
import avro.datafile
import avro.io
def translateGCPType(dataType):
     
    if isinstance(dataType, dict):
       
        precision =  dataType['precision']
        scale =  dataType['scale']
        mytype = dataType['logicalType']
        return f"{mytype}({precision},{scale})"

    if dataType in ('string'):
        return 'varchar(999)' 

    if dataType in ('INTEGER') :
         return 'numeric' 

    if dataType in ('long') :
         return 'bigint' 

    if dataType in ['FLOAT'] :
        return  'decimal(20,5)' 
     
    if dataType in ( 'date'):
        return 'datetime'

    return 'varchar(999)'

#--> require location of avro file, and eventual table name.

#--> pull avro file in.
reader = avro.datafile.DataFileReader(open('c:/Users/t949662/downloads/download_E360_MATCHED_ENTITIES.avro000000000000',"rb"),avro.io.DatumReader())

#--> convert the schema to a dictionary item
mydict = json.loads(reader.meta.get('avro.schema').decode('utf-8'))

#--> Loop thru the 'fields', which contain all the column names and types.
createstmt = ''
for cols in mydict['fields']:
    if len(createstmt) > 0:
            createstmt = createstmt + ","
    
    createstmt = createstmt +  (f" {cols['name']} {translateGCPType(cols['type'][1])}")
                
print( f"create table xxxx ( {createstmt} ) ")              