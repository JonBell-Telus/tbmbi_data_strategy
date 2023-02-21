import pandas as pd
import os
import sys
import math
import datetime

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
commpath = parent + os.path.sep + "Common"
sys.path.append(commpath)
driverpath = parent + os.path.sep + "Drivers"

from sqlserver_spark  import SqlServer

import json
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf 
from pyspark.sql.functions import lit,col,udf
from pyspark.sql.types  import  ByteType,StringType,BinaryType , DoubleType,StructType, StructField,DecimalType,DateType,TimestampType,IntegerType
 
def startSpark():
    appName = "Datarobot"
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
    .set ("spark.sql.shuffle.partitions","100")  
    
    spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
    
    return spark


if len(sys.argv) < 2:
    print("==> Error - Missing input arguments: <Input Path > <Output Table Name>")
    exit()

print(f"==> Start  script: {str(datetime.datetime.now())}") 

inpPath = sys.argv[1]
outTable = sys.argv[2]   ##outTable = "firmographics.dbo.jontest"

myspark = startSpark()
conn = SqlServer('btwp014239',myspark)
print(inpPath)
from pathlib import Path

#--> Get all parquet files in the input directory.
for p in Path( inpPath ).rglob( '*.parquet' ):
    print( p )
    data= myspark.read.parquet(f"{p}")

    conn.write_append(data,outTable)

print(f"==> End script: {str(datetime.datetime.now())}") 

exit()
with open('C:/Users/t949662/Downloads/download_jontest.parquet') as f:
    data = json.load(f)
print(data)
 