from pyspark import SparkContext, SparkConf 
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

def startSpark(appName=''):
    master = "local[*]"
    
    conf = SparkConf() \
    .setAppName(appName) \
    .setMaster(master) \
    .set("spark.driver.memory", "15g") \
    .set("spark.executor.memory", "45g") \
    .set("spark.executor.extraJavaOptions", "-Xss1024m") \
    .set("spark.driver.extraJavaOptions", "-Xss1024m") \
    .set("spark.logConf", "true") \
    .set("spark.sql.shuffle.partitions","100")  \
    .set("spark.sql.execution.arrow.pyspark.enabled", "true")
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    return spark
