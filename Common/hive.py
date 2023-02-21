
import os
import jaydebeapi
import pandas as pd
#--test
class Hive:
    base_sql = ["set hive.mapred.mode=nonstrict"]
    myscript = __file__
    def __init__(self):
       
        real_path = os.path.dirname(os.path.realpath(__file__))
        dir_path = os.path.dirname(real_path)
        
        #--> change windows backslash to unix forward slash.
        DIRDRIVERS = dir_path + os.path.sep + "Drivers" + os.path.sep
        DIRDRIVERS = DIRDRIVERS.replace('\\','/') 
        DIRKEYS = dir_path + os.path.sep + "Keys" + os.path.sep
        DIRKEYS = DIRKEYS.replace('\\','/')
         
        url = 'jdbc:hive2://datalake-gw.oss.ads:8443/default;ssl=true;sslTrustStore=' + DIRKEYS + 'client_truststore.jks;trustStorePassword=8df2d75fa62b6e9d31d231;transportMode=http;httpPath=gateway/default/hive'
        
        dirver = 'org.apache.hive.jdbc.HiveDriver'

        jarFile = [
            DIRDRIVERS + 'hive-jdbc.jar', 
            DIRDRIVERS + 'hadoop-common.jar',
            DIRDRIVERS + 'hive-jdbc-0.14.0.2.2.4.2-2.jar',
            DIRDRIVERS + 'hive-jdbc-0.14.0.2.2.4.2-2-standalone.jar',
            DIRDRIVERS + 'hadoop-common-2.6.0.2.2.4.2-2.jar',
            DIRDRIVERS + 'mssql-jdbc-7.4.1.jre8.jar',
        ]
 
        userid = 'svc_dlk_r1_pr'
        password = 'D01ph1nPwd'
		
        try: 
            self.connection = jaydebeapi.connect(dirver, url, [userid, password], jarFile)

            self.cursor = self.connection.cursor()
        except Exception as e:
            print(e)
            print(f'***ERROR*** Unable to connect to datalake check credentials or network connection.')
            exit()

    def query(self,sql):
        try:
            df = pd.read_sql_query(sql,self.connection)
            return df
        except Exception as e: 
            print(f'***ERROR*** Query error: {sql}')
            print(e)
            exit()

    def execute(self,sql):
        try:
            self.cursor.execute(sql)
            return 0
        except:
            return -1

    def close(self):
         self.connection.close()