
import os
import jaydebeapi
import pandas as pd
 
import datetime

 #--> Connection values.
mydict = {}

mydict['btwp001612'] = (
    'btwp001612',
    'datalake_sqoop',
    '$qo4L@kop',
    'master'
)

mydict['btwp001611'] = (
    'btwp001611',
    'datalake_sqoop',
    '$qo4L@kop',
    'master'
)

mydict['btwp000777'] = (
    'btwp000777',
    'T884127',
    'T884127',
    'master'
)

mydict['vision'] = (
    'WP40991',
    'LavSAS_APP',
    'app#927sas#8',
    'master'
)

mydict['btwp014239'] = (
    'btwp014239\sql17pr01',
    'domo_etl',
    'DOMO@ETL1',
    'master'
)

mydict['btwp014239-tbmbi'] = (
    'btwp014239\sql17pr01',
    'svc_tbmbi',
    'svc_tbmbi2021',
    'tbmbi'
)

mydict['btwp014240'] = (
    'btwp014240\sql17pr01',
    'domo_etl',
    'DOMO@ETL1',
    'master'
)

mydict['cherry'] = (
    'cherry',
    'datalake_sqoop',
    '$qo4L@kop',
    'master'
)

mydict['panda'] = (
    'panda',
    'idea-voc',
    'idea-voc',
    'master'
)

class SqlServer:
    
    def __init__(self,passServer='',passDB=''):
       
        #--> change windows backslash to unix forward slash.
        myscript = __file__

        real_path = os.path.dirname(os.path.realpath(__file__))
        dir_path = os.path.dirname(real_path)

        DIRDRIVERS = dir_path + os.path.sep + "Drivers" + os.path.sep
        DIRDRIVERS = DIRDRIVERS.replace('\\','/') 
        
        #DIR = dir_path + os.path.sep + "Drivers" + os.path.sep
       
        if passServer:
            serverID = mydict[passServer]

            host_name = serverID[0]
            user_name = serverID[1]
            user_password = serverID[2]
            if passDB:
                database = passDB
            else:
                database = serverID[3]
            
            url = f'jdbc:sqlserver://{host_name};databaseName={database}'

            driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'

            jarFile = [
            DIRDRIVERS + 'hive-jdbc.jar', 
            DIRDRIVERS + 'hadoop-common.jar',
            DIRDRIVERS + 'hive-jdbc-0.14.0.2.2.4.2-2.jar',
            DIRDRIVERS + 'hive-jdbc-0.14.0.2.2.4.2-2-standalone.jar',
            DIRDRIVERS + 'hadoop-common-2.6.0.2.2.4.2-2.jar',
            DIRDRIVERS + 'mssql-jdbc-7.4.1.jre8.jar',
            ]
            
            
            try: 
                self.connection = jaydebeapi.connect(driver, url, [user_name, user_password], jarFile)
                self.connection.jconn.setAutoCommit(False)
                self.cursor = self.connection.cursor()
              
            except Exception as e: 
                
                print(e)
                print(f'***ERROR*** Unable to connect to {host_name} check credentials or network connection. {myscript}')
                exit()

   
    def query(self,sql):

        df = pd.read_sql_query(sql,self.connection)
        
        return df
    
    def insert(self,df,tablename,schema):
         
        df.to_sql(tablename,self.connection, if_exists='append', schema = schema)
        
    def query2(self,sql):
        print(f"==> start query: {str(datetime.datetime.now())}") 
        queryArray = []
        self.cursor.execute(sql)
        field_names = [i[0] for i in self.cursor.description]
         
        for x in self.cursor.fetchall():  
             queryArray.append(x)
        print(f"==> end   query: {str(datetime.datetime.now())}")       
        df = pd.DataFrame(queryArray,columns=field_names)
        print(f"==> end dataframe: {str(datetime.datetime.now())}")    
        return df
    
    def execute(self,sql):
        
        try: 
            self.cursor.execute(sql)
    
        except Exception as e:  
             print(e)
             return(1)
         
        return (0)

    def modify(self,sql,data):
        try:
            self.cursor.execute(sql,data)
            self.connection.commit()
        except Exception as e:  
             print(e)
             return(1)
         
        return (0)
  

    def close(self):
         self.connection.close()
    
    def rtnMydict(self,passServer=''):

        if not passServer:
            return mydict

        key_list = list(mydict.keys())

        if passServer in key_list:
            return mydict[passServer]
        else:
            return False