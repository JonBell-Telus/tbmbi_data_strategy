class SqlServer:
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

    mydict['btwp014239-tbmbi'] = (
        'btwp014239\sql17pr01',
        'svc_tbmbi',
        'svc_tbmbi2021',
        'tbmbi'
    )

    mydict['btwp014239'] = (
        'btwp014239\sql17pr01',
        'domo_etl',
        'DOMO@ETL1',
        'master'
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
    
    mydict['wp40991'] = (
        'wp40991',
        'LavSAS_APP',
        'app#927sas#8',
        'master'
    )

    mydict['panda'] = (
        'panda',
        'idea-voc',
        'idea-voc',
        'master'
    )

    encoding = 'UTF-8'

    host_name = ''
    user_name = ''
    user_password = ''
    svc_name = ''
    url = ''

    driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'

    spark = ''
    myerror = {}
    def __init__(self, passServer='',session='',passDB=''):
        
        self.spark = session

        if passServer:
            serverID = self.mydict[passServer]

            self.host_name = serverID[0]
            self.user_name = serverID[1]
            self.user_password = serverID[2]
            self.svc_name = serverID[3]
            
            if passDB:
                database = passDB
            else:
                database = serverID[3]
 
            self.url = f'jdbc:sqlserver://{self.host_name};databaseName={database}'
             
    def query(self,sql):
       # self.cursor.execute(sql)
        try:
            df = self.spark.read.format('jdbc').option('url',self.url).option('driver',self.driver).option('dbtable',sql).option('user',self.user_name).option('password',self.user_password).load()
        except Exception as e:
            print(e)
            
        return df
    
    def writeerr(self):
        print(self.myerror)
        
    def write(self,df,sql):
       # self.cursor.execute(sql)
         
        try:
            df.write.format('jdbc').option('batchsize',1000).option('truncate',True).option("numPartitions", 10).option('url',self.url).option('driver',self.driver).mode('overwrite').option('dbtable',sql).option('user',self.user_name).option('password',self.user_password).save()
        except Exception as e:
            print(e)
            print(str(e)[:25])
            self.myerror[str(e)[:25]] = 'missing'
            return -1
        
        return  0
    def write_append(self,df,sql):
           # self.cursor.execute(sql)
         
        try:
            df.write.format('jdbc').option('batchsize',1000).option('truncate',False).option("numPartitions", 10).option('url',self.url).option('driver',self.driver).mode('append').option('dbtable',sql).option('user',self.user_name).option('password',self.user_password).save()
        except Exception as e:
            print(e)
            print(str(e)[:25])
            self.myerror[str(e)[:25]] = 'missing'
            return -1
        
        return  0
    
    def rtnMydict(self,passServer=''):
    
        if not passServer:
            return self.mydict

        key_list = list(self.mydict.keys())

        if passServer in key_list:
            return self.mydict[passServer]
        else:
            return False