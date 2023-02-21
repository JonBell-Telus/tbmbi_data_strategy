class Oracle:
     #--> Connection values.
    mydict = {}

    mydict['hpbipr'] = (
        'HPBIPR-scan.corp.ads',
        'tbmbi_rpt',
        'HZBU02CD#',
        'HPBIPRsva.WORLD'
    )

    mydict['camppr'] = (
        'CAMPPR-scan.corp.ads',
        'DBMARKETSVC',
        'telus2017',
        'CAMPPRsva.WORLD'
    )

    mydict['racep12db01'] = (
        'rac-ep12db01-scan.corp.ads',
        't873272',
        'T873s3#t',
        'GISRT1PRsv1'
    )

    mydict['POL1rpt2.WORLD'] = (
        'KBPR-scan',
        'DBMARKETSVC',
        'welcome2016',
        'POL1rpt2.WORLD'
    )
    
    mydict['buswspr'] = (
        'rac-btlp000789-scan.corp.ads',
        'DBMARKETSVC',
        'SVC_DBMARKET',
        'BUSWSPR'
    )
    encoding = 'UTF-8'

    host_name = ''
    user_name = ''
    user_password = ''
    svc_name = ''
    url = ''

    driver = 'oracle.jdbc.driver.OracleDriver'

    spark = ''
    def __init__(self, passServer='',session=''):
        
        self.spark = session

        if passServer:
            serverID = self.mydict[passServer]

            self.host_name = serverID[0]
            self.user_name = serverID[1]
            self.user_password = serverID[2]
            self.svc_name = serverID[3]
 
            self.url = f'jdbc:oracle:thin:@{self.host_name}:41521/{self.svc_name}'
             
    def query(self,sql):
        
        try:
            df = self.spark.read.format('jdbc').option('url',self.url).option('driver',self.driver).option("fetchsize", "100000").option('dbtable',sql).option('user',self.user_name).option('password',self.user_password).load()
        except Exception as e:
            print(e)
            
        return df
    
    def rtnMydict(self,passServer=''):
    
        if not passServer:
            return self.mydict

        key_list = list(self.mydict.keys())
        
        if passServer in key_list:
            return self.mydict[passServer]
        else:
            return False