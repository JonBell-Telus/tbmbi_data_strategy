
import pandas as pd
import cx_Oracle

 #--> Connection values.
mydict = {}

mydict['hpbipr'] = (
    'HPBIPR-scan.corp.ads',
    'tbmbi_rpt',
    'HZBU02CD#',
    'HPBIPRsva.WORLD'
)

mydict['hpbipr-2'] = (
    'HPBIPR-scan.corp.ads',
    'DBMARKETSVC_RPT',
    'rpt#marketsvc',
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

encoding = 'UTF-8'

class Oracle:

    def __init__(self,passServer=''):
        
        if passServer:
            serverID = mydict[passServer]

            host_name = serverID[0]
            user_name = serverID[1]
            user_password = serverID[2]
            svc_name = serverID[3]
            
            dsnStr = cx_Oracle.makedsn(host_name, "41521", service_name=svc_name)
    
            self.connection = cx_Oracle.connect(user_name,user_password,dsnStr,encoding= encoding)
                            
            self.cursor = self.connection.cursor()

    def modify(self,sql,data):
        self.cursor.execute(sql,data)
        self.connection.commit()
        return  

    def query(self,sql):
       # self.cursor.execute(sql)
        return pd.read_sql(sql,self.connection)

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