import os
import jaydebeapi
import pandas as pd

from sqlserver import SqlServer

class DNTL:
    def __init__(self):
        dbConn = SqlServer('btwp001611')

        query = "select cbu_cid,rcid from rgu.dbo.dntl_hpbi"

        df = dbConn.query(query) #--> Retrieve all the rows from the input table.

        #--> Change columns to integers for faster lookup 

        df['cbu_cid'] = df['cbu_cid'].astype(int)
        df['rcid'] = df['rcid'].astype(int)

        #--> create two dataframes 1 for each cbu_cid and rcid.
        cbudf = df[['cbu_cid']]
        rdf = df[['rcid']]

        self.cbudfIdx = cbudf.drop_duplicates(subset=['cbu_cid']).set_index('cbu_cid')
        self.rdfIdx = rdf.drop_duplicates().set_index('rcid')
         
    def checkCbuCid(self,cbucid):
         
        try:
            self.cbudfIdx.loc[int(cbucid)]
            return True
        except:
           return False

    def checkRCid(self,rcid):
        try:
            self.rdfIdx.loc[int(rcid)]
            return True
        except:
           return False