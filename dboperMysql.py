import pymysql

# -*- coding: utf-8 -*-
"""
Created: 2015/12/21 15:13
@author: Xiaodong
"""

class MysqlDB:
    def __init__(self, user, passwd, host):
        self.user = user
        self.passwd = passwd
        self.host = host
        pass
        
    def __del__(self):
        try:
            if(self.hCur):
                self.hCur.close()
            if(self.hConn):
                self.hConn.close()
        except(pymysql.Error) as e:
            print('release error')
        pass 
    
    def dbConnect(self,database):
        try:
            self.hConn = pymysql.connect(host=self.host, user=self.user, passwd=self.passwd,db=database,charset="utf8")
            self.hCur = self.hConn.cursor()
        except(pymysql.Error) as e:
            print('Error Connect')
            pass
    
    def dbSwapColumn(self, src, dst, tbl):
        try:
            self.hCur.execute('update %s set %s = %s'%(tbl,dst,src))
            self.hConn.commit()
            #print 'update %s set %s = %s'%(tbl,dst,src)
        except(pymysql.Error) as e:
            print('Error Swap')
        pass
    
    def dbSwapColumnNonZero(self,src,dst,tbl):
        try:
            self.hCur.execute('update %s set %s = %s where %s <> 0'%(tbl,dst,src,src))
            self.hConn.commit()
        except(pymysql.Error) as e:
            print('Error Swap')
        pass
    
    def dbClear(self,tbl):
        try:
            self.hCur.execute('truncate table %s'%tbl)
            self.hConn.commit()
        except(pymysql.Error) as e:
            print('Error Clear')
        pass
    
    #set one column to zero
    def dbSetColumnZero(self,col, tbl):
        try:
            self.hCur.execute('update %s set %s = 0'%(tbl,col))
            self.hConn.commit()
        except(pymysql.Error) as e:
            print('Error Clear Column')
        pass
    
    def dbQueryTable(self, query):
        self.hCur.execute(query)
        return self.hCur.fetchall()
    
    def dbExecuteMany(self, query, values):
        try:
            self.hCur.executemany(query, values)
            self.hConn.commit()
        except(pymysql.Error) as e:
            print(e.args[0],e.args[1])
    
    def dbExecute(self, query):
        self.hCur.execute(query)
        self.hConn.commit()
        pass

if __name__ == '__main__':
    db_op = MysqlDB('root','db@101_WATER','192.168.10.101')
    #db_op.dbConnect('backoffice_cmb1')
    #result = db_op.dbQueryTable("select * from price where date='2015-12-17'")
    #print result[0]
    
    db_op.dbConnect('test')
    db_op.dbSwapColumn('ltp','prev','test')
    values = [(11,12),(12,13),(13,14),(15,15),(16,16)]
    query = "insert into test(ltp,prev) values(%s,%s)"
    db_op.dbExecuteMany(query, values)
