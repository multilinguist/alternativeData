#!/usr/bin/env python
# coding=utf-8  
# Python 3.7
# xuxiaodong,2015-12-24 14:30:30
import time,csv,sys,math,platform
import logging
sys.path.append(".")
from dboperMysql import MysqlDB

def genUpdateCmdStr(tblname,valuecols,keycols):
    querycol1 = map(lambda x:x+'=%s',valuecols)
    querycol2 = map(lambda x:x+'=%s',keycols)
    updateCmd = "update %s set "%tblname
    updateCmd += ','.join(querycol1)
    updateCmd += ' where '
    updateCmd += ' and '.join(querycol2)
    return updateCmd

def updateItems(db_op,tblname,cols,keycols,values):
    updateCmd = genUpdateCmdStr(tblname,cols,keycols)
    print(updateCmd)
    db_op.dbExecuteMany(updateCmd,values)

def genInsertQueryStr(tblname, columns):
    insertCmd = "insert ignore into " + tblname
    insertCmd += "("
    insertCmd += ','.join(columns)
    insertCmd += ")"

    formatlist = ['%s']*len(columns)
    formatstr = " values(" + ','.join(formatlist) + ")"
    
    insertCmd += formatstr
    return insertCmd

def genReplaceQueryStr(tblname, columns):
    insertCmd = "replace into " + tblname
    insertCmd += "("
    insertCmd += ','.join(columns)
    insertCmd += ")"

    formatlist = ['%s']*len(columns)
    formatstr = " values(" + ','.join(formatlist) + ")"
    
    insertCmd += formatstr
    return insertCmd

def genSelectQueryStr(tblname, columns):
    selectCmd = "select "
    selectCmd += ','.join(columns)
    selectCmd += ' from '
    selectCmd += tblname
    
    return selectCmd

def selectItems(db_op,table,columns):
    selectCmd = genSelectQueryStr(table, columns)
    print(selectCmd)
    return db_op.dbQueryTable(selectCmd)
    
def insertItems(db_op,table,columns,values):
    insertCmd = genInsertQueryStr(table,columns)
    print(insertCmd)
    #print values
    db_op.dbExecuteMany(insertCmd,values)

def replaceItems(db_op,table,columns,values):
    insertCmd = genReplaceQueryStr(table,columns)
    print(insertCmd)
    db_op.dbExecuteMany(insertCmd,values)

if __name__ == '__main__':
    db_op = MysqlDB('root','root','192.168.10.105')
    
    today = time.strftime('%Y%m%d',time.localtime(time.time()))
    #today = '20151225'
    print(today)