#! /bin/env python3
import sys
import pickle
import pymysql.cursors
from utils.SendEmail import *

def getActiveRunFolders(conn):
    activeSeq = ""
    for tempdict in conn.Execute("SELECT runFolder from sequencers where active = '1'"):
        activeSeq += tempdict['runFolder'] + " "
    return activeSeq

class CronControlPanel():
    """a object of CronControlPanel, do all functions on table cronControlPanel"""
    def __init__(self, conn):
        self.conn = conn

    def start_process(self, column):
        status = self.conn.Execute("SELECT %s FROM cronControlPanel" % (column))
        if status[0][column] == 1:
            SendEmail("%s is still running..." % (column), "weiw.wang@sickkids.ca", "abort...")
            sys.exit(0)
        else :
            self.conn.Execute("UPDATE cronControlPanel SET %s = '1'" % (column))

    def stop_process(self, column):
        self.conn.Execute("UPDATE cronControlPanel SET %s = '0'" % (column))

    def update_rf(self, folder):
        self.conn.Execute("UPDATE cronControlPanel SET  sequencer_RF = '%s'" % (folder))

    def get_rf(self ):
        status = self.conn.Execute("SELECT sequencer_RF FROM cronControlPanel")
        return(status[0]['sequencer_RF'])

class DB_Connector():
    """ Humble Database Connection Class """
    def __init__(self, dbfile):
        self.DBCfg(dbfile)
        self.CreateConnection()

    def DBCfg(self, dbfile):
        self.dbcfg = pickle.load( open( dbfile, "rb" ) )

    def CreateConnection( self ):
        self.conn = pymysql.connect(host=self.dbcfg['host'], db=self.dbcfg['db'], user=self.dbcfg['user'], port=self.dbcfg['port'], passwd=self.dbcfg['passwd'])
        self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)

    def DestroyConnection( self ):
        self.cursor.close()

    def Execute( self, sql_statement ):
        self.cursor.execute(sql_statement)
        return self.cursor.fetchall()

    def Recode(self, keyword):
        self.alldict = dict()
        for row in self.cursor:
            if row[keyword] in self.alldict:
                self.alldict[row[keyword]].append(row)
            else:
                self.alldict[row[keyword]] = [row]
        return self.alldict

