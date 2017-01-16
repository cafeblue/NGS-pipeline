#! /bin/env python3

import sys
import pickle
import pymysql
import pymysql.cursors


class DBCfg:
    def __init__(self, dbfile):
        self.db = pickle.load( open( dbfile, "rb" ) )

    #def AllCfg(self):
    #    return self.db

class DB_Connector(DBCfg):
    """ Humble Database Connection Class """
    def __init__(self, dbfile):
        self.cfg = DBCfg(dbfile)
        self.CreateConnection()
        #self.cfg = self.cfg.AllCfg

    def CreateConnection( self ):
        #self.conn = pymysql.connect(host='localhost', db='clinicalB', user='wei.wang', port=5029, passwd='baccaharis')
        self.conn = pymysql.connect(host=self.cfg.db['host'], db=self.cfg.db['db'], user=self.cfg.db['user'], port=self.cfg.db['port'], passwd=self.cfg.db['passwd'])
        self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)

    def DestroyConnection( self ):
        self.cursor.close()

    def Execute( self, sql_statement ):
        self.cursor.execute(sql_statement)
        return self.cursor

class SampleSheet(DB_connector):
    
    def __init_(self, rows):
        self.rows = rows;
        self.Fcontent = ''

    def 

    def SS4Sequencer(self):
        for row in self.rows:
            tmp = row
            for mykey in self.keys:
                tmpk = row[mykey]
                tmp = dict(tmpk=tmp)
            else:
                self.alldict.append(tmp)

        return self.Fcontent

    def SS4Demultiplex(self):
        for row in self.rows:
            pass

        return self.Fcontent


def main(prog_parm):
    dbconn = DB_Connector(sys.argv[1])
    sql_query = "SELECT * FROM sampleSheet WHERE TIMESTAMPADD(DAY,20,time) > NOW()"
    dbconn.Execute(sql_query)
    keywords = ['flowcell_ID', 'machine']
    rows = dbconn.ReCode(keywords)
    print(rows)
    #for row in rows:
    #    print(row)
    #print(DBCfg.db)

if __name__ == '__main__' :
    main(sys.argv[:])
