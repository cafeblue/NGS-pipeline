import sys

import pickle
import pymysql
import pymysql.cursors

#conn= pymysql.connect(host='localhost',user='user',password='user',db='testdb',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
#a=conn.cursor()
#sql='CREATE TABLE `users` (`id` int(11) NOT NULL AUTO_INCREMENT,`email` varchar(255) NOT NULL,`password` varchar(255) NOT NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;'
#a.execute(sql)

class Usage:
    """

    Program to read the new records in table sampleSheet and create the sampleSheet for sequencer.

        Usage: python3 read_new_sampleSheet_rows.py database.cnf
        Example: python3 read_new_sampleSheet_rows.py clinicalB.cnf

    """

class DBCfg():
    def __init__(self, dbfile):
        db = pickle.load( open( dbfile, "rb" ) )

class DB_Connector(DBCfg):
    """ Humble Database Connection Class """
    def __init__(self, dbfile):
        self.cfg = DBcfg(dbfile)
        self.CreateConnection()

    def CreateConnection( self ):
        #self.conn = pymysql.connect(host='localhost', db='clinicalB', user='wei.wang', port=5029, passwd='baccaharis')
        self.conn = pymysql.connect(host=self.cfg.db['host'], db=self.cfg.db['db'], user=self.cfg.db['user'], port=self.cfg.db['port'], passwd=self.cfg.db['passwd'])
        self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)

    def DestroyConnection( self ):
        self.cursor.close()

    def Execute( self, sql_statement ):
        self.cursor.execute(sql_statement)
        return self.cursor

class NewRows(DB_Connector):
    """ Grab the latest rows in the table sampleSheet """
    def __init__(self):
        self.newrows = []

    def __len__(self):
        return len(self.newrows)

    def ReadNewRows(self):
        sql_query = "SELECT * FROM sampleSheet WHERE TIMESTAMPADD(DAY,5,time) > NOW()"
        self.Execute(sql_query)

class SampleSheet():

    def __init__(self, rows):
        self.rows = rows

    def SequencerSampleSheet(self):
        pass

    def DemultiplexSampleSheet(self):
        pass

class NewSampleSheet(NewRows):
    def __init__(self):
        self.allrows = []

    def GetNewRows(self):
        self.ReadNewRows()


def main(prog_argvs):
    if len(prog_argvs) != 2:
        print(Usage.__doc__)
        sys.exit(2)

    else :
        samplesheets = NewRows()
        abc = samplesheets.ReadNewRows()
        for row in abc:
            print(row)
        #samplesheets.GetNewRows()


if __name__ == '__main__':
    main(sys.argv[:])
