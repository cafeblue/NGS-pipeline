#! /bin/env python3

import sys
import os
import pickle
import pymysql.cursors
from pathlib import Path

class Usage:
    """ 
    Check the idle jobs on HPF

        Usage: python3 sys.argv[0] 
        Example: python3 find_missing_job.py clinicalB.dbcfg

        """

def SendEmail(Subject, Receiptors, Content):
    import smtplib
    from email.mime.text import MIMEText
    msg= MIMEText(Content)
    msg['Subject'] = Subject
    msg['From'] = 'notice@thing1.sickkids.ca'
    msg['To'] = Receiptors

    s = smtplib.SMTP('127.0.0.1')
    s.send_message(msg)
    s.quit()

class DB_Connector():
    """ Humble Database Connection Class """
    def __init__(self, dbfile):
        self.DBCfg(dbfile)
        self.CreateConnection()
        #self.dbcfg = self.dbcfg.AllCfg

    def DBCfg(self, dbfile):
        self.dbcfg = pickle.load( open( dbfile, "rb" ) )

    def CreateConnection( self ):
        #self.conn = pymysql.connect(host='localhost', db='clinicalB', user='wei.wang', port=5029, passwd='baccaharis')
        self.conn = pymysql.connect(host=self.dbcfg['host'], db=self.dbcfg['db'], user=self.dbcfg['user'], port=self.dbcfg['port'], passwd=self.dbcfg['passwd'])
        self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)

    def DestroyConnection( self ):
        self.cursor.close()

    def Execute( self, sql_statement ):
        self.cursor.execute(sql_statement)
        return self.cursor

    def Recode(self, keyword):
        self.alldict = dict()
        for row in self.cursor:
            if row[keyword] in self.alldict:
                self.alldict[row[keyword]].append(row)
            else:
                self.alldict[row[keyword]] = [row]
        return self.alldict

class QueryDB(DB_Connector):

    def __init__(self, dbfile):
        self.querydb = DB_Connector(dbfile)

    def RunningPPID(self):
        self.ppids = list()
        ppidl = self.querydb.Execute("SELECT postprocID FROM sampleInfo WHERE currentStatus = '2'")
        for row in ppidl:
            self.ppids.append(row['postprocID'])

        return self.ppids

    def MissingJobID (self):
        self.stuckppids = list()
        for ppid in self.ppids:
            mylist = self.querydb.Execute("SELECT sampleID,postprocID,jobName FROM hpfJobStatus WHERE postprocID = '%s' AND exitcode IS NULL AND TIMESTAMPADD(HOUR,4,time)<CURRENT_TIMESTAMP LIMIT 1" % (ppid))
            for job in mylist:
                tmpstr = "job " + str(job['jobName']) + " of sampleID: " + str(job['sampleID']) + " ppID: " + str(job['postprocID']) + " idled over 4 hours!"
                self.stuckppids.append(tmpstr)

        return self.stuckppids

def main(name, dbfile):

    if Path(dbfile).is_file():
        samplesheets = QueryDB(dbfile)
        samplesheets.RunningPPID()
        lines = samplesheets.MissingJobID()
        myemail_content = "\n".join(lines)
        if myemail_content != '':
            SendEmail('Jobs idling on HPF', 'weiw.wang@sickkids.ca', myemail_content)

    else :
        print("file: " + str(dbfile) + " does not exists!")

if __name__ == '__main__':

    if len(sys.argv) != 2:
        print(Usage.__doc__)
        sys.exit(2)

    main(*sys.argv)
