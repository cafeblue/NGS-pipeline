#! /bin/env python3
import pickle
import pymysql.cursors
import smtplib
from email.mime.text import MIMEText

def SendEmail(Subject, Receiptors, Content):
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
        return self.cursor.fetchall()

    def Recode(self, keyword):
        self.alldict = dict()
        for row in self.cursor:
            if row[keyword] in self.alldict:
                self.alldict[row[keyword]].append(row)
            else:
                self.alldict[row[keyword]] = [row]
        return self.alldict

