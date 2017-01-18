#! /bin/env python3

import sys
import re
import pickle
import pymysql.cursors
import smtplib
from datetime import datetime, date, time
from email.mime.text import MIMEText

def SendEmail(Subject, Receiptors, Content):
    msg= MIMEText(Content)
    msg['Subject'] = Subject
    msg['From'] = 'notice@thing1.sickkids.ca'
    msg['To'] = Receiptors

    s = smtplib.SMTP('127.0.0.1')
    s.send_message(msg)
    s.quit()

class TimeString():
    """ generate a string of the date/time"""

    def __init__(self):
        self.timestring = datetime.now()
        self.fulltime = self.timestring.strftime("%Y%m%d%H%M%S")
        self.dateslash = self.timestring.strftime('%m/%d/%Y')
        self.longdate = self.timestring.strftime('%Y%m%d')
        self.yesterday = self.timestring.replace(day=self.timestring.day-1).strftime('%Y%m%d')
        self.timestamp = self.timestring.strftime("%Y-%m-%d %H:%M:%S")

    def print_timestamp(self):
        print("\n\n_/ _/ _/ _/ _/ _/ _/ _/\n  " + self.timestamp + "\n_/ _/ _/ _/ _/ _/ _/ _/\n")
        print("\n\n_/ _/ _/ _/ _/ _/ _/ _/\n  " + self.timestamp + "\n_/ _/ _/ _/ _/ _/ _/ _/\n", file=sys.stderr)

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

class GlobalConfig():
    """ Read in all the config from table config in the database."""
    def __init__(self, conn):
        self.rows = conn.Execute('SELECT * FROM config')
        for row in self.rows:
            setattr(self, row['vName'], row['vValue'])

class SampleSheet():
    """ An object for samplesheet issues """

    def __init__(self, rows, conn):
        self.conn = conn
        self.byflowcell = dict()
        self.hiseq_samplesheet = "FCID,Lane,SampleID,SampleRef,Index,Description,Control,Recipe,Operator,SampleProject\n"
        self.bcltools_samplesheet = '[Header]\nIEMFileVersion,4\nDate,{1}\nWorkflow,GenerateFASTQ\nApplication,{2} FASTQ Only\nAssay,TruSeq HT\nDescription,\nChemistry,Default\n\n[Reads]\n{3}\n{4}\n\n[Settings]\nAdapter,AGATCGGAAGAGCACACGTCTGAACTCCAGTCA\nAdapterRead2,AGATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT\n\n[Data]\n'
        self.list2dict(rows)
    
    def list2dict(self, rows):
        for row in rows:
            if not row['flowcell_ID'] in self.byflowcell:
                self.byflowcell[row['flowcell_ID']] = list()
                self.byflowcell[row['flowcell_ID']].append(row)
                setattr(self, row['flowcell_ID'], re.sub(r'_.*', "", row['machine']))

            else:
                self.byflowcell[row['flowcell_ID']].append(row)

    def seq_samplesheet(self):
        for fc,rows in self.byflowcell.items():
            self.samplesheet = getattr(self, getattr(self, fc))
            self.samplesheet(rows)

    def demultiplex_samplesheet(self, flowcellid = None):
        pass

    def hiseq2500(self, rows):
        print("hiseq2500")
        self.samplesheetstring = self.hiseq_samplesheet
        for row in rows:
            for lane in row['lane'].split(r','):
                self.samplesheetstring += '%s,%s,%s,b37,Index,%s-%s,,N,R1,%s,%s-%s\n' % (row['flowcell_ID'], lane, row['sampleID'], row['capture_kit'], row['sample_type'], row['ran_by'], row['machine'], row['flowcell_ID'] )

        print(self.samplesheetstring)

    def nextseq500(self, rows):
        pass

    def miseqdx(self, rows):
        print('miseqdx')
        print(rows)
