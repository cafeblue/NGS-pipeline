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

class ilmnBarCode():
    def __init__(self, conn):
        self.barcodes = conn.Execute("SELECT code,value FROM encoding WHERE tablename = 'sampleSheet' AND fieldname = 'barcode'")
        setattr(self, "", "")
        for row in self.barcodes:
            setattr(self, row['code'], row['value'])

class GlobalConfig():
    """ Read in all the config from table config in the database."""
    def __init__(self, conn):
        self.rows = conn.Execute('SELECT * FROM config')
        setattr(self, "", "")
        for row in self.rows:
            setattr(self, row['vName'], row['vValue'])

class Sequencers():
    """Read in all the config of Sequencers"""
    def __init__(self, conn):
        self.active_seq = conn.Execute("SELECT * FROM sequencers WHERE active = '1'")
        for row in self.active_seq:
            setattr(self, row['machine'], row)


class SampleSheet(Sequencers, ilmnBarCode, GlobalConfig):
    """ An object for samplesheet issues """

    def __init__(self, rows, conn, time):
        self.time = time
        self.barcode = ilmnBarCode(conn) 
        self.globalconfig = GlobalConfig(conn)
        self.sequencers = Sequencers(conn)
        self.conn = conn
        self.hiseq_samplesheet = "FCID,Lane,SampleID,SampleRef,Index,Description,Control,Recipe,Operator,SampleProject\n"
        self.bcltools_samplesheet = '[Header]\nIEMFileVersion,4\nDate,%s\nWorkflow,GenerateFASTQ\nApplication,%s FASTQ Only\nAssay,TruSeq HT\nDescription,\nChemistry,Default\n\n[Reads]\n%s\n%s\n\n[Settings]\nAdapter,AGATCGGAAGAGCACACGTCTGAACTCCAGTCA\nAdapterRead2,AGATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT\n\n[Data]\n'
        self.list2dict(rows)
    
    def list2dict(self, rows):
        self.byflowcell = dict()
        for row in rows:
            if not row['flowcell_ID'] in self.byflowcell:
                self.byflowcell[row['flowcell_ID']] = list()
                self.byflowcell[row['flowcell_ID']].append(row)
                setattr(self, row['flowcell_ID'], row['machine'])

            else:
                self.byflowcell[row['flowcell_ID']].append(row)

    def seq_samplesheet(self):
        for fc,rows in self.byflowcell.items():
            self.sequencer = getattr(self.sequencers, getattr(self, fc))
            if self.sequencer['sampleSheetFolder'] == '':
                continue

            self.samplesheet = getattr(self, re.sub(r'_.*', "", getattr(self, fc)))
            self.samplesheet(rows)

            print(self.samplesheetstring)
            f = open( self.sequencer['sampleSheetFolder'] + "/%s_%s_samplesheet.csv" % (self.time.longdate, fc), 'w')
            f.write(self.samplesheetstring)

    def demultiplex_samplesheet(self, flowcellid = None):
        print("4demultiplexing")
        self.list2dict(self.conn.Execute("SELECT * FROM sampleSheet WHERE flowcell_ID = '%s'" % (flowcellid)))

        self.samplesheet = getattr(self, re.sub(r'_.*', "", getattr(self, flowcellid)))
        self.samplesheet(rows)




    def hiseq2500(self, rows, cycle=None):
        if cycle is None:
            cycle = (101, 101)

        self.samplesheetstring = self.bcltools_samplesheet % (self.time.dateslash, 'HiSeq2500', cycle[0], cycle[1])
        self.samplesheetstring += getattr(self.globalconfig, "SAMPLESHEET_HEADER_hiseq2500")
        for row in rows:
            for lane in row['lane'].split(r','):
                self.samplesheetstring += '%s,%s,,,,,%s,,\n' % (lane,row['sampleID'], getattr(self.barcode, row['barcode']))
        #self.samplesheetstring = self.hiseq_samplesheet
        #for row in rows:
        #    for lane in row['lane'].split(r','):
        #        self.samplesheetstring += '%s,%s,%s,b37,%s,%s-%s,,N,R1,%s,%s-%s\n' % (row['flowcell_ID'], lane, row['sampleID'], getattr(self.barcode, row['barcode']), row['capture_kit'], row['sample_type'], row['ran_by'], row['machine'], row['flowcell_ID'] )

    def nextseq500(self, rows, cycle=None):
        if cycle is None:
            cycle = (151, 151)

        self.samplesheetstring = self.bcltools_samplesheet % (self.time.dateslash, 'NextSeq500', cycle[0], cycle[1])
        self.samplesheetstring += getattr(self.globalconfig, "SAMPLESHEET_HEADER_nextseq500")
        for row in rows:
            self.samplesheetstring += '%s,,,,%s,%s,,\n' % (row['sampleID'], row['barcode'], getattr(self.barcode, row['barcode']))


    def miseqdx(self, rows, cycle=None):
        if cycle is None:
            cycle = (151, 151)

        self.samplesheetstring = self.bcltools_samplesheet % (self.time.dateslash, 'MiSeqDx', cycle[0], cycle[1])
        self.samplesheetstring += getattr(self.globalconfig, "SAMPLESHEET_HEADER_miseqdx")
        for row in rows:
            self.samplesheetstring += '%s,,,,%s,%s,%s,%s,,,,\n' % (row['sampleID'], row['barcode'], getattr(self.barcode, row['barcode']), row['barcode2'], getattr(self.barcode, row['barcode2']))


