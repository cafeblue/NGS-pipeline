#! /bin/env python3
import re
import subprocess
from pathlib import Path
from utils.config import GlobalConfig

def parseRunInfo(folder):
    runinfo = { 'LaneCount': 0, 'SurfaceCount': 0, 'SwathCount': 0, 'TileCount': 0, 'SectionPerLane': 0, 'LanePerSection': 0 }
    if Path(folder).is_file():
        command = 'grep "NumCycles=" ' + folder
        numcycles = subprocess.run(command, stdout=subprocess.PIPE, shell=True).stdout.decode("utf-8")
        if numcycles.count('NumCycles="') == 3 :
            cycNum1, indexCycNum, cycNum2 = re.findall('(?<=NumCycles=")\d+', numcycles)
            runinfo['NumCycles'] = [int(cycNum1), int(indexCycNum), int(cycNum2)]

        command = 'grep "<FlowcellLayout" ' + folder
        for items in  subprocess.run(command, stdout=subprocess.PIPE, shell=True).stdout.decode("utf-8").split(' '):
            if re.match('\w+="\d+"', items) :
                flowcellLayout, num = items.split('=')
                num = int(re.sub(r'"', r'', num))
                runinfo[flowcellLayout] = num
    return runinfo

class Sequencers():
    """Read in all the config of Sequencers"""
    def __init__(self, conn):
        self.active_seq = conn.Execute("SELECT * FROM sequencers WHERE active = '1'")
        for row in self.active_seq:
            setattr(self, row['machine'], row)


class ilmnBarCode():
    def __init__(self, conn):
        self.barcodes = conn.Execute("SELECT code,value FROM encoding WHERE tablename = 'sampleSheet' AND fieldname = 'barcode'")
        setattr(self, "", "")
        for row in self.barcodes:
            setattr(self, row['code'], row['value'])

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

    def demultiplex_samplesheet(self):
        for fc,rows in self.byflowcell.items():
            self.samplesheet = getattr(self, re.sub(r'_.*', "", getattr(self, fc)))
            self.samplesheet(rows)
            
            self.sampleSheetFile = getattr(self.globalconfig, "SAMPLE_SHEET") + "/%s_%s_%s_samplesheet.csv" % (re.sub(r'_.*', "", getattr(self, fc)),  fc, self.time.longdate)
            f = open( self.sampleSheetFile, 'w')
            f.write(self.samplesheetstring)

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


