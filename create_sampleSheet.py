#! /bin/env python3

"""
    This script is to read the latest records in table sampleSheet and generate the samplesheet for the sequencer.
"""


import sys
import os
import pickle
from pathlib import Path
from utils import *


class Usage:
    """

    Program to read the new records in table sampleSheet and create the sampleSheet for sequencer.

        Usage: python3 read_new_sampleSheet_rows.py database.cnf
        Example: python3 read_new_sampleSheet_rows.py clinicalB.cnf

    """

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

            else:
                self.byflowcell[row['flowcell_ID']].append(row)

    def hiseq_samplesheet(self):
        self.hiseq_samplesheet.append("abc")


def main(name, dbfile):

    if Path(dbfile).is_file(): 
        pass
    else:
        print("ERR: file: " + str(dbfile) + " does not exists!")
        print(Usage.__doc__)
        sys.exit(2)

    conn = DB_Connector(dbfile)
    newRows = conn.Execute("SELECT * FROM sampleSheet WHERE TIMESTAMPADD(SECOND,361,time) > NOW()")

    if len(newRows) == 0 :
        sys.exit(0)

    samplesheet = SampleSheet(newRows)


if __name__ == '__main__':

    if len(sys.argv) != 2:
        print(Usage.__doc__)
        sys.exit(2)

    main(*sys.argv)
