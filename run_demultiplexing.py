#! /bin/env python3
"""
This script is to find the finished runfolders on the sequencers and run demultiplexing.
"""

import sys
import os
import re
import parseInterOp
from pathlib import Path
from utils.dbtools import DB_Connector 
from utils.sequencers import getRunInfo, SampleSheet
from utils.common import TimeString
from utils.config import GlobalConfig
from utils.SendEmail import SendEmail
from utils.jsub import jsub
from interop import py_interop_run_metrics, py_interop_run, py_interop_summary
from datetime import datetime, date, time

class Usage:
    """
        Find the finished runfolders on the sequencers and run demultiplexing.

        Usage: python3 rsyncIlmnRunDir.py database.cnf
        Example: python3 rsyncIlmnRunDir.py clinicalB.cnf

    """

def getSequencingList(config, conn):
    runningSeqs = conn.Execute('SELECT flowcellID,machine,destinationDir,cycleNum,LaneCount,SurfaceCount,SwathCount,TileCount,SectionPerLane,LanePerSection from thing1JobStatus where sequencing ="2"')
    if len(runningSeqs) > 0 :
        flag = 0
        finished_seqs = list()
        for runningSeq in runningSeqs:
            if check_status(runningSeq, config) == 1:
                finished_seqs.append(dict(runningSeq))
                flag += 1
        if flag > 0:
            return finished_seqs

    sys.exit(0)

def check_status(runningSeq, config):
    runningSeq['destinationDir'] = re.sub(r'wei.wang@data1.ccm.sickkids.ca:', '', runningSeq['destinationDir'])
    if Path(runningSeq['destinationDir'] + eval(getattr(config, "LAST_BCL_" + re.sub(r'_.{1,2}$', '', runningSeq['machine'])))).is_file():
        completeFile = runningSeq['destinationDir'] + getattr(config, "COMPLETE_FILE_" + re.sub(r'_.{1,2}$', '', runningSeq['machine']))
        if Path(completeFile).is_file() :
            if (datetime.now() - datetime.fromtimestamp(os.path.getmtime(completeFile))).seconds > 600:
                return 1
    return 2

def main(name, dbfile):

    if Path(dbfile).is_file():
        pass
    else:
        print("ERR: file: " + str(dbfile) + " does not exists!")
        print(Usage.__doc__)
        sys.exit(2)

    conn = DB_Connector(dbfile)
    config = GlobalConfig(conn)

    finishedSeqs = getSequencingList(config, conn)

    timestamp = TimeString()
    timestamp.print_timestamp()
    for runningSeq in finishedSeqs:
        ssRows = conn.Execute("SELECT * FROM sampleSheet WHERE flowcell_ID = '%s'" % (runningSeq['flowcellID'])) 
        samplesheet = SampleSheet(ssRows, conn, timestamp)
        samplesheet.demultiplex_samplesheet()

        realCycleNum = sum(getRunInfo(runningSeq['destinationDir'] + "/" + getattr(config, 'SEQ_RUN_INFO_FILE'))['NumCycles'])
        if realCycleNum != int(runningSeq['cycleNum']) :
            conn.Execute("UPDATE thing1JobStatus SET sequencing = '0' where flowcellID = '%s'" % (runningSeq['flowcellID']))
            SendEmail( "%s on %s failed!!" % (runningSeq['flowcellID'], runningSeq['machine']), "weiw.wang@sickkids.ca", "%s failed. The final cycle number, %d does not equal to the initialed cycle number %s \n" % (runningSeq['runDir'], realCycleNum, runningSeq['cycleNum']))
        else:
            jsubLogFolder = getattr(config, 'JSUB_LOG_FOLDER') + "demultiplex_" + runningSeq['machine'] + '_' + runningSeq['flowcellID'] + "_" + timestamp.fulltime 
            command = "bcl2fastq -R %s -o %s --sample-sheet %s" % (runningSeq['destinationDir'], getattr(config, "FASTQ_FOLDER") + runningSeq['machine'] + "_" + runningSeq['flowcellID'], samplesheet.sampleSheetFile)
            jobID = jsub( "demultiplex_" + runningSeq['machine'] + '_' + runningSeq['flowcellID'] + "_" + timestamp.fulltime, getattr(config, 'JSUB_LOG_FOLDER'),  command, "22000", "12", "01:00:00", "30", "bcl2fastq/2.19.0", '', '1')
            conn.Execute("UPDATE thing1JobStatus SET sequencing = '1', demultiplexJobID = '%s' , demultiplex = '2' , seqFolderChksum = '2', demultiplexJfolder = '%s' where flowcellID = '%s'" % (jobID, jsubLogFolder, runningSeq['flowcellID']))
            SendEmail( "status of %s on %s" % (runningSeq['flowcellID'], runningSeq['machine']), "weiw.wang@sickkids.ca", "Sequencing finished successfully, demultiplexing is starting...\n")
            ###  interOp
            parseInterOp(conn, runningSeq['destinationDir'], runningSeq['flowcellID'])

if __name__ == '__main__':

    if len(sys.argv) != 2:
        print(Usage.__doc__)
        sys.exit(2)

    main(*sys.argv)
