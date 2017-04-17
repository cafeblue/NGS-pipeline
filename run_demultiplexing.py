#! /bin/env python3
"""
This script is to find the finished runfolders on the sequencers and run demultiplexing.
"""

import sys
import os
import re
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

def parseInterOp(conn, run_folder, flowcellID):
    run_metrics = py_interop_run_metrics.run_metrics()
    run_folder = run_metrics.read(run_folder)
    summary = py_interop_summary.run_summary()
    py_interop_summary.summarize_run_metrics(run_metrics, summary)

    readsClusterDensity = ",".join(str(int(round(summary.at(0).at(i).density().mean()/1000.0))) + "+/-" + str(int(round(summary.at(0).at(i).density().stddev()/1000.0))) for i in range(summary.lane_count()))
    perReadsPassingFilter = ",".join(str(round(summary.at(0).at(i).reads_pf()/1000000.0, 2)) for i in range(summary.lane_count()))
    perQ30Score = ",".join(str(round(summary.at(0).at(i).percent_gt_q30(), 2)) for i in range(summary.lane_count()))
    numTotalReads = ",".join(str(round(summary.at(0).at(i).reads()/1000000.0, 2)) for i in range(summary.lane_count()))
    aligned = ",".join(str(round(summary.at(0).at(i).percent_aligned().mean(), 2)) + "+/-" + str(round(summary.at(0).at(i).percent_aligned().stddev(), 2)) for i in range(summary.lane_count()))
    errorRate = ",".join(str(round(summary.at(0).at(i).error_rate().mean(), 2)) + "+/-" + str(round(summary.at(0).at(i).error_rate().stddev(), 2)) for i in range(summary.lane_count()))
    clusterPF = ",".join(str(round(summary.at(0).at(i).percent_pf().mean(), 2)) + "+/-" + str(round(summary.at(0).at(i).percent_pf().stddev(), 2)) for i in range(summary.lane_count())) 

    readsClusterDensity += "," + ",".join(str(int(round(summary.at(2).at(i).density().mean()/1000.0))) + "+/-" + str(int(round(summary.at(2).at(i).density().stddev()/1000.0))) for i in range(summary.lane_count()))
    perReadsPassingFilter += "," + ",".join(str(round(summary.at(2).at(i).reads_pf()/1000000.0, 2)) for i in range(summary.lane_count()))
    perQ30Score += "," + ",".join(str(round(summary.at(2).at(i).percent_gt_q30(), 2)) for i in range(summary.lane_count()))
    numTotalReads += "," + ",".join(str(round(summary.at(2).at(i).reads()/1000000.0, 2)) for i in range(summary.lane_count()))
    aligned += "," + ",".join(str(round(summary.at(2).at(i).percent_aligned().mean(), 2)) + "+/-" + str(round(summary.at(2).at(i).percent_aligned().stddev(), 2)) for i in range(summary.lane_count()))
    errorRate += "," + ",".join(str(round(summary.at(2).at(i).error_rate().mean(), 2)) + "+/-" + str(round(summary.at(2).at(i).error_rate().stddev(), 2)) for i in range(summary.lane_count()))
    clusterPF += "," + ",".join(str(round(summary.at(2).at(i).percent_pf().mean(), 2)) + "+/-" + str(round(summary.at(2).at(i).percent_pf().stddev(), 2)) for i in range(summary.lane_count())) 
 
    conn.Execute("UPDATE thing1JobStatus SET `readsClusterDensity` = '%s', clusterPF = '%s', `numTotalReads` = '%s', `perReadsPassingFilter` = '%s', `perQ30Score` = '%s', aligned = '%s', `ErrorRate` = '%s' WHERE flowcellID = '%s'" % (readsClusterDensity, clusterPF, numTotalReads, perReadsPassingFilter, perQ30Score, aligned, errorRate, flowcellID))

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
