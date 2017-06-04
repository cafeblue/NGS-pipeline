#! /bin/env python3
"""
This script will do the following jobs:
    1. check the demultiplex status
    2. if done, rename the fastq files accordingly.
    3. parse the demultiplex information and upate the table thing1JobStatus in the database.
    4. Send and email if there are samples failed to pass the QC.
"""

import sys
import os
import re
import subprocess 
from pathlib import Path
from utils.dbtools import DB_Connector 
from utils.TimeString import TimeString
from utils.QualityControl import QualityControl
from utils.RenameFastq import RenameFastq
from utils.config import GlobalConfig, gpConfig
from utils.SendEmail import SendEmail
from utils.HPF import JsubExitCode
from utils.parseHTML import parseDemultiplexTable 

class Usage:
    """
        Rename the fastq files, parse the demultiplex info, do the QC for samples and update the table thing1JobStatus..

        Usage: python3 post_demultiplex.py database.cnf
        Example: python3 post_demultiplex.py clinicalB.cnf

    """

def getVer(pipelineroot, webroot):
    #website version
    cmd = 'ssh wei.wang@thing1.sickkids.ca "cd %s ; git tag | tail -1 ; git log -1 |head -1 |cut -b 8-14"' %(webroot)
    try:
        sshstr = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True, check=True).stdout.decode("utf-8").rstrip().split('\n')
    except subprocess.CalledProcessError as grepexc: 
        SendEmail("Failed to get the website version.", "weiw.wang@sickkids.ca", "Please check the following command:\n\n" + cmd + "\n\n" + "error code: " + str(grepexc.returncode) + "\n\n" + str(grepexc.output))
        sys.exit(2)
    webver = sshstr[0] + "(" + sshstr[1] + ")"
    #pipeline version
    cmd = "cd %s  ; git tag | tail -1 ; git log -1 | head -1 |cut -b 8-14" % (pipelineroot)
    try:
        sshstr = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True, check=True).stdout.decode("utf-8").rstrip().split('\n')
    except subprocess.CalledProcessError as grepexc: 
        SendEmail("Failed to get the pipeline version.", "weiw.wang@sickkids.ca", "Please check the following command:\n\n" + cmd + "\n\n" + "error code: " + str(grepexc.returncode) + "\n\n" + str(grepexc.output))
        sys.exit(2)
    pipelinever = sshstr[0] + "(" + sshstr[1] + ")"
    return pipelinever, webver

def insertIntoSampleInfo_QC(conn, samplesheet, demultiplex, config, gpconfig):
    emailcontent = ""
    pipelinever, webver = getVer(getattr(config, 'PIPELINE_ROOT'), getattr(config, 'WEB_ROOT'))
    for sample in samplesheet:
        keywd = sample['gene_panel'] + "\t" + sample['capture_kit']
        pipeID = getattr(gpconfig, keywd)['pipeID']
        if sample['sample_type'] == 'tumor':
            pipeID += '-t'
        if sample['pairedSampleID'] == '':
            sample['pairedSampleID'] = '0'
        ## Insert into table sampleInfo
        conn.Execute("INSERT INTO sampleInfo (sampleID, flowcellID, machine, captureKit, pairID, genePanelVer, pipeID, filterID, annotateID, yieldMB, numReads, perQ30Bases, specimen, sampleType, testType, priority, currentStatus, pipeThing1Ver, pipeHPFVer, webVer, perIndex ) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', '%s', '%s','%s','%s','%s','%s','%s','%s')" % (sample['sampleID'], sample['flowcell_ID'], sample['machine'], sample['capture_kit'], sample['pairedSampleID'], sample['gene_panel'], pipeID, getattr(gpconfig, keywd)['filterID'], getattr(gpconfig, keywd)['annotationID'], demultiplex[sample['sampleID']]['yieldMB'], demultiplex[sample['sampleID']]['numReads'], demultiplex[sample['sampleID']]['perQ30Bases'], sample['specimen'], sample['sample_type'], sample['testType'], sample['priority'], 0, '', pipelinever, webver, demultiplex[sample['sampleID']]['perIndex'] ))
        ##  qc4sampleLevel1
        qc1 = QualityControl(conn)
        tmpcont = qc1.qc4sample(sample['sampleID'], sample['machine'].replace("_1R", "").replace("_2", "").replace("_1", ""), sample['capture_kit'], demultiplex[sample['sampleID']], 1)
        if tmpcont != "":
            emailcontent += "sampleID %s:\n" % (sample['sampleID']) + tmpcont + "\n"
    if emailcontent != "":
        SendEmail("QC Warnings for samples on flowcell %s" % (samplesheet[0]['flowcell_ID']), 'weiw.wang@sickkids.ca', emailcontent)

def postDemultiplex(row, conn, config, gpconfig):
    conn.Execute("UPDATE thing1JobStatus SET demultiplex = '1' WHERE flowcellID = '%s'" % (row['flowcellID']))
    samplesheet = conn.Execute("SELECT * FROM sampleSheet WHERE flowcell_ID = '%s'" % (row['flowcellID']))
    sampleIDs = []
    sampleIDs.extend(s['sampleID'] for s in samplesheet)
    subFlowcellID = row['flowcellID']
    if not row['machine'].startswith('miseq'):
        subFlowcellID = subFlowcellID[1:]
    fastqdir = getattr(config, "FASTQ_FOLDER") + row['flowcellID']
    if os.path.isdir(fastqdir):
        RenameFastq(fastqdir, row['LaneCount'], row['flowcellID'], sampleIDs)
        insertIntoSampleInfo_QC(conn, samplesheet, parseDemultiplexTable("%s/Reports/html/%s/default/all/all/laneBarcode.html" % (fastqdir, subFlowcellID)), config, gpconfig)
    else:
        SendEmail("fastq folder not found!", 'weiw.wang@sickkids.ca', fastqdir + " can't be found on HPF")
    
def main(name, dbfile):
    if Path(dbfile).is_file():
        pass
    else:
        print("ERR: file: " + str(dbfile) + " does not exists!")
        print(Usage.__doc__)
        sys.exit(2)
    conn = DB_Connector(dbfile)
    demultiplexing = conn.Execute('SELECT flowcellID,machine,demultiplexJfolder,demultiplexJobID,LaneCount from thing1JobStatus where demultiplex = "2"')
    if len(demultiplexing) == 0:
        sys.exit(0)

    timestamp = TimeString()
    timestamp.print_timestamp()
    config = GlobalConfig(conn)
    gpconfig = gpConfig(conn)
    for row in demultiplexing:
        exitcode = JsubExitCode(row['demultiplexJfolder'], row['demultiplexJobID'])
        if exitcode == 0:
            postDemultiplex(row, conn, config, gpconfig)
        elif exitcode > 0 :
            SendEmail("demultiplex of flowcellID %s failed" % (row['flowcellID']), "weiw.wang@sickkids.ca", "Please check the log/err under folder: %s\n\nOR\n\n set demultiplex to 2 in table thing1JobStatus for flowcellID %s to re-run the demultiplex." % (row['demultiplexJfolder'], row['flowcellID']))
            conn.Execute("UPDATE thing1JobStatus SET demultiplex = '0' WHERE flowcellID = '%s'" % (row['flowcellID']))
            continue

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(Usage.__doc__)
        sys.exit(2)
    main(*sys.argv)

