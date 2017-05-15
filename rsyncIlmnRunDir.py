#! /bin/env python3
"""
This script is to rsync the running folder from the sequencer to thing1
"""
import sys
import re
from pathlib import Path
import subprocess 
from utils.dbtools import DB_Connector, CronControlPanel
from utils.common import TimeString
from utils.SendEmail import SendEmail

class Usage:
    """

    Read the records in table thing1JobStatus which sequencing = '2'(means it it sequencing), and then check the status.

        Usage: python3 rsyncIlmnRunDir.py database.cnf
        Example: python3 rsyncIlmnRunDir.py clinicalB.cnf

    """
def rsync_folder(conn):
    runningfolder = conn.Execute("SELECT rundir,destinationDir,flowcellID FROM thing1JobStatus WHERE sequencing = '2'")
    if len(runningfolder) != 0:
        timestamp = TimeString()
        timestamp.print_timestamp()
        for row in runningfolder :
            command = "rsync -La --stats wei.wang@thing1.sickkids.ca:%s/ %s" % (row['rundir'], row['destinationDir'])
            try:
                subprocess.run(command, shell=True, check=True)
            except subprocess.CalledProcessError as grepexc: 
                SendEmail("rsync failed for flowcellID: " + row['flowcellID'], "weiw.wang@sickkids.ca", "Please check the following command:\n\n" + command + "\n\n" + "error code: " + str(grepexc.returncode) + "\n\n" + str(grepexc.output))

def check_failed_flowcell(conn):
    runningfolder = conn.Execute("SELECT flowcellID,machine FROM thing1JobStatus WHERE sequencing = '2' AND TIMESTAMPADD(HOUR,36,time)<CURRENT_TIMESTAMP")
    if len(runningfolder) != 0:
        timestamp = TimeString()
        timestamp.print_timestamp()
        content = "";
        for row in runningfolder :
            content += "flowellID %s on amchine %s can't be finished in 36 hours, it will be marked as failed.\n" % (row['flowcellID'], row['machine'])
            conn.Execute("UPDATE thing1JobStatus SET sequencing = '1' WHERE flowcellID = '%s'" % (row['flowcellID']))
        SendEmail("Flowcell(s) failed on sequencer", "weiw.wang@sickkids.ca", content)

def main(name, dbfile):

    if Path(dbfile).is_file(): 
        pass
    else:
        print("ERR: file: " + str(dbfile) + " does not exists!")
        print(Usage.__doc__)
        sys.exit(2)

    conn = DB_Connector(dbfile)
    cron_control = CronControlPanel(conn)
    cron_control.start_process('rsync_sequencer')
    rsync_folder(conn)
    check_failed_flowcell(conn)
    cron_control.stop_process('rsync_sequencer')

if __name__ == '__main__':

    if len(sys.argv) != 2:
        print(Usage.__doc__)
        sys.exit(2)

    main(*sys.argv)
