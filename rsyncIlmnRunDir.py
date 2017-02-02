#! /bin/env python3
"""
This script is to rsync the running folder from the sequencer to thing1
"""
import sys
import os
import pickle
from pathlib import Path
import subprocess 
from utils import *

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
            command = "rsync -Lav --progress --stats %s/ %s 1>/dev/null" % (row['runnndir'], row['destinationDir'])
            print(command)
            try :
                subprocess.check_call(command, shell=True)
            except CalledProcessError:
                SendEmail("Failed to rsync runfolder for flowcell %s" % (row['flowcellID']), "weiw.wang@sickkids.ca", "As Title.")

def check_failed_flowcell(conn):
    runningfolder = conn.Execute("SELECT flowcellID,machine FROM thing1JobStatus WHERE sequencing = '2' AND TIMESTAMPADD(HOUR,36,time)<CURRENT_TIMESTAMP")
    if len(runningfolder) != 0:
        timestamp = TimeString()
        timestamp.print_timestamp()
        content = "";
        for row in runningfolder :
            content += "flowellID %s on amchine %s can't be finished in 36 hours, it will be marked as failed.\n" % (row['flowcellID'], row['machine'])
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
